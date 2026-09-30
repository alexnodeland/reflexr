"""REST: publishing, commands, reads and administration, with auth and rejections."""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from reflexr import Rule, on, run
from reflexr.core import Event, PredicateFilter
from reflexr.workspace import Reactor, Schedule, Workspaces
from tests.event_types import Deploy
from tests.fastapi.conftest import STORED_RULES, build, chat_rule, command, heartbeat, spike

ERROR = {"type": "app:service.error", "service": "auth"}
COMMANDS = "/workspaces/prod/commands"
PROVENANCE = {"source": "artifactr", "artifact": "art_7", "version": 3}


@pytest.fixture
async def app() -> AsyncIterator[tuple[httpx.AsyncClient, Workspaces, Reactor[None]]]:
    application, workspaces, reactor = build()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:
        yield client, workspaces, reactor


type App = tuple[httpx.AsyncClient, Workspaces, Reactor[None]]


@pytest.fixture
async def client() -> AsyncIterator[httpx.AsyncClient]:
    """A client of an application with stored rules, in which Ada has installed chat:deploys."""
    application, _, _ = build(stored_rules=STORED_RULES)
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:
        install = command("c0", type="install_rule", rule=chat_rule(), provenance=PROVENANCE)
        assert (await client.post(COMMANDS, json=install)).status_code == 200
        yield client


async def test_producers_publish_events_one_at_a_time_or_in_batches(app: App) -> None:
    client, _, _ = app
    one = await client.post("/workspaces/prod/events", json={"event": ERROR, "id": "e1"})
    assert one.json() == [{"type": "published", "seq": 1, "id": "e1", "duplicate": False}]
    batch = await client.post(
        "/workspaces/prod/events",
        json={"events": [{"event": ERROR, "id": "e1"}, {"event": ERROR}], "correlation_id": "e1"},
    )
    assert [(o["seq"], o["duplicate"]) for o in batch.json()] == [(1, True), (2, False)]
    events = (await client.get("/workspaces/prod/events", params={"after_seq": 1})).json()
    assert [(e["seq"], e["correlation_id"], e["actor"]["id"]) for e in events] == [(2, "e1", "ada")]
    later = {"events": [{"event": ERROR}], "correlation_id": events[0]["id"]}
    wrong = await client.post("/workspaces/prod/events", json=later)
    assert (wrong.status_code, wrong.json()["detail"]["type"]) == (422, "validation_failed")
    assert wrong.json()["detail"]["message"].endswith("it belongs to chain e1")
    refused = await client.post(
        "/workspaces/prod/events", json={"event": {"type": "reflexr:rule_fired", "rule": "x"}}
    )
    assert refused.status_code == 422  # not a valid fact, so never reaches the workspace
    unknown = await client.post("/workspaces/prod/events", json={"event": {"type": "app:pager"}})
    assert (unknown.status_code, unknown.json()["detail"]["type"]) == (404, "not_found")
    bare = await client.post("/workspaces/prod/events", json={"event": {"type": "pager"}})
    assert (bare.status_code, bare.json()["detail"]["type"]) == (422, "validation_failed")


async def test_reads_filter_the_log_and_list_rules_runs_and_schedules(app: App) -> None:
    client, _, reactor = app
    for _ in range(2):
        await client.post("/workspaces/prod/events", json={"event": ERROR})
    await client.post(
        "/workspaces/prod/events",
        json={"event": {"type": "app:deploy.finished", "service": "auth"}},
    )
    await reactor.settle()
    only = await client.get("/workspaces/prod/events", params={"type": ["app:deploy.finished"]})
    assert [e["seq"] for e in only.json()] == [3]
    bare = await client.get("/workspaces/prod/events", params={"type": ["deploy.finished"]})
    assert (bare.status_code, bare.json()["detail"]["message"]) == (
        422,
        "event type 'deploy.finished' has no namespace; did you mean 'app:deploy.finished'?",
    )
    limited = await client.get("/workspaces/prod/events", params={"limit": 2})
    assert len(limited.json()) == 2
    assert [r["name"] for r in (await client.get("/rules")).json()] == ["app:spike"]
    [status] = (await client.get("/workspaces/prod/rules")).json()
    assert status == {
        "rule": "app:spike",
        "origin": "code",
        "version": None,
        "enabled": True,
        "cursor": status["cursor"],
        "lag": 0,
        "generation": 0,
        "dead_letters": 0,
    }
    [done] = (await client.get("/workspaces/prod/runs", params={"rule": "app:spike"})).json()
    assert (done["status"], done["output"]) == ("succeeded", {"paged": "auth"})
    fetched = await client.get(f"/workspaces/prod/runs/{done['id']}")
    assert fetched.json()["id"] == done["id"]
    missing = await client.get("/workspaces/prod/runs/nope")
    assert (missing.status_code, missing.json()["detail"]["type"]) == (404, "not_found")
    assert (await client.get("/workspaces/prod/dead-letters")).json() == []
    assert [s["name"] for s in (await client.get("/schedules")).json()] == ["heartbeat-check"]
    [ticking] = (await client.get("/workspaces/prod/schedules")).json()
    assert ticking["last_tick"] is not None
    assert ticking["next_tick"] > ticking["last_tick"]
    [fresh] = (await client.get("/workspaces/empty/schedules")).json()
    assert (fresh["last_tick"], fresh["next_tick"]) == (None, None)
    unevaluated = (await client.get("/workspaces/empty/rules")).json()
    assert unevaluated == [
        {
            "rule": "app:spike",
            "origin": "code",
            "version": None,
            "enabled": True,
            "cursor": 0,
            "lag": 0,
            "generation": 0,
            "dead_letters": 0,
        }
    ]


async def test_the_log_is_read_from_its_end_and_backwards(app: App) -> None:
    client, _, _ = app
    deploy = {"type": "app:deploy.finished", "service": "auth"}
    for event in (ERROR, deploy, ERROR, deploy, ERROR, deploy):
        await client.post("/workspaces/prod/events", json={"event": event})

    async def seqs(**params: int | list[str]) -> list[int]:
        response = await client.get("/workspaces/prod/events", params=params)
        return [envelope["seq"] for envelope in response.json()]

    assert await seqs(last=2) == [5, 6]
    assert await seqs(last=2, type=["app:deploy.finished"]) == [4, 6]
    assert await seqs(last=2, type=["app:deploy.finished"], before_seq=4) == [2]
    assert await seqs(after_seq=1, before_seq=4) == [2, 3]
    both = await client.get("/workspaces/prod/events", params={"limit": 1, "last": 1})
    assert (both.status_code, both.json()["detail"]) == (
        422,
        {"type": "validation_failed", "message": "give limit or last, not both", "errors": []},
    )
    negative = await client.get("/workspaces/prod/events", params={"last": -1})
    assert negative.status_code == 422


async def test_commands_are_idempotent_and_map_rejections_to_statuses(app: App) -> None:
    client, _, _ = app
    frame = {"type": "command", "command_id": "c1", "command": {"type": "publish", "event": ERROR}}
    first = await client.post("/workspaces/prod/commands", json=frame)
    again = await client.post("/workspaces/prod/commands", json=frame)
    assert first.json() == again.json()
    assert first.json()["outcome"]["seq"] == 1
    assert len((await client.get("/workspaces/prod/events")).json()) == 1
    retry = {
        "type": "command",
        "command_id": "c2",
        "command": {"type": "retry_run", "run_id": "nope"},
    }
    rejected = await client.post("/workspaces/prod/commands", json=retry)
    assert (rejected.status_code, rejected.json()["rejection"]["type"]) == (404, "not_found")
    feedback = {
        "type": "command",
        "command_id": "c3",
        "command": {
            "type": "give_feedback",
            "feedback_type": "useful",
            "target": {"kind": "chain", "correlation_id": first.json()["outcome"]["id"]},
            "value": {"useful": True},
        },
    }
    recorded = await client.post("/workspaces/prod/commands", json=feedback)
    assert recorded.json()["outcome"] == {
        "type": "recorded",
        "seq": 2,
        "id": recorded.json()["outcome"]["id"],
    }


async def test_a_command_id_is_remembered_per_participant(app: App) -> None:
    client, _, _ = app
    frame = {"type": "command", "command_id": "c1", "command": {"type": "publish", "event": ERROR}}
    url = "/workspaces/prod/commands"
    first = await client.post(url, json=frame, headers={"x-name": "Ada"})
    renamed = await client.post(url, json=frame, headers={"x-name": "Ada Lovelace"})
    assert first.json() == renamed.json(), "one participant, whatever its display name"
    other = await client.post(url, json=frame, headers={"x-user": "grace"})
    assert other.json()["outcome"]["seq"] == 2, "another participant's command runs"
    assert len((await client.get("/workspaces/prod/events")).json()) == 2


async def test_the_rule_status_says_whether_a_rule_is_enabled() -> None:
    application, _, _ = build(rules=[spike.model_copy(update={"enabled": False})])
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:
        for _ in range(2):
            await client.post("/workspaces/prod/events", json={"event": ERROR})
        [status] = (await client.get("/workspaces/prod/rules")).json()
    assert (status["enabled"], status["cursor"], status["lag"]) == (False, 0, 2)


class Clock:
    def __init__(self) -> None:
        self.now = datetime(2026, 1, 1, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now


def boom(event: Event) -> bool:
    raise RuntimeError("no")


async def test_the_rule_and_schedule_status_bodies_keep_their_json() -> None:
    fragile = Rule(
        name="app:fragile", when=on(Deploy).where(PredicateFilter(name="boom")), then=run("page")
    )
    off = spike.model_copy(update={"name": "app:off", "enabled": False})
    staging = Schedule(
        name="staging", every=timedelta(minutes=5), workspaces=(("acme", "staging"),)
    )
    clock = Clock()
    application, _, reactor = build(
        rules=[spike, fragile, off],
        schedules=[heartbeat, staging],
        predicates={"boom": boom},
        clock=clock,
    )
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:
        for _ in range(2):
            await client.post("/workspaces/prod/events", json={"event": ERROR})
        await client.post(
            "/workspaces/prod/events",
            json={"event": {"type": "app:deploy.finished", "service": "a"}},
        )
        await reactor.settle()
        clock.now += timedelta(seconds=65)
        await reactor.settle()
        rules = (await client.get("/workspaces/prod/rules")).json()
        schedules = (await client.get("/workspaces/prod/schedules")).json()
        fresh = (await client.get("/workspaces/empty/schedules")).json()
    assert rules == [
        {
            "rule": "app:spike",
            "origin": "code",
            "version": None,
            "enabled": True,
            "cursor": 8,
            "lag": 0,
            "generation": 0,
            "dead_letters": 0,
        },
        {
            "rule": "app:fragile",
            "origin": "code",
            "version": None,
            "enabled": True,
            "cursor": 8,
            "lag": 0,
            "generation": 0,
            "dead_letters": 1,
        },
        {
            "rule": "app:off",
            "origin": "code",
            "version": None,
            "enabled": False,
            "cursor": 0,
            "lag": 8,
            "generation": 0,
            "dead_letters": 0,
        },
    ]
    assert schedules == [
        {
            "schedule": "heartbeat-check",
            "last_tick": "2026-01-01T00:01:00Z",
            "next_tick": "2026-01-01T00:01:30Z",
        }
    ]
    assert fresh == [{"schedule": "heartbeat-check", "last_tick": None, "next_tick": None}]


def test_the_status_bodies_forbid_unknown_fields() -> None:
    application, _, _ = build()
    schemas = application.openapi()["components"]["schemas"]
    assert [schemas[name]["additionalProperties"] for name in ("RuleStatus", "ScheduleStatus")] == [
        False,
        False,
    ]


async def test_requests_are_authenticated_and_authorized(app: App) -> None:
    client, _, _ = app
    bad = await client.get("/workspaces/prod/events", headers={"x-token": "bad"})
    assert (bad.status_code, bad.json()["detail"]) == (401, "bad token")
    assert (await client.get("/rules", headers={"x-token": "bad"})).status_code == 401
    refused = await client.get("/workspaces/secret/events")
    assert (refused.status_code, refused.json()["detail"]) == (
        403,
        {"type": "forbidden", "message": "this workspace is not yours to use"},
    )
    await client.post("/workspaces/prod/events", json={"event": ERROR})
    other = await client.get("/workspaces/prod/events", headers={"x-tenant": "globex"})
    assert other.json() == []  # the same workspace id in another tenant is another workspace


async def test_unauthorized_without_a_message_says_so() -> None:
    from starlette.requests import HTTPConnection

    from reflexr import Actor
    from reflexr.core import TenantId
    from reflexr.fastapi import Unauthorized, reflexr_router

    async def refuse(connection: HTTPConnection) -> tuple[TenantId, Actor]:
        raise Unauthorized

    _, workspaces, _ = build()
    application = FastAPI()
    application.include_router(reflexr_router(workspaces, resolve_actor=refuse))
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get("/rules")
    assert response.json() == {"detail": "unauthorized"}


async def test_only_recent_command_ids_are_remembered() -> None:
    application, _, _ = build(remembered_commands=1)
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:

        async def send(command_id: str) -> int:
            frame = {
                "type": "command",
                "command_id": command_id,
                "command": {"type": "publish", "event": ERROR},
            }
            response = await client.post("/workspaces/prod/commands", json=frame)
            return response.json()["outcome"]["seq"]

        assert [await send("c1"), await send("c2"), await send("c1")] == [1, 2, 3]


async def test_a_tenant_sees_only_its_own_schedule_targets() -> None:
    both = Schedule(
        name="both",
        every=timedelta(minutes=5),
        workspaces=(("acme", "prod"), ("globex", "prod"), ("acme", "staging")),
    )
    theirs = Schedule(name="theirs", every=timedelta(minutes=5), workspaces=(("globex", "ops"),))
    application, _, _ = build(schedules=[heartbeat, both, theirs])
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:
        acme = (await client.get("/schedules", headers={"x-tenant": "acme"})).json()
        globex = (await client.get("/schedules", headers={"x-tenant": "globex"})).json()
        refused = await client.get("/schedules", headers={"x-token": "bad"})
    assert {s["name"]: s["workspaces"] for s in acme} == {
        "heartbeat-check": "all",
        "both": [["acme", "prod"], ["acme", "staging"]],
    }
    assert {s["name"]: s["workspaces"] for s in globex} == {
        "heartbeat-check": "all",
        "both": [["globex", "prod"]],
        "theirs": [["globex", "ops"]],
    }
    assert refused.status_code == 401


async def test_stored_rules_are_installed_updated_and_archived_through_commands(
    client: httpx.AsyncClient,
) -> None:
    install = command("c1", type="install_rule", rule=chat_rule("chat:releases"))
    first = await client.post(COMMANDS, json=install)
    assert first.json()["outcome"] == {
        "type": "rule_version",
        "rule": "chat:releases",
        "version": 1,
        "seq": 2,
        "duplicate": False,
    }
    assert (await client.post(COMMANDS, json=install)).json() == first.json(), "one id, once"
    again = await client.post(COMMANDS, json=install | {"command_id": "c2"})
    assert again.json()["outcome"] == first.json()["outcome"] | {"duplicate": True}, "no change"
    billing = chat_rule("chat:releases", service="billing")
    update = command("c3", type="update_rule", rule=billing, expected_version=1)
    updated = (await client.post(COMMANDS, json=update)).json()["outcome"]
    assert (updated["version"], updated["duplicate"]) == (2, False)
    got = await client.get("/workspaces/prod/rules/chat:releases")
    assert got.json() == {"rule": billing, "origin": "stored", "version": 2, "provenance": {}}
    archive = command("c4", type="archive_rule", rule="chat:releases", expected_version=2)
    archived = (await client.post(COMMANDS, json=archive)).json()["outcome"]
    assert (archived["version"], archived["duplicate"]) == (3, False)
    gone = await client.get("/workspaces/prod/rules/chat:releases")
    assert (gone.status_code, gone.json()["detail"]) == (
        404,
        {
            "type": "not_found",
            "message": "rule chat:releases does not exist",
            "entity": "rule",
            "id": "chat:releases",
        },
    )


async def test_a_workspace_reads_its_stored_rules_beside_the_code_rules(
    client: httpx.AsyncClient,
) -> None:
    stored_rule = await client.get("/workspaces/prod/rules/chat:deploys")
    assert stored_rule.json() == {
        "rule": chat_rule(),
        "origin": "stored",
        "version": 1,
        "provenance": PROVENANCE,
    }
    code_rule = await client.get("/workspaces/prod/rules/app:spike")
    assert code_rule.json() == {
        "rule": spike.model_dump(mode="json"),
        "origin": "code",
        "version": None,
        "provenance": None,
    }
    statuses = (await client.get("/workspaces/prod/rules")).json()
    assert [(s["rule"], s["origin"], s["version"]) for s in statuses] == [
        ("app:spike", "code", None),
        ("chat:deploys", "stored", 1),
    ]
    rules = (await client.get("/rules")).json()
    assert [r["name"] for r in rules] == ["app:spike"], "the code rules, as every tenant's"


async def test_no_other_tenant_or_workspace_sees_a_stored_rule(client: httpx.AsyncClient) -> None:
    for workspace, headers in (("prod", {"x-tenant": "globex"}), ("staging", {})):
        url = f"/workspaces/{workspace}"
        missing = await client.get(f"{url}/rules/chat:deploys", headers=headers)
        assert missing.status_code == 404
        statuses = (await client.get(f"{url}/rules", headers=headers)).json()
        assert [s["rule"] for s in statuses] == ["app:spike"]
        assert (await client.get(f"{url}/events", headers=headers)).json() == []
        archive = command("c1", type="archive_rule", rule="chat:deploys")
        refused = await client.post(f"{url}/commands", json=archive, headers=headers)
        assert (refused.status_code, refused.json()["rejection"]["type"]) == (404, "not_found")
    secret = await client.get("/workspaces/secret/rules/chat:deploys")
    assert (secret.status_code, secret.json()["detail"]["type"]) == (403, "forbidden")


@pytest.mark.parametrize(
    ("change", "headers", "status", "rejection"),
    [
        pytest.param(
            {"type": "install_rule", "rule": chat_rule("chat:releases")},
            {"x-user": "grace"},
            403,
            {
                "type": "forbidden",
                "message": "this change to rule chat:releases is not yours to make",
            },
            id="allow refuses",
        ),
        pytest.param(
            {"type": "update_rule", "rule": spike.model_dump(mode="json")},
            {},
            403,
            {"type": "forbidden", "message": "rule app:spike is registered in code"},
            id="a code rule",
        ),
        pytest.param(
            {"type": "install_rule", "rule": chat_rule("chat:releases") | {"timeout": None}},
            {},
            422,
            {
                "type": "validation_failed",
                "message": "rule chat:releases cannot be stored",
                "errors": ["timeout is required"],
            },
            id="beyond the limits",
        ),
        pytest.param(
            {"type": "install_rule", "rule": chat_rule(service="billing")},
            {},
            409,
            {
                "type": "invalid_state",
                "message": "rule chat:deploys is installed already: update it",
            },
            id="installed already",
        ),
        pytest.param(
            {"type": "update_rule", "rule": chat_rule(service="billing"), "expected_version": 2},
            {},
            409,
            {"type": "invalid_state", "message": "rule chat:deploys is at version 1, not 2"},
            id="another version",
        ),
        pytest.param(
            {"type": "archive_rule", "rule": "chat:releases"},
            {},
            404,
            {
                "type": "not_found",
                "message": "stored rule chat:releases does not exist",
                "entity": "stored rule",
                "id": "chat:releases",
            },
            id="no such rule",
        ),
    ],
)
async def test_a_refused_change_to_a_stored_rule_answers_its_status(
    client: httpx.AsyncClient,
    change: dict[str, Any],
    headers: dict[str, str],
    status: int,
    rejection: dict[str, Any],
) -> None:
    response = await client.post(COMMANDS, json=command("c1", **change), headers=headers)
    assert (response.status_code, response.json()["rejection"]) == (status, rejection)


async def test_without_stored_rules_every_change_is_forbidden(app: App) -> None:
    client, _, _ = app
    install = command("c1", type="install_rule", rule=chat_rule())
    refused = await client.post(COMMANDS, json=install)
    assert (refused.status_code, refused.json()["rejection"]) == (
        403,
        {"type": "forbidden", "message": "stored rules are off in these workspaces"},
    )
