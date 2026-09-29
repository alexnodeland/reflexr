"""REST: publishing, commands, reads and administration, with auth and rejections."""

from collections.abc import AsyncIterator

import httpx
import pytest
from fastapi import FastAPI

import reflexr.fastapi
import reflexr.workspace
from reflexr.workspace import Reactor, Workspaces
from tests.fastapi.conftest import build, spike

ERROR = {"type": "service.error", "service": "auth"}


@pytest.fixture
async def app() -> AsyncIterator[tuple[httpx.AsyncClient, Workspaces, Reactor[None]]]:
    application, workspaces, reactor = build()
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:
        yield client, workspaces, reactor


type App = tuple[httpx.AsyncClient, Workspaces, Reactor[None]]


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
        "/workspaces/prod/events", json={"event": {"type": "rule_fired", "rule": "x"}}
    )
    assert refused.status_code == 422  # not a valid rule_fired, so never reaches the workspace
    unknown = await client.post("/workspaces/prod/events", json={"event": {"type": "pager"}})
    assert (unknown.status_code, unknown.json()["detail"]["type"]) == (404, "not_found")


async def test_reads_filter_the_log_and_list_rules_runs_and_schedules(app: App) -> None:
    client, _, reactor = app
    for _ in range(2):
        await client.post("/workspaces/prod/events", json={"event": ERROR})
    await client.post(
        "/workspaces/prod/events", json={"event": {"type": "deploy.finished", "service": "auth"}}
    )
    await reactor.settle()
    only = await client.get("/workspaces/prod/events", params={"type": ["deploy.finished"]})
    assert [e["seq"] for e in only.json()] == [3]
    limited = await client.get("/workspaces/prod/events", params={"limit": 2})
    assert len(limited.json()) == 2
    assert [r["name"] for r in (await client.get("/rules")).json()] == ["spike"]
    [status] = (await client.get("/workspaces/prod/rules")).json()
    assert status == {
        "rule": "spike",
        "enabled": True,
        "cursor": status["cursor"],
        "lag": 0,
        "generation": 0,
        "dead_letters": 0,
    }
    [done] = (await client.get("/workspaces/prod/runs", params={"rule": "spike"})).json()
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
            "rule": "spike",
            "enabled": True,
            "cursor": 0,
            "lag": 0,
            "generation": 0,
            "dead_letters": 0,
        }
    ]


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


async def test_the_rule_status_says_whether_a_rule_is_enabled() -> None:
    application, _, _ = build(rules=[spike.model_copy(update={"enabled": False})])
    transport = httpx.ASGITransport(app=application)
    async with httpx.AsyncClient(transport=transport, base_url="http://test/v1") as client:
        for _ in range(2):
            await client.post("/workspaces/prod/events", json={"event": ERROR})
        [status] = (await client.get("/workspaces/prod/rules")).json()
    assert (status["enabled"], status["cursor"], status["lag"]) == (False, 0, 2)


async def test_requests_are_authenticated_and_authorized(app: App) -> None:
    assert reflexr.fastapi.Authorize is reflexr.workspace.Authorize  # every surface's hook
    client, _, _ = app
    bad = await client.get("/workspaces/prod/events", headers={"x-token": "bad"})
    assert (bad.status_code, bad.json()["detail"]) == (401, "bad token")
    assert (await client.get("/rules", headers={"x-token": "bad"})).status_code == 401
    assert (await client.get("/workspaces/secret/events")).status_code == 403
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
