"""The server end to end: severe alerts are triaged, and the incident is worked to resolution."""

import asyncio
from datetime import timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from mcp import Client
from mcp.types import TextContent

import oncall.app
from conftest import BASE, FakeClock, Script, log, publish, triage
from oncall.app import Oncall, create_app, open_database
from oncall.services import Rollback


async def test_severe_alerts_are_triaged_and_the_incident_resolved(
    client: httpx.AsyncClient, oncall: Oncall, script: Script, clock: FakeClock
) -> None:
    script.steps += triage(summary="api v2 is failing requests")
    await publish(client, type="deploy.completed", service="api", version="v1")
    clock.advance(3600)
    await publish(client, type="deploy.completed", service="api", version="v2")
    clock.advance(120)
    for n in range(3):
        await publish(client, type="alert.fired", service="api", severity=8, message=f"5xx {n}")
        clock.advance(20)

    settled = await oncall.reactor.settle()

    assert (settled.firings, settled.attempts) == (2, 2)
    runbook, triaged = (await client.get(f"{BASE}/runs")).json()  # newest first
    assert (triaged["rule"], triaged["status"]) == ("triage", "succeeded")
    assert triaged["output"] == {"severity": 8, "summary": "api v2 is failing requests"}
    assert (runbook["rule"], runbook["status"]) == ("runbook", "succeeded")
    assert runbook["output"] == (
        "api v2 was deployed just before the incident; rolled api back to v1; api is healthy"
    )
    assert oncall.deps.deployer.rollbacks == [Rollback("api", "v1")]
    assert oncall.deps.pager.pages == []

    # The agent was told what fired it, and given the log's tools.
    [prompt] = script.sent(0)
    assert prompt.startswith('Rule "triage" fired for {"service": "api"}.')
    assert "Three or more alerts of severity 7 or more" in prompt
    assert prompt.count('type="alert.fired"') == 3
    assert script.tools[0] == ["read_events", "emit_event"]
    assert '<event seq="2" type="deploy.completed"' in script.sent(1)[0]

    # The agent opened the incident, and the runbook resolved it, all in the alert's chain.
    [opened] = await log(client, "incident.opened")
    [resolved] = await log(client, "incident.resolved")
    third_alert = (await log(client, "alert.fired"))[-1]
    assert opened["event"] == {
        "type": "incident.opened",
        "service": "api",
        "severity": 8,
        "summary": "api v2 is failing requests",
    }
    assert opened["actor"] == {
        "kind": "agent",
        "rule": "triage",
        "run_id": triaged["id"],
        "name": "triage",
    }
    assert resolved["event"]["resolution"] == runbook["output"]
    assert resolved["actor"]["name"] == "runbook"
    chain = {envelope["correlation_id"] for envelope in (third_alert, opened, resolved)}
    assert chain == {third_alert["id"]}
    assert (opened["causation"]["depth"], resolved["causation"]["depth"]) == (1, 2)

    # The runbook checkpointed after every step, the decision included.
    steps = [e["event"]["step"] for e in await log(client, "run_progressed")]
    assert steps == ["__start__", "diagnose", "decide", "roll_back", "verify", "resolve", "__end__"]


async def test_alerts_count_per_service_within_five_minutes(
    client: httpx.AsyncClient, oncall: Oncall, clock: FakeClock
) -> None:
    async def alert(service: str, severity: int) -> None:
        await publish(client, type="alert.fired", service=service, severity=severity, message="x")

    await alert("api", 8)
    await alert("api", 6)  # not severe
    await alert("db", 9)  # another service
    clock.advance(301)
    await alert("api", 8)  # the first is more than five minutes old
    await alert("db", 7)
    assert (await oncall.reactor.settle()).firings == 0
    assert (await client.get(f"{BASE}/runs")).json() == []


async def test_the_app_runs_the_reactor_while_it_is_up(script: Script) -> None:
    script.steps += triage()
    app = create_app(model=script.model, poll_interval=timedelta(milliseconds=10))
    transport = httpx.ASGITransport(app=app)
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(transport=transport, base_url="http://oncall") as client,
    ):
        for version in ("v1", "v2"):
            await publish(client, type="deploy.completed", service="api", version=version)
        for _ in range(3):
            await publish(client, type="alert.fired", service="api", severity=9, message="down")
        resolved: list[dict[str, Any]] = []
        for _ in range(500):  # bounded: five seconds at most
            resolved = await log(client, "incident.resolved")
            if resolved:
                break
            await asyncio.sleep(0.01)
        [envelope] = resolved
        assert envelope["event"]["resolution"].startswith("api v2 was deployed")


@pytest.mark.parametrize(
    ("headers", "params", "user"),
    [({"x-user": "bob"}, {}, "bob"), ({}, {"user": "carol"}, "carol"), ({}, {}, "guest")],
)
async def test_demo_authentication_trusts_the_caller(
    client: httpx.AsyncClient, headers: dict[str, str], params: dict[str, str], user: str
) -> None:
    client.headers.pop("x-user")
    event = {"type": "service.heartbeat", "service": "api"}
    await client.post(f"{BASE}/events", json={"event": event}, headers=headers, params=params)
    [published] = await log(client)
    assert published["actor"] == {"kind": "user", "id": user, "name": user}


@pytest.mark.parametrize(
    ("event", "status"),
    [
        ({"type": "tick", "schedule": "heartbeat-check", "at": "2026-09-28T09:00:00Z"}, 403),
        ({"type": "rollback.proposed", "service": "api"}, 404),
        ({"type": "alert.fired", "service": "api", "severity": 11, "message": "x"}, 422),
    ],
)
async def test_only_oncall_events_are_accepted(
    client: httpx.AsyncClient, event: dict[str, Any], status: int
) -> None:
    response = await client.post(f"{BASE}/events", json={"event": event})
    assert response.status_code == status


async def test_the_rules_and_schedule_are_described(client: httpx.AsyncClient) -> None:
    rules = (await client.get("/v1/rules")).json()
    assert [(r["name"], r["then"]["action"]) for r in rules] == [
        ("triage", "triage"),
        ("runbook", "runbook"),
        ("silence", "page"),
    ]
    assert all(rule["description"] for rule in rules)
    [schedule] = (await client.get("/v1/schedules")).json()
    assert (schedule["name"], schedule["every"]) == ("heartbeat-check", "PT30S")


async def test_the_root_describes_the_surfaces(client: httpx.AsyncClient) -> None:
    about = (await client.get("/")).json()
    assert (about["stream"], about["mcp"]) == ("/v1/workspaces/{workspace_id}/stream", "/mcp")


async def test_mcp_clients_publish_into_the_same_workspace(server: str) -> None:
    async with Client(f"{server}/mcp/") as mcp:
        result = await mcp.call_tool(
            "publish_event",
            {
                "workspace_id": "prod",
                "event": {"type": "deploy.completed", "service": "api", "version": "v3"},
            },
        )
        assert not result.is_error
        rules = await mcp.call_tool("list_rules", {})
        [content] = rules.content
        assert isinstance(content, TextContent)
        assert '"name":"silence"' in content.text.replace(" ", "")
    async with httpx.AsyncClient(base_url=server) as http:
        [deploy] = await log(http, "deploy.completed")
    assert deploy["actor"] == {"kind": "external_agent", "client_id": "mcp", "name": "MCP client"}


async def test_workspaces_can_live_in_a_database(
    tmp_path: Path, script: Script, monkeypatch: pytest.MonkeyPatch
) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'oncall.db'}"
    first = Oncall(model=script.model, database_url=url).app(serve=False)
    async with first.router.lifespan_context(first):  # migrates the database
        transport = httpx.ASGITransport(app=first)
        async with httpx.AsyncClient(transport=transport, base_url="http://oncall") as client:
            await publish(client, type="deploy.completed", service="api", version="v1")

    monkeypatch.setenv("ONCALL_DATABASE_URL", url)
    restarted = create_app(model=script.model)
    async with restarted.router.lifespan_context(restarted):
        transport = httpx.ASGITransport(app=restarted)
        async with httpx.AsyncClient(transport=transport, base_url="http://oncall") as client:
            [deploy] = await log(client, "deploy.completed")
    assert deploy["event"]["version"] == "v1", "a restarted server finds it"


def test_other_databases_get_a_plain_async_engine() -> None:
    assert open_database("postgresql+asyncpg://localhost/oncall").dialect.name == "postgresql"


def test_main_serves_on_the_configured_address(monkeypatch: pytest.MonkeyPatch) -> None:
    served: dict[str, Any] = {}
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: served.update(kwargs))
    monkeypatch.setenv("ONCALL_PORT", "9000")
    oncall.app.main()
    assert served == {"host": "127.0.0.1", "port": 9000}
