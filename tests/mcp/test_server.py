"""External agents over MCP: tools, the run resource, and change notifications."""

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any, Literal

import pytest
from mcp import Client
from mcp.server.mcpserver import Context
from mcp.server.subscriptions import InMemorySubscriptionBus, ResourceUpdated, ServerEvent
from mcp.types import TextContent, TextResourceContents

from reflexr import Actor, F, Feedback, Rule, by, on, run
from reflexr.core import EvaluationError, ExternalAgentActor, TenantId, WorkspaceId
from reflexr.mcp import ReflexrMcp, run_uri
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, WorkspaceRef, Workspaces
from tests.event_types import Deploy, ServiceError

CLAUDE = ExternalAgentActor(client_id="claude-code", name="Claude Code")
deploys = Rule(name="deploys", when=on(Deploy), scope=by(F.service), then=run("note"))


class Useful(Feedback, name="mcp_useful", targets={"run"}):
    useful: bool


class RecordingBus(InMemorySubscriptionBus):
    def __init__(self) -> None:
        super().__init__()
        self.events: list[ResourceUpdated] = []

    async def publish(self, event: ServerEvent) -> None:
        if isinstance(event, ResourceUpdated):
            self.events.append(event)
        await super().publish(event)


class Identity:
    """Who the MCP client is; tests switch it to act as another tenant."""

    def __init__(self) -> None:
        self.tenant: TenantId = "acme"

    async def resolve(self, ctx: Context) -> tuple[TenantId, ExternalAgentActor]:
        return self.tenant, CLAUDE


async def note(reaction: Reaction[None]) -> Literal["noted"]:
    if reaction.scope["service"] == "billing":
        raise RuntimeError("billing is down")
    return "noted"


@pytest.fixture
def workspaces() -> Workspaces:
    return Workspaces(InMemoryStorage(), events=[Deploy, ServiceError], rules=[deploys])


@pytest.fixture
async def server(
    workspaces: Workspaces,
) -> AsyncIterator[tuple[ReflexrMcp, Identity, RecordingBus]]:
    identity, bus = Identity(), RecordingBus()
    mcp = ReflexrMcp(workspaces, resolve=identity.resolve, bus=bus)
    yield mcp, identity, bus
    await mcp.aclose()


type Server = tuple[ReflexrMcp, Identity, RecordingBus]


async def call(client: Client, tool: str, **args: Any) -> tuple[bool, str]:
    result = await client.call_tool(tool, args)
    [content] = result.content
    assert isinstance(content, TextContent)
    return result.is_error, content.text


async def test_agents_publish_read_and_operate(server: Server, workspaces: Workspaces) -> None:
    mcp, _, bus = server
    async with Client(mcp.server) as client:
        tools = {tool.name for tool in (await client.list_tools()).tools}
        assert {"publish_event", "read_events", "replay_rule", "cancel_run"} <= tools
        error, published = await call(
            client,
            "publish_event",
            workspace_id="prod",
            event={"type": "deploy.finished", "service": "auth"},
            id="d1",
        )
        assert (error, json.loads(published)["seq"]) == (False, 1)
        await call(
            client,
            "publish_event",
            workspace_id="prod",
            event={"type": "deploy.finished", "service": "billing"},
        )
        await Reactor(workspaces, actions={"note": note}).settle()
        _, lines = await call(client, "read_events", workspace_id="prod", types=["deploy.finished"])
        envelopes = [json.loads(line) for line in lines.splitlines()]
        assert [e["seq"] for e in envelopes] == [1, 2]
        assert envelopes[0]["actor"] == CLAUDE.model_dump(mode="json")
        assert (await call(client, "read_events", workspace_id="prod", after_seq=99))[
            1
        ] == "No events."
        [rule] = json.loads((await call(client, "list_rules"))[1])
        assert rule["name"] == "deploys"
        status = (await call(client, "rule_status", workspace_id="prod"))[1]
        assert status == "- deploys: enabled, cursor 8, 0 behind, generation 0, 0 dead letters"
        assert (await call(client, "rule_status", workspace_id="empty"))[1] == (
            "- deploys: enabled, cursor 0, 0 behind, generation 0, 0 dead letters"
        )
        failing, done = [
            json.loads(line)
            for line in (await call(client, "list_runs", workspace_id="prod"))[1].splitlines()
        ]
        assert (done["status"], done["output"]) == ("succeeded", "noted")
        assert failing["status"] == "retrying"
        assert (await call(client, "list_runs", workspace_id="empty"))[1] == "No runs."
        got = json.loads((await call(client, "get_run", workspace_id="prod", run_id=done["id"]))[1])
        assert got["id"] == done["id"]
        assert (await call(client, "get_run", workspace_id="prod", run_id="nope"))[0] is True
        retried = json.loads(
            (await call(client, "retry_run", workspace_id="prod", run_id=failing["id"]))[1]
        )
        assert retried["run"]["status"] == "pending"
        skipped = json.loads(
            (
                await call(
                    client, "skip_run", workspace_id="prod", run_id=failing["id"], reason="dup"
                )
            )[1]
        )
        assert skipped["run"]["status"] == "skipped"
        cancelled = await call(client, "cancel_run", workspace_id="prod", run_id=failing["id"])
        assert cancelled[0]
        assert cancelled[1].endswith(f"cannot cancel run {failing['id']}, which is skipped")
        replayed = json.loads(
            (await call(client, "replay_rule", workspace_id="prod", rule="deploys"))[1]
        )
        assert replayed["progress"]["generation"] == 1
        feedback = await call(
            client,
            "give_feedback",
            workspace_id="prod",
            feedback_type="mcp_useful",
            target={"kind": "run", "run_id": done["id"]},
            value={"useful": True},
        )
        assert json.loads(feedback[1])["type"] == "recorded"
        assert (await call(client, "list_dead_letters", workspace_id="prod"))[1] == "None."
        bad = await call(
            client, "publish_event", workspace_id="prod", event={"type": "deploy.finished"}
        )
        assert (bad[0], "invalid deploy.finished event" in bad[1]) == (True, True)
        await asyncio.sleep(0.01)  # let the watcher forward the run facts
    uris = {str(event.uri) for event in bus.events}
    assert run_uri("acme", "prod", done["id"]) in uris


async def test_runs_are_resources_of_their_tenant_only(
    server: Server, workspaces: Workspaces
) -> None:
    mcp, identity, _ = server
    workspace = await workspaces.open("acme", "prod", actor=CLAUDE)
    await workspace.publish(Deploy(service="auth"))
    await Reactor(workspaces, actions={"note": note}).settle()
    [done] = await workspace.runs()
    async with Client(mcp.server) as client:
        result = await client.read_resource(run_uri("acme", "prod", done.id))
        [content] = result.contents
        assert isinstance(content, TextResourceContents)
        assert json.loads(content.text)["id"] == done.id
        with pytest.raises(Exception, match="run nope does not exist"):
            await client.read_resource(run_uri("acme", "prod", "nope"))
        identity.tenant = "globex"
        with pytest.raises(Exception, match="runs of tenant acme are not available"):
            await client.read_resource(run_uri("acme", "prod", done.id))


async def test_authorize_decides_which_workspaces_a_client_may_use(
    workspaces: Workspaces,
) -> None:
    asked: list[tuple[TenantId, WorkspaceId, str]] = []

    async def authorize(tenant_id: TenantId, workspace_id: WorkspaceId, actor: Actor) -> bool:
        asked.append((tenant_id, workspace_id, actor.kind))
        return workspace_id != "secret"

    secret = await workspaces.open("acme", "secret", actor=CLAUDE)
    await secret.publish(Deploy(service="auth"))
    await Reactor(workspaces, actions={"note": note}).settle()
    [done] = await secret.runs()
    head = await secret.head_seq()
    mcp = ReflexrMcp(workspaces, resolve=Identity().resolve, authorize=authorize)
    run_id = {"run_id": done.id}
    tools: dict[str, dict[str, Any]] = {
        "publish_event": {"event": {"type": "deploy.finished", "service": "auth"}},
        "read_events": {},
        "rule_status": {},
        "replay_rule": {"rule": "deploys"},
        "list_runs": {},
        "get_run": run_id,
        "retry_run": run_id,
        "skip_run": run_id,
        "cancel_run": run_id,
        "list_dead_letters": {},
        "give_feedback": {
            "feedback_type": "mcp_useful",
            "target": {"kind": "run", **run_id},
            "value": {"useful": True},
        },
    }
    try:
        async with Client(mcp.server) as client:
            for tool, args in tools.items():
                error, text = await call(client, tool, workspace_id="secret", **args)
                assert (error, text.endswith("this workspace is not yours to use")) == (
                    True,
                    True,
                ), tool
            with pytest.raises(Exception, match="this workspace is not yours to use"):
                await client.read_resource(run_uri("acme", "secret", done.id))
            assert (await call(client, "read_events", workspace_id="prod"))[0] is False
            assert (await call(client, "list_rules"))[0] is False  # names no workspace
    finally:
        await mcp.aclose()
    assert asked.count(("acme", "secret", "external_agent")) == len(tools) + 1
    assert asked[-1] == ("acme", "prod", "external_agent")
    assert await secret.head_seq() == head  # nothing was written


async def test_the_rule_status_lists_every_registered_rule_as_rest_does() -> None:
    storage = InMemoryStorage()
    retired = Rule(name="retired", when=on(Deploy), then=run("note"))
    before = Workspaces(storage, events=[Deploy], rules=[deploys, retired])
    await (await before.open("acme", "prod", actor=CLAUDE)).publish(Deploy(service="auth"))
    async with storage.transaction(WorkspaceRef("acme", "prod")) as transaction:
        await transaction.dead_letter([EvaluationError(rule="deploys", seq=1, error="no")])
    off = Rule(name="off", when=on(Deploy), then=run("note"), enabled=False)  # has never run
    workspaces = Workspaces(storage, events=[Deploy], rules=[deploys, off])
    workspace = await workspaces.open("acme", "prod", actor=CLAUDE)
    mcp = ReflexrMcp(workspaces, resolve=Identity().resolve)
    bare = ReflexrMcp(Workspaces(storage, events=[Deploy]), resolve=Identity().resolve)
    try:
        async with Client(mcp.server) as client, Client(bare.server) as unruled:
            _, status = await call(client, "rule_status", workspace_id="prod")
            _, none = await call(unruled, "rule_status", workspace_id="prod")
    finally:
        await mcp.aclose()
        await bare.aclose()
    assert status == (
        "- deploys: enabled, cursor 0, 1 behind, generation 0, 1 dead letter\n"
        "- off: disabled, cursor 0, 1 behind, generation 0, 0 dead letters"
    )
    assert [s.rule for s in await workspace.rule_statuses()] == ["deploys", "off"]  # REST's list
    assert none == "No rules are registered."


async def test_the_http_app_and_lifespan(server: Server) -> None:
    mcp, _, _ = server
    assert mcp.http_app() is not None
    async with mcp.lifespan():
        pass
