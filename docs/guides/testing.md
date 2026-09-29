# Testing your application

Everything in reflexr runs in one process without a network: in-memory storage implements the whole storage protocol, time can be injected, the reactor can be driven until it settles, and pydantic-ai lets a scripted model stand in for a real one. So an application's tests can exercise its real rules, actions and surfaces, deterministically. reflexr's own tests work this way; this page shows the patterns.

The examples use [pytest-asyncio](https://pytest-asyncio.readthedocs.io) with `asyncio_mode = "auto"`, so async tests and fixtures need no decorators. They test an application whose rules and actions live in a module of its own, here `oncall`: the incident-response rules `error-spike`, `heartbeat-lost` and `deploy-regression`, their actions `triage`, `page` and `runbook`, and a `build_workspaces(storage, clock=...)` that builds the `Workspaces`.

## Workspaces in memory

Give each test fresh `InMemoryStorage`. It implements transactions that roll back, a live subscription and leases, so nothing needs mocking:

```python
import pytest

from reflexr import SourceActor
from reflexr.workspace import InMemoryStorage, Reactor, Settled, Workspace, Workspaces

from oncall import ACTIONS, AppDeps, Deploy, Heartbeat, Pager, ServiceError, build_workspaces


@pytest.fixture
def workspaces(clock: FakeClock) -> Workspaces:
    return build_workspaces(InMemoryStorage(clock=clock), clock=clock)


@pytest.fixture
async def monitor(workspaces: Workspaces) -> Workspace:
    return await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))


@pytest.fixture
def pager() -> Pager:
    return Pager()  # a fake that records what it was asked to send


@pytest.fixture
def reactor(workspaces: Workspaces, pager: Pager) -> Reactor[AppDeps]:
    return Reactor(workspaces, actions=ACTIONS, deps=AppDeps(pager=pager))
```

Fakes of the systems your actions call go in the reactor's `deps`, which is one more reason to reach them through the `Reaction` rather than through globals.

The log records everything that happened, in order and attributed, which makes it the natural thing to assert on:

```python
async def test_publishing_is_idempotent(monitor: Workspace) -> None:
    first = await monitor.publish(ServiceError(service="auth", severity=8), id="alert-7")
    again = await monitor.publish(ServiceError(service="auth", severity=8), id="alert-7")
    assert again.duplicate
    assert again.envelope == first.envelope
    assert [e.event_type for e in await monitor.read()] == ["service.error"]
```

Reads such as `monitor.runs(rule=...)`, `monitor.run(run_id)` and `monitor.dead_letters()` check the resulting state.

## Controlling time

Rules measure time by the log: each envelope's `ts`, assigned from the storage's clock when it is appended ([Time](rules.md#time)). Retries and schedules use the workspaces' clock. Give both a clock the test moves by hand, and windows, absences, backoff and ticks become deterministic:

```python
from datetime import UTC, datetime, timedelta

START = datetime(2026, 1, 1, tzinfo=UTC)


class FakeClock:
    """A clock the test moves by hand."""

    def __init__(self) -> None:
        self.now = START

    def __call__(self) -> datetime:
        return self.now

    def advance(self, **delta: float) -> None:
        self.now += timedelta(**delta)


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()
```

An absence needs time to pass in the log, not just on the clock. A [schedule](schedules.md)'s ticks move it forward, so this test pages when `auth` goes quiet for five minutes:

```python
async def test_a_quiet_service_is_paged(
    monitor: Workspace, reactor: Reactor[AppDeps], clock: FakeClock, pager: Pager
) -> None:
    await monitor.publish(Heartbeat(service="auth"))
    await reactor.settle()  # the clock schedule starts its timetable here
    clock.advance(minutes=6)
    settled = await reactor.settle()  # a tick moves the log's clock past the deadline
    assert settled == Settled(ticks=1, firings=1, attempts=1)
    assert pager.sent == ["auth"]
```

## Testing rules without storage

A rule's decisions come from pure functions in `reflexr.core`, so a rule can be tested with a list of envelopes and no storage, reactor or event loop. Build the envelopes with the times you want, start the rule with `begin`, and `evaluate`:

```python
from reflexr.core import Envelope, Event, begin, evaluate, needs

from oncall import error_spike


def envelope(seq: int, seconds: int, event: Event) -> Envelope:
    return Envelope(
        seq=seq,
        id=f"evt_{seq}",
        ts=START + timedelta(seconds=seconds),
        workspace_id="prod",
        actor=SourceActor(name="monitor"),
        correlation_id=f"evt_{seq}",
        event=event,
    )


def test_three_severe_errors_within_a_minute_fire_once() -> None:
    log = [
        envelope(1, 0, ServiceError(service="auth", severity=8)),
        envelope(2, 10, ServiceError(service="auth", severity=3)),  # not severe
        envelope(3, 20, ServiceError(service="billing", severity=9)),  # another scope
        envelope(4, 30, ServiceError(service="auth", severity=7)),
        envelope(5, 50, ServiceError(service="auth", severity=9)),
    ]
    progress = begin(error_spike, head_seq=0)
    assert needs(error_spike, progress, log) == {'["auth"]', '["billing"]'}
    evaluation = evaluate(error_spike, progress, {}, log)
    [firing] = evaluation.firings
    assert (firing.scope, firing.matched) == ({"service": "auth"}, (1, 4, 5))
    assert evaluation.progress.cursor == 5
```

`needs` says which scopes the batch touches; with no stored state they are all new, so `evaluate` gets an empty mapping. The `Evaluation` it returns holds the firings, the evaluation errors, the facts to append, the changed scope states and the new progress ([ADR-0017](../adr/0017-the-cores-evaluation-contract.md)).

reflexr's own rules are specified this way, as language-neutral JSON: each case in [`tests/conformance/cases/`](https://github.com/alexnodeland/reflexr/tree/main/tests/conformance/cases) gives a rule, envelopes with times, and the firings or errors expected. [`test_conformance.py`](https://github.com/alexnodeland/reflexr/blob/main/tests/core/test_conformance.py) evaluates each case in one batch and again one envelope at a time, with the state saved as JSON in between, as a storage host would, and checks that both decide the same. The same approach suits an application with many rules: a table of logs and the firings they should cause.

## Settling the reactor

`reactor.settle()` ticks, evaluates and executes until nothing more happens now, and returns what it did. Runs waiting to retry are left waiting, so a test moves the clock to let them go:

```python
async def test_a_failing_page_is_retried(
    monitor: Workspace, reactor: Reactor[AppDeps], clock: FakeClock, pager: Pager
) -> None:
    pager.failures = 1
    await monitor.publish(Heartbeat(service="auth"))
    await reactor.settle()
    clock.advance(minutes=6)
    await reactor.settle()
    [waiting] = await monitor.runs(rule="heartbeat-lost")
    assert (waiting.status, waiting.error) == ("retrying", "RuntimeError: the pager is down")
    clock.advance(seconds=1)  # the default retry policy waits a second
    await reactor.settle()
    [done] = await monitor.runs(rule="heartbeat-lost")
    assert (done.status, done.attempts) == ("succeeded", 2)
```

`settle` raises `RuntimeError` if work is still going on after 100 rounds, which in a test usually means rules that keep triggering each other. `reactor.evaluate()` and `reactor.execute()` run one half at a time, for tests that need to look between them, such as a run that is pending but not yet started.

## Scripting agents

Agent actions call a model, which tests replace with pydantic-ai's `FunctionModel`, answering each request from a function, or `TestModel`, which answers without being told how. Swap either into the agent with `agent.override`; runs the reactor starts inside the `with` block use it.

A scripted model decides what the agent does, so a test can check both the agent's effects and the run's output. This one opens an incident with the `EventContext` capability's `emit_event` tool, then answers with the structured output (pydantic-ai's output tool, `final_result`):

```python
from pydantic_ai import ModelMessage, ModelRequest, ModelResponse, ToolCallPart
from pydantic_ai.models.function import AgentInfo, FunctionModel

from oncall import IncidentOpened, triage_agent


def triage_script(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    requests = [m for m in messages if isinstance(m, ModelRequest)]
    if len(requests) == 1:  # the first request: open an incident
        incident = {"service": "auth", "summary": "token checks failing"}
        return ModelResponse(
            parts=[ToolCallPart("emit_event", {"type": "incident.opened", "fields": incident})]
        )
    return ModelResponse(  # then answer with the structured output
        parts=[ToolCallPart("final_result", {"severity": "high", "summary": "token checks"})]
    )


async def test_the_triage_agent_opens_an_incident(
    monitor: Workspace, reactor: Reactor[AppDeps]
) -> None:
    for _ in range(3):
        await monitor.publish(ServiceError(service="auth", severity=8))
    with triage_agent.override(model=FunctionModel(triage_script)):
        await reactor.settle()
    [done] = await monitor.runs(rule="error-spike")
    assert done.output == {"severity": "high", "summary": "token checks"}
    [opened] = [e for e in await monitor.read() if isinstance(e.event, IncidentOpened)]
    assert opened.actor.kind == "agent"
    assert opened.correlation_id == done.correlation_id
```

The `messages` a scripted model receives are what the agent was sent, so a test can also check the prompt, which describes the firing ([Agents](actions.md#agents)), and each tool's result. When the test only needs the run to finish, `TestModel(call_tools=[])` returns a valid output of the agent's type without calling any tools:

```python
from pydantic_ai.models.test import TestModel

with triage_agent.override(model=TestModel(call_tools=[])):
    await reactor.settle()
```

Building an agent from a model name such as `"anthropic:claude-sonnet-5-5"` needs that provider's package, and a real run needs its API key. Pass `defer_model_check=True` to the `Agent` so that tests can import it without either.

## Testing graphs

Graph actions checkpoint after each step, so the interesting test is a failure part-way through. Make a step's dependency fail once, and check where the retry resumed. In `oncall`, the `runbook` graph runs `diagnose`, then `mitigate`, which pages:

```python
from reflexr.core import RunProgressed


async def test_the_runbook_resumes_after_the_step_that_failed(
    monitor: Workspace, reactor: Reactor[AppDeps], clock: FakeClock, pager: Pager
) -> None:
    pager.failures = 1  # mitigate fails once
    await monitor.publish(Deploy(service="auth", version="v42"))
    await monitor.publish(ServiceError(service="auth", severity=8))
    await reactor.settle()
    [waiting] = await monitor.runs(rule="deploy-regression")
    assert (waiting.status, waiting.step) == ("retrying", "diagnose")
    clock.advance(seconds=30)
    await reactor.settle()
    [done] = await monitor.runs(rule="deploy-regression")
    assert done.output == "diagnosed auth; paged auth"  # diagnose ran once
    steps = [e.event.step for e in await monitor.read() if isinstance(e.event, RunProgressed)]
    assert steps == ["__start__", "diagnose", "mitigate", "__end__"]
```

`run.step` is the last step saved, and the `run_progressed` events list every boundary saved across attempts ([Graphs](actions.md#graphs)).

## The web surfaces

FastAPI's `TestClient` drives the router in process, over REST and the WebSocket. A `resolve_actor` that reads a test header lets one test act as several people:

```python
from collections.abc import Iterator

from fastapi import FastAPI
from fastapi.testclient import TestClient
from starlette.requests import HTTPConnection

from reflexr import Actor, UserActor
from reflexr.core import TenantId
from reflexr.fastapi import reflexr_router


async def resolve_actor(connection: HTTPConnection) -> tuple[TenantId, Actor]:
    return "acme", UserActor(id=connection.headers.get("x-user", "ada"))


@pytest.fixture
def client(workspaces: Workspaces) -> Iterator[TestClient]:
    app = FastAPI()
    app.include_router(reflexr_router(workspaces, resolve_actor=resolve_actor), prefix="/v1")
    with TestClient(app) as client:
        yield client


def test_a_client_publishes_and_follows_the_log(client: TestClient) -> None:
    error = {"type": "service.error", "service": "auth", "severity": 8}
    published = client.post("/v1/workspaces/prod/events", json={"event": error, "id": "alert-7"})
    assert published.json() == [
        {"type": "published", "seq": 1, "id": "alert-7", "duplicate": False}
    ]

    with client.websocket_connect(
        "/v1/workspaces/prod/stream", subprotocols=["reflexr.v1"]
    ) as socket:
        socket.send_json({"type": "hello", "protocol": "reflexr.v1", "resume_after_seq": 0})
        assert socket.receive_json()["type"] == "welcome"
        assert socket.receive_json()["event"]["type"] == "service.error"
        assert socket.receive_json() == {"type": "replay_complete", "up_to_seq": 1}
```

The MCP server is tested with the MCP SDK's in-process client, which talks to the server object directly:

```python
import json

from mcp import Client
from mcp.server.mcpserver import Context
from mcp.types import TextContent

from reflexr import ExternalAgentActor
from reflexr.mcp import ReflexrMcp


async def resolve_client(ctx: Context) -> tuple[TenantId, ExternalAgentActor]:
    return "acme", ExternalAgentActor(client_id="claude-code")


async def test_an_external_agent_publishes_over_mcp(workspaces: Workspaces) -> None:
    server = ReflexrMcp(workspaces, resolve=resolve_client)
    try:
        async with Client(server.server) as client:
            result = await client.call_tool(
                "publish_event",
                {"workspace_id": "prod", "event": {"type": "heartbeat", "service": "auth"}},
            )
            [content] = result.content
            assert isinstance(content, TextContent)
            assert json.loads(content.text)["seq"] == 1
    finally:
        await server.aclose()
```

`aclose()` stops the tasks the server starts to notify clients about runs, so none outlives the test's event loop. [Serving over REST and WebSocket](serving.md) and [External agents over MCP](mcp.md) cover the endpoints and tools.

## Tips

- pydantic-ai prints a banner when it first runs an agent without observability set up. Set `PYDANTIC_AI_NO_BANNER=1` in the test environment to keep output clean.
- For storage beyond memory, run the same tests over [SQL storage](storage.md#sql-storage) on SQLite with a temporary file: reflexr's own workspace behaviour suite runs unchanged on in-memory storage, SQLite and PostgreSQL.
- The [reference implementation](../reference-implementation.md) is tested end to end with a scripted model, the pattern to follow for a whole application.
