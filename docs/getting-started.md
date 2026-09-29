# Getting started

This page builds a small incident-response application: a monitoring system publishes errors, a rule notices when one service keeps failing, and a workflow responds, first a plain function that pages someone and then an agent that triages the problem. Along the way it covers the pieces every reflexr application has: event types, rules, actions, `Workspaces` and the `Reactor`.

## Install

reflexr needs Python 3.12 or newer. Until it is published to PyPI, install it from GitHub:

=== "uv"

    ```bash
    uv add "reflexr @ git+https://github.com/alexnodeland/reflexr"
    ```

=== "pip"

    ```bash
    pip install "reflexr @ git+https://github.com/alexnodeland/reflexr"
    ```

The core install covers event types, rules, workspaces with in-memory storage, the reactor, schedules, and agent and graph actions. The adapters are optional extras:

| Extra | Adds |
|---|---|
| `fastapi` | Publishing, REST and the WebSocket stream, as a FastAPI router ([Serving](guides/serving.md)) |
| `mcp` | An MCP server for external agents ([External agents over MCP](guides/mcp.md)) |
| `postgres` | [SQL storage](guides/storage.md) on PostgreSQL, with asyncpg |
| `sqlite` | SQL storage on SQLite, with aiosqlite |
| `sql` | SQL storage without a driver, if you bring your own |
| `otel` | `configure_telemetry`: the OpenTelemetry SDK, OTLP export and instrumentation in one call ([Observability](guides/observability.md)) |
| `langfuse` | Langfuse: whole traces, each run's session, and feedback as scores |
| `litellm` | Models over a LiteLLM proxy, with tenants' keys and guardrails ([The LLM gateway](guides/gateway.md)) |
| `evals` | evalr: feedback as datasets, evaluators as rules, replay experiments ([Feedback and evaluation](guides/evaluation.md)) |

Combine extras with commas, as in `"reflexr[fastapi,postgres] @ git+https://github.com/alexnodeland/reflexr"`. You also need the pydantic-ai extra for your model provider, such as `pydantic-ai-slim[anthropic]`, and its API key in the environment.

!!! note "evalr comes from GitHub too"

    The `evals` extra depends on [evalr](https://github.com/alexnodeland/evalr), which is not on PyPI yet either. reflexr's own repository pins it to a GitHub revision, but that pin does not travel with the package, so add evalr from GitHub yourself: `uv add "evalr @ git+https://github.com/alexnodeland/evalr"`.

## 1. Define event types

An event type is a Pydantic model that subclasses `Event`. Defining the class registers it under the name you give, which is its `type` on the wire.

```python
from reflexr import Event


class ServiceError(Event, name="service.error"):
    service: str
    severity: int
    message: str = ""


class IncidentOpened(Event, name="incident.opened"):
    service: str
    summary: str
```

Events are facts, so they are immutable, and unknown fields are rejected, so a producer's typo fails loudly. [Events and envelopes](guides/events.md) covers the rest.

## 2. Write a rule

A rule says what to watch, how to group it, and what to run. This one fires when a service reports three severe errors within a minute, at most once every fifteen minutes per service:

```python
from datetime import timedelta

from reflexr import F, Rule, by, on, run

error_spike = Rule(
    name="error-spike",
    description="Three severe errors from one service within a minute.",
    when=on(ServiceError)
    .where(F.severity >= 7)
    .count(at_least=3, within=timedelta(minutes=1))
    .at_most(1, per=timedelta(minutes=15)),
    scope=by(F.service),
    then=run("page"),
)
```

- `when` is the condition: `on` picks the event types, `where` filters them, `count` is the pattern that makes the rule fire, and `at_most` throttles it.
- `scope=by(F.service)` keeps separate counts for each service, so errors from `auth` and `billing` never add up, and a slow response for one never holds up the other.
- `then=run("page")` names the action to run. Rules are data and actions are code, so a rule refers to its action by name.
- `F.severity` is checked against `ServiceError`'s fields when the rule is built, so a typo fails at startup rather than silently never matching.

[Rules](guides/rules.md) covers every kind of condition: sequences, absences, deduplication and custom predicates.

## 3. Write an action

An action is what a firing runs. The simplest is an async function over a `Reaction`, which carries the run, the rule, the events that made it fire, a workspace handle that acts as the run, and your application's dependencies:

```python
from reflexr.workspace import Reaction


async def page(reaction: Reaction[None]) -> str:
    service = reaction.scope["service"]
    print(f"Paging the on-call engineer about {service}, run {reaction.run_id}")
    return f"paged about {service}"
```

Whatever the action returns is the run's output. If it raises, the attempt fails and the run is retried under the rule's retry policy. `reaction.run_id` is also an idempotency key: every attempt of the run sees the same id, so a pager that deduplicates on it pages once even if the run is retried. `Reaction[None]` says the application passes no dependencies of its own; pass your clients and settings there instead.

## 4. Open a workspace and react

`Workspaces` holds the rules and opens tenant-scoped workspace handles over a storage. Each workspace has one ordered log. The `Reactor` evaluates the rules over new events and runs what fires:

```python
import asyncio

from reflexr import SourceActor
from reflexr.workspace import InMemoryStorage, Reactor, Workspaces

workspaces = Workspaces(
    InMemoryStorage(), events=[ServiceError], emitted=[IncidentOpened], rules=[error_spike]
)
reactor = Reactor(workspaces, actions={"page": page})


async def main() -> None:
    monitor = SourceActor(name="monitor")
    workspace = await workspaces.open("acme", "prod", actor=monitor)

    await workspace.publish(ServiceError(service="auth", severity=8, message="token check failed"))
    await workspace.publish(ServiceError(service="billing", severity=9, message="card declined"))
    await workspace.publish(ServiceError(service="auth", severity=8, message="token check failed"))
    await workspace.publish(ServiceError(service="auth", severity=3, message="slow response"))
    await workspace.publish(ServiceError(service="auth", severity=9, message="token check failed"))

    await reactor.settle()  # evaluate the rules and run what fired, until nothing is left

    for envelope in await workspace.read():
        print(envelope.seq, envelope.actor.kind, envelope.event_type)


asyncio.run(main())
```

- `events=[...]` is an allowlist: clients cannot publish event types you did not list. `emitted=[...]` lists the types only runs may publish, such as the incident the agent in step 5 opens; clients are refused them.
- `open("acme", "prod", actor=monitor)` is the only place a tenant id enters, and every event published through the handle is attributed to its actor, here a monitoring system.
- `settle()` evaluates and executes until nothing more happens now, which suits scripts and tests. A service runs `serve()` instead, which keeps going; see step 6.

The rule counted the three severe errors from `auth`, ignored the mild one and `billing`'s single error, fired once, and ran `page`:

```text
Paging the on-call engineer about auth, run fir_59dd6d39e0f9af54
1 source service.error
2 source service.error
3 source service.error
4 source service.error
5 source service.error
6 system rule_fired
7 system run_started
8 system run_succeeded
```

reflexr's own facts are on the same log as your events: the firing (`rule_fired`, naming the events it matched) and each step of the run. `await workspace.runs()` returns the run itself, with its status, attempts and output. [The reactor](guides/reactor.md) explains evaluation, retries and ordering.

## 5. Let an agent triage

An action can be a pydantic-ai agent. Its dependencies are the `Reaction`, and the `EventContext` capability makes it a response to the firing: its prompt describes what fired, it can read back through the log, and it can publish the event types you allow:

```python
from typing import Literal

from pydantic import BaseModel
from pydantic_ai import Agent

from reflexr.agent import AgentAction, EventContext


class Triage(BaseModel):
    severity: Literal["low", "high", "critical"]
    summary: str


triage_agent = Agent(
    "anthropic:claude-sonnet-5-5",
    deps_type=Reaction[None],
    output_type=Triage,
    instructions="Decide how bad this is. Open an incident if a person should look now.",
    capabilities=[EventContext(emit=[IncidentOpened])],  # tools to read the log and emit
)
triage = AgentAction(triage_agent, name="triage")
```

Point the rule at it with `then=run(triage)`, and give it to the reactor with `actions={"triage": triage}`. When the rule fires, the agent is told:

```text
Rule "error-spike" fired for {"service": "auth"}. This is attempt 1.
The rule: Three severe errors from one service within a minute.
The events that made it fire, oldest first:
<event seq="1" type="service.error" at="..." by="source">{"message": "token check failed", ...}</event>
...

Respond to this firing.
```

Its structured output is the run's output, and the incident it opens is on the log, attributed to the agent and caused by the run:

```text
4 system rule_fired
5 system run_started
6 agent incident.opened
7 system run_succeeded
```

The exact tool calls depend on the model. Because the incident's envelope records the run as its cause, other rules can react to it, and the chain of events stays in one trace and one session. [Actions](guides/actions.md) covers agents, graphs with checkpoints, and functions in depth.

## 6. Serve it

A monitoring system or a frontend publishes over HTTP. Install the `fastapi` extra, include the router with your authentication, and run the reactor beside the application:

```python
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.requests import HTTPConnection

from reflexr import Actor, UserActor
from reflexr.core import TenantId
from reflexr.fastapi import Unauthorized, reflexr_router


async def resolve_actor(connection: HTTPConnection) -> tuple[TenantId, Actor]:
    user = await authenticate(connection)  # your application's authentication
    if user is None:
        raise Unauthorized("sign in first")
    return user.tenant_id, UserActor(id=user.id, name=user.name)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    stop = asyncio.Event()
    serving = asyncio.create_task(reactor.serve(stop=stop))  # evaluate and execute
    yield
    stop.set()  # at shutdown: let running actions end, then let go of every lease
    await serving


app = FastAPI(lifespan=lifespan)
app.include_router(reflexr_router(workspaces, resolve_actor=resolve_actor), prefix="/v1")
```

Then publish, and read the runs:

```bash
curl -X POST localhost:8000/v1/workspaces/prod/events -H "authorization: Bearer $TOKEN" \
  -H "content-type: application/json" \
  -d '{"event": {"type": "service.error", "service": "auth", "severity": 8}}'
# [{"type":"published","seq":1,"id":"evt_4d2b782c09fcbab4","duplicate":false}]

curl localhost:8000/v1/workspaces/prod/runs -H "authorization: Bearer $TOKEN"
```

Clients that want to watch connect a WebSocket to `/v1/workspaces/{workspace_id}/stream`, say `hello` with the last `seq` they saw, and receive the log from there followed by live events. [Serving over REST and WebSocket](guides/serving.md) covers every endpoint and authentication, and the [stream protocol](protocol.md) specifies every frame. Any number of processes can run a reactor over the same SQL storage; leases share the work ([Storage](guides/storage.md)).

## Next steps

- Read the [guides](guides/events.md), starting with [rules](guides/rules.md) and [the reactor](guides/reactor.md).
- Keep workspaces in [PostgreSQL or SQLite](guides/storage.md).
- Keep runaway workflows and spend in check: [Loop and spend safety](guides/safety.md).
- See how to [test](guides/testing.md) rules and actions without calling a model.
- Look at the [reference implementation](reference-implementation.md), `examples/oncall`: incident response with an agent, a runbook graph, a paging function, a schedule and every surface.
