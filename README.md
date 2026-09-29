<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/assets/brand/banner-dark.svg">
  <img alt="reflexr: rules that watch streams of events, and run agents, graphs and functions when they fire." src="docs/assets/brand/banner-light.svg" width="100%">
</picture>

<p>
  <a href="https://reflexr.alexnodeland.com"><img alt="Docs" src="https://img.shields.io/badge/docs-reflexr.alexnodeland.com-B3122E"></a>
  <a href="https://github.com/alexnodeland/reflexr/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/alexnodeland/reflexr/actions/workflows/ci.yml/badge.svg?branch=main"></a>
  <img alt="Python 3.12, 3.13 and 3.14" src="https://img.shields.io/badge/python-3.12%20%7C%203.13%20%7C%203.14-B3122E">
  <img alt="Coverage: 100%" src="https://img.shields.io/badge/coverage-100%25-B3122E">
  <img alt="Typed: pyright strict" src="https://img.shields.io/badge/typed-pyright%20strict-B3122E">
  <a href="LICENSE"><img alt="License: MIT" src="https://img.shields.io/badge/license-MIT-B3122E"></a>
</p>

**reflexr** is a Python library for **reactive agent workflows**: rules watch the event log of each workspace, and when a rule's condition holds (three errors from one service within a minute, a deploy followed by a spike, a heartbeat that stops), it runs a workflow: a [pydantic-ai](https://ai.pydantic.dev) agent, a pydantic-graph graph, or a plain async function.

It is the sibling of [artifactr](https://github.com/alexnodeland/artifactr): artifactr is for live chats in which people and agents edit shared artifacts, and reflexr is for workflows that events start. The two share their conventions (tenants and workspaces, envelopes, actors, storage, the protocol's shape), and neither imports the other.

> **Status:** pre-release. reflexr is built in the phases tracked by [RFC-0001](docs/rfcs/0001-v0.1-implementation-plan.md); everything but the reference implementation is on `main`, and the API may change before the first release. Work from before the rebuild is on the `archive/pre-rebuild` branch.

## Why

Most LLM applications start with a person typing. Many useful workflows start with something happening instead, and need something to watch for it, decide exactly when a pattern holds, and run a workflow in response, without running it twice, losing it in a crash, or letting it trigger itself forever. In reflexr, rules are typed data evaluated as pure reducers over the log, so the same log always makes the same decisions; runs execute at least once under leases, with retries, dead letters and an idempotency key; and depth limits, throttles and allowlists keep workflows in bounds.

## Install

Python 3.12 or newer. Until reflexr is on PyPI, install it from this repository:

```bash
uv add "reflexr[fastapi] @ git+https://github.com/alexnodeland/reflexr"
```

Extras: `fastapi` (publishing, REST and the WebSocket stream), `mcp` (external agents), `postgres` or `sqlite` (SQL storage with a driver), `sql` (SQL storage without one), `otel` (OpenTelemetry in one call), `langfuse` (traces and scores in Langfuse), `litellm` (the LLM gateway) and `evals` (evaluation with [evalr](https://github.com/alexnodeland/evalr), which you also install from GitHub for now).

## Example

A monitoring system publishes errors. When one service reports three severe errors within a minute, a function pages someone:

```python
import asyncio
from datetime import timedelta

from reflexr import Event, F, Rule, SourceActor, by, on, run
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspaces


class ServiceError(Event, name="service.error"):  # an event type: a Pydantic model
    service: str
    severity: int


error_spike = Rule(  # a rule: typed, serializable data
    name="error-spike",
    when=on(ServiceError).where(F.severity >= 7).count(at_least=3, within=timedelta(minutes=1)),
    scope=by(F.service),
    then=run("page"),
)


async def page(reaction: Reaction[None]) -> str:  # an action: a function, an agent or a graph
    return f"paged about {reaction.scope['service']}"


workspaces = Workspaces(InMemoryStorage(), events=[ServiceError], rules=[error_spike])
reactor = Reactor(workspaces, actions={"page": page})


async def main() -> None:
    workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))
    for severity in (8, 9, 3, 8):
        await workspace.publish(ServiceError(service="auth", severity=severity))
    await reactor.settle()  # evaluate the rules, then run what fired

    for envelope in await workspace.read():
        print(envelope.seq, envelope.actor.kind, envelope.event_type)


asyncio.run(main())
```

The log it prints shows the four errors, the rule firing on the three severe ones, and the run of its action, in order and attributed. Swap `page` for an `AgentAction` and the same rule runs a pydantic-ai agent that is told what fired, can read the log, and can publish the events you allow ([Getting started](https://reflexr.alexnodeland.com/getting-started/)).

## What you get

- **Events as Pydantic models**, published idempotently by id into tenant-scoped workspaces with one ordered log each.
- **Rules as data**: filters, deduplication, counts in a window, sequences, absences and throttles, scoped per field, with a JSON Schema.
- **Exact decisions**: a pure, synchronous core with per-rule cursors, where the log is the clock, pinned by language-neutral conformance fixtures.
- **Durable actions**: at-least-once runs under leases, retries, dead letters, ordering per scope, typed failures, replays, and graphs that resume after their last completed step.
- **Agents, graphs and functions** over one `Reaction`, with the `EventContext` capability for pydantic-ai agents.
- **Schedules** on intervals or cron expressions.
- **Storage in memory, SQLite or PostgreSQL**, behind one protocol.
- **Surfaces that behave the same**: publishing, REST and a resumable WebSocket stream, and MCP for external agents, through one command handler.
- **Observability and evaluation**: OpenTelemetry with every run in its causal chain's session, Langfuse, the LiteLLM gateway, typed feedback as scores, and evaluators as rules.
- **Safety by default**: no self-reaction, causation depth limits, throttles, emit allowlists, timeouts and usage limits.

## Try it

[`examples/oncall`](examples/oncall/README.md) is a complete application built on the library, for incident response: severe alerts run a triage agent that opens an incident, the incident runs a runbook graph that rolls back a recent deploy or pages a person, and a service that stops sending heartbeats is paged. It serves REST, the WebSocket stream and MCP, with a terminal client that watches the log live.

```sh
make install
export ANTHROPIC_API_KEY=...
uv run oncall-serve       # in one terminal
uv run oncall watch       # in another
uv run oncall alert api --severity 8 --message "5xx rate above 5%"   # three times, in a third
```

Set `ONCALL_DATABASE_URL` (for example `sqlite+aiosqlite:///oncall.db`) to keep its workspaces in a database instead of memory.

## Documentation

The documentation site is at **<https://reflexr.alexnodeland.com>**. It is built from [`docs/`](docs/index.md) and published from `main` on every push; run `make docs-serve` to read it locally at <http://localhost:8000>.

- [Getting started](docs/getting-started.md) and the [guides](docs/guides/events.md): events, rules, workspaces, the reactor, actions, schedules, storage, serving, MCP, security, observability, the LLM gateway, evaluation, safety and testing.
- [Architecture](docs/architecture.md): concepts, layers, rules, deciding and acting, actions, safety, tenancy and storage.
- [Stream protocol v1](docs/protocol.md): the REST, WebSocket and MCP contracts, with generated JSON Schemas for [the protocol](schemas/reflexr.v1.json) and [rules](schemas/reflexr.rules.v1.json).
- [Architecture decision records](docs/adr/README.md): why each part is the way it is.
- [RFCs](docs/rfcs/README.md): proposals and the v0.1 build plan.
- [Brand](docs/assets/brand/README.md): the mark, colours and type, and the family system reflexr shares with artifactr, evalr and stackr.

## Built on

[Pydantic](https://docs.pydantic.dev), [pydantic-ai](https://ai.pydantic.dev), [pydantic-graph](https://ai.pydantic.dev/graph/), [cronsim](https://github.com/cuu508/cronsim), [SQLAlchemy](https://www.sqlalchemy.org), [FastAPI](https://fastapi.tiangolo.com), the [MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk) and [OpenTelemetry](https://opentelemetry.io).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for setup, the trunk-based workflow, and the RFC and ADR process.

## License

[MIT](LICENSE)
