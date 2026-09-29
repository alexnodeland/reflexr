# The reference implementation

[`examples/oncall`](https://github.com/alexnodeland/reflexr/tree/main/examples/oncall) is a small, complete application built on reflexr: incident response. Monitoring reports errors, CI reports deploys, and services send heartbeats; rules watch the log and run an agent, a graph and a function in response. It uses only the library's public API, and it is held to the same quality gates as the library ([ADR-0015](adr/0015-reference-implementation-oncall.md)). Read it when you want to see every piece of this documentation working together.

!!! note "Being built"

    `examples/oncall` is RFC-0001 phase 6, and is being built as this site is published. This page describes it from its design in [ADR-0015](adr/0015-reference-implementation-oncall.md); once it is on `main`, the links on this page lead to its code, and [its README](https://github.com/alexnodeland/reflexr/blob/main/examples/oncall/README.md) is the authority on names, commands and flags.

## What it shows

Incident response exercises every part of reflexr: windows and scopes, sequences, absence, agents, graphs, functions, schedules and every surface. It also pairs with artifactr, for the system that will combine the two: an opened incident becomes a document that people and an agent work on together.

| Piece | What it shows | Guide |
|---|---|---|
| Events | Event types that producers publish (errors, deploys, heartbeats) and that workflows emit (an incident opened, a page sent, a rollback proposed) | [Events and envelopes](guides/events.md) |
| A triage rule | A count of severe errors per service within a window, running a **triage agent**: a pydantic-ai agent with the `EventContext` capability that may emit an incident | [Rules](guides/rules.md), [Actions](guides/actions.md#agents) |
| A runbook rule | A deploy followed by errors from the same service, running a **runbook graph**: diagnose, decide, then roll back or escalate, checkpointed after every step | [Actions](guides/actions.md#graphs) |
| A silence rule | No heartbeat from a service for a while, running a **paging function**, idempotent by its run id | [Rules](guides/rules.md#patterns) |
| A schedule | Ticks that keep time moving in a quiet workspace, so a silence is noticed on time | [Schedules](guides/schedules.md) |
| A server | FastAPI with publishing, REST and the WebSocket stream, and MCP for external agents, running the reactor in its lifespan, over in-memory or SQL storage | [Serving](guides/serving.md), [MCP](guides/mcp.md), [Storage](guides/storage.md) |
| A terminal client | Publishes events, follows the stream live, and operates runs: a template for a client in any language | The [stream protocol](protocol.md) |
| Tests | The whole stack end to end with a scripted model and a clock the tests move, inside the coverage gate | [Testing your application](guides/testing.md) |

## How it fits together

```mermaid
graph LR
    error[/service.error/] --> spike{{"error spike<br/>severe errors from one<br/>service within a window"}}
    spike --> agent["triage agent<br/>pydantic-ai"]
    agent -- emit_event --> opened[/incident.opened/]
    deploy[/deploy.finished/] --> regression{{"deploy regression<br/>a deploy, then errors<br/>from the same service"}}
    error --> regression
    regression --> graph["runbook graph<br/>pydantic-graph"]
    graph --> rollback[/rollback.proposed/]
    heartbeat[/heartbeat/] --> lost{{"heartbeat lost<br/>no heartbeat from<br/>a service for a while"}}
    tick[/tick/] -. moves time .-> lost
    lost --> page["page<br/>function"]
    page --> sent[/page.sent/]
```

Events are slanted boxes, rules hexagons and actions rectangles. Everything one error leads to is in its causal chain: the firing, the run, the incident it opens and whatever that sets off share the error's `correlation_id`, so they are one session in a trace backend ([Observability](guides/observability.md#sessions-and-causal-chains)).

## Run it

oncall is a member of the repository's uv workspace, so it runs against the library in the same checkout. From the repository root, with a model provider's key, start the server, watch the workspace from one terminal and publish from another; the [README](https://github.com/alexnodeland/reflexr/blob/main/examples/oncall/README.md) lists the commands, and shows a session from the first alert to a resolved incident.

By default the server keeps its workspaces in memory. With a database URL in its configuration it keeps them in SQLite or PostgreSQL through [SQL storage](guides/storage.md#sql-storage), migrating the database when it starts.

!!! warning "Demo authentication"

    The reference implementation trusts whichever user a request names and puts everyone in one tenant. It shows where authentication plugs in, not how to do it. See [Multi-tenancy and security](guides/security.md).

## Test it

Its tests run the real server and client with a scripted model and a clock they move by hand, so they need no API key and never wait on timing:

```bash
uv run pytest examples/oncall/tests
```

[Testing your application](guides/testing.md) explains the patterns.
