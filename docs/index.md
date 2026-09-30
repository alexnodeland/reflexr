---
title: reflexr
description: A Python library for reactive agent workflows, in which rules over event streams run pydantic-ai agents, pydantic-graph graphs and functions.
hide:
  - navigation
---

<div class="reflexr-hero" markdown>

<h1 class="reflexr-visually-hidden">reflexr</h1>

![reflexr](assets/brand/lockup-light.svg#gh-light-mode-only)
![reflexr](assets/brand/lockup-dark.svg#gh-dark-mode-only)

A Python library for reactive agent workflows: rules watch streams of events, and when one's condition holds, it runs a pydantic-ai agent, a pydantic-graph graph or a plain function, exactly once per decision and at least once per action.

</div>

!!! note "Pre-release"

    reflexr has not been released yet. Everything described here is built and on `main`, as planned in [RFC-0001](rfcs/0001-v0.1-implementation-plan.md), including a [reference implementation](reference-implementation.md). The API may still change before the first release.

## Why: workflows that events start

Most LLM applications start with a person typing. Many useful workflows don't: three errors from one service within a minute, a deploy followed by a spike, a heartbeat that stops. Something has to watch for those patterns, decide exactly when they hold, and run a workflow in response, without running it twice, losing it in a crash, or letting it trigger itself forever.

reflexr is that something. Applications publish events into workspaces, each with one ordered log. Rules watch the log, and when a rule's condition holds, the reactor runs its action:

- **Rules are data.** A condition, a scope and an action, as a Pydantic model with a JSON Schema, so a rule can be listed, stored, diffed, validated and written by an agent as easily as by a person.
- **Deciding is exact.** Rules are pure reducers over the log, which is also their clock, so the same log always produces the same firings: testable without I/O, and replayable after a fix.
- **Acting is durable.** Every firing's run executes at least once under a lease, with retries, dead letters, ordering per scope and an idempotency key, and graph workflows resume after their last completed step.
- **Workflows stay in bounds.** Rules never react to their own activity, causal chains have a depth limit, and throttles, emit allowlists, timeouts and usage limits cap what a workflow can do and spend.

## A short example

A monitoring system publishes errors. When one service reports three severe errors within a minute, a function pages someone:

```python
import asyncio
from datetime import timedelta

from reflexr import Event, F, Rule, SourceActor, by, on, run
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspaces


class OpsEvent(Event, abstract=True, event_namespace="ops"):  # the application's namespace
    pass


class ServiceError(OpsEvent, name="service.error"):  # an event type, ops:service.error
    service: str
    severity: int


error_spike = Rule(  # a rule: typed, serializable data
    name="ops:error-spike",
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

The workspace's log tells the whole story, in order: the three severe errors and the mild one, the rule firing on the severe three, and the run of its action:

```text
1 source ops:service.error
2 source ops:service.error
3 source ops:service.error
4 source ops:service.error
5 system reflexr:rule_fired
6 system reflexr:run_started
7 system reflexr:run_succeeded
```

[Getting started](getting-started.md) builds on this step by step, with an agent that triages and opens an incident, and a server.

## What you get

- **Events as Pydantic models.** Subclass `Event`, and the library provides the registry, validation, idempotent publishing by id, and envelopes with their position, actor and causal chain. [Events and envelopes](guides/events.md)
- **Rules you can read and write as data.** Filters, deduplication, counts in a window, sequences, absences and throttles, scoped per field, checked against your event types at startup, with a JSON Schema for rules written elsewhere. [Rules](guides/rules.md)
- **Workspaces like artifactr's.** Tenant-scoped handles over one ordered log per workspace, bound to one actor, with operations on runs and rules recorded on the log. [Workspaces and the log](guides/workspaces.md)
- **A reactor that decides exactly and acts at least once.** Per-rule cursors, leases across any number of processes, retries with backoff, dead letters, ordering per scope, typed failures and replays. [The reactor](guides/reactor.md)
- **Agents, graphs and functions as actions.** A pydantic-ai agent with the `EventContext` capability, a pydantic-graph graph checkpointed at its step boundaries, or an `async def`, all over one `Reaction`. [Actions](guides/actions.md)
- **Schedules** that publish ticks on intervals or cron expressions, so time moves in quiet workspaces. [Schedules](guides/schedules.md)
- **Storage in memory, SQLite or PostgreSQL**, behind one protocol with one behaviour suite. [Storage](guides/storage.md)
- **Surfaces that behave the same.** Publishing, REST and a resumable WebSocket stream for your services and frontends, and an MCP server for external agents, all through one command handler. [Serving over REST and WebSocket](guides/serving.md), [External agents over MCP](guides/mcp.md)
- **Observability and evaluation built in.** OpenTelemetry spans and metrics with every run in its causal chain's session, Langfuse, typed feedback as scores, and evaluators as rules through evalr. [Observability](guides/observability.md), [Feedback and evaluation](guides/evaluation.md)
- **Rules you can port.** The decisions live in a pure, synchronous core, pinned by language-neutral conformance fixtures, and the library is held to 100% branch coverage and pyright strict. [Architecture](architecture.md)

## Where to go next

| If you want to | Read |
|---|---|
| Build something now | [Getting started](getting-started.md) |
| Understand one part in depth | The [guides](guides/events.md) |
| Look up a class or function | The [API reference](reference/index.md) |
| Publish from another service, or write a frontend | The [stream protocol](protocol.md) and its [JSON Schemas](reference/schema.md) |
| Understand why it is built this way | The [architecture](architecture.md) and the [decision records](adr/README.md) |
| Use it beside artifactr | [artifactr](https://artifactr.alexnodeland.com), its sibling for chats in which people and agents edit shared artifacts |
| Contribute | [Contributing](project/contributing.md) |
