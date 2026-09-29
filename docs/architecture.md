# Architecture

> **Status:** accepted design, being built in the phases tracked by [RFC-0001](rfcs/0001-v0.1-implementation-plan.md). This document is evergreen: it is updated in the same pull request as the code that changes it, and the table below shows what exists today. Decisions are recorded in [`adr/`](adr/README.md), proposals in [`rfcs/`](rfcs/README.md), and the wire protocol in [`protocol.md`](protocol.md).

| Package | Status |
|---|---|
| `reflexr.core` | Implemented |
| `reflexr.workspace` | Planned (phase 2) |
| `reflexr.agent` | Planned (phase 3) |
| `reflexr.sql` | Planned (phase 4) |
| `reflexr.fastapi`, `reflexr.mcp` | Planned (phase 5) |
| `examples/oncall` | Planned (phase 6) |

## What reflexr is

reflexr is a Python library for **reactive agent workflows**. Applications publish events into workspaces. Rules watch each workspace's event log, and when a rule's condition holds (three errors from one service within a minute, a deploy followed by a spike, a heartbeat that stops), it runs a workflow: a pydantic-ai agent, a pydantic-graph graph, or a plain async function.

It is the sibling of [artifactr](https://github.com/alexnodeland/artifactr), and the two cover different ways of working with LLM agents:

| | artifactr | reflexr |
|---|---|---|
| Trigger | A person talks to an agent | Something happens in the world |
| Shape | Live chat, with shared artifacts both sides edit | Rules over a stream of events, running workflows in the background |
| Unit | A workspace of artifacts and threads, with one log | A workspace of events, with one log and the rules that watch it |
| Agent's job | Collaborate, turn by turn | Decide and act, then report |

Neither library imports the other. They share conventions (tenants and workspaces, envelopes, actors, storage protocols, pydantic-ai integration, protocol shape), so that a third system can bring them together over a shared context with thin adapters ([ADR-0003](adr/0003-independent-sibling-of-artifactr.md)).

### Goals

- Rules are typed, serializable data. Conditions compose, rules have a JSON Schema, and an agent can author one as easily as a person.
- Deciding is deterministic. The same log and the same rules always produce the same firings, so rules are testable without I/O and replayable after a fix.
- Rules are isolated. Each has its own cursor, state, retries and dead letters, so one failing rule never re-runs or blocks another.
- Acting is durable. Every firing's workflow runs at least once, with an idempotency key, and graph workflows resume from their last completed step after a crash.
- Multi-tenant, with tenants and workspaces exactly like artifactr's, scaled across processes with storage leases.
- Idiomatic use of Pydantic, pydantic-ai, pydantic-graph, SQLAlchemy, FastAPI and the MCP SDK, rather than parallel abstractions next to them.
- Small: seven core concepts, readable in an afternoon.

### Non-goals (for now)

- A general stream processor. There are no joins across workspaces, and a workspace's appends are serialized, so throughput per workspace is modest; scale comes from many workspaces.
- Exactly-once external effects. Actions run at least once and receive an idempotency key.
- Connectors for every broker. Applications publish over the API or in process; Kafka or SQS consumers can be written against the same `publish`.
- A frontend.

## Core concepts

| Concept | What it is |
|---|---|
| **Event** | A Pydantic model subclass for one kind of fact (`service.error`, `deploy.finished`), registered by name. Stored in an **envelope** with its `seq`, time, actor and causation. |
| **Workspace** | A tenant-scoped unit with one append-only log of events (its stream), and the rules' cursors and runs over it. The unit of ordering, isolation and scale, as in artifactr. |
| **Rule** | Typed data: a condition over events (`when`), a scope that partitions its state and ordering (`scope`), and the action it runs (`then`). |
| **Firing** | The durable record that a rule's condition held for one scope at one point in the log, with the events that matched. Written atomically with the rule's cursor and state. |
| **Action** | What a firing runs: a pydantic-ai agent, a pydantic-graph graph or an async function, registered under a name that rules refer to. |
| **Run** | One execution of an action for one firing. Retried until it succeeds or is dead-lettered; its lifecycle is recorded in the log. |
| **Reactor** | The runtime that evaluates rules and executes runs across workspaces, in any number of processes. |

Supporting types: `Condition` (the stages of a rule's `when`), `Scope`, `Schedule`, `Actor`, and `Reaction` (the dependencies an action receives: the workspace, the firing, the run and the application's own deps).

## Layers

```mermaid
graph TD
    app["Your application"] --> fastapi["reflexr.fastapi<br/>ingest, REST, WebSocket"]
    app --> mcp["reflexr.mcp<br/>MCP server"]
    app --> agent["reflexr.agent<br/>agent and graph actions"]
    fastapi --> workspace
    mcp --> workspace
    agent --> workspace["reflexr.workspace<br/>workspaces, storage protocol, Reactor"]
    sql["reflexr.sql<br/>SQLAlchemy storage"] --> workspace
    workspace --> core["reflexr.core<br/>pure, synchronous rules"]
```

Dependencies point one way. Each layer is usable without the ones above it, and a test enforces the imports.

| Package | Depends on | Responsibility |
|---|---|---|
| `reflexr.core` | pydantic | Events and envelopes, actors, conditions and their reducers, rules, evaluation, the run lifecycle and retry policy. Pure, synchronous, no I/O. |
| `reflexr.workspace` | core | `Workspaces`, `Workspace`, the storage protocol, in-memory storage, the `Reactor`, function actions, schedules and their runner. |
| `reflexr.agent` | workspace, pydantic-ai, pydantic-graph | `Reaction`, agent actions with the `EventContext` capability, graph actions with checkpoints. |
| `reflexr.sql` (extra) | workspace, SQLAlchemy 2 async, Alembic | Durable storage on PostgreSQL and SQLite, and its migrations. |
| `reflexr.fastapi` (extra) | workspace, FastAPI | HTTP ingest, REST reads and administration, and the WebSocket stream protocol. |
| `reflexr.mcp` (extra) | workspace, mcp | Publishing, reading and administration as MCP tools. |

### `reflexr.core`: sans-IO

Core holds every decision as plain functions over immutable values ([ADR-0001](adr/0001-library-with-a-sans-io-core.md), [ADR-0017](adr/0017-the-cores-evaluation-contract.md)). The host (the workspace layer, or any other) asks core what to load, loads it, lets core decide, and saves what core returns in one transaction:

```python
scopes = core.needs(rule, progress, batch, predicates=predicates)
evaluation = core.evaluate(rule, progress, load(scopes), batch, predicates=predicates)
save(evaluation)  # scope states, the new progress and cursor, and the firings' runs and events
```

- `needs` returns the scopes a batch reads: those of the envelopes that pass the rule's filter, and those whose `absence` deadline passes during the batch. Scopes the host has no state for are new.
- `evaluate` folds the envelopes into the rule's per-scope state and returns an `Evaluation`: the changed scope states, the new `RuleProgress` (cursor, generation, definition hash, deadlines), the firings, the evaluation errors, and the `rule_fired` and `rule_errored` events to append, in order. It reads nothing but its arguments; time is the envelopes' `ts`.
- `begin` gives a new rule its starting progress (the head of the log, or the beginning), and `reset` starts a new generation when a rule's definition changes or it is replayed.
- The run lifecycle is pure too: `create_run`, `start`, `checkpoint`, `succeed`, `fail` (retry under the rule's `RetryPolicy`, or dead-letter), `cancel`, `skip`, `retry`, and `runnable`, which picks the runs of a scope that may start now.

Firing ids are derived from the rule, its generation, the scope and the `seq`, so evaluating the same log twice produces the same ids.

Because core is pure, its behaviour is pinned by a **conformance suite** of JSON fixtures in `tests/conformance/cases/`: given a rule and a sequence of envelopes, expect these firings or these errors. Every case is also evaluated one envelope at a time with the state saved as JSON between batches, and property tests check that any batching of any log decides the same. The fixtures are the language-neutral specification.

## Events and envelopes

An event type is a Pydantic model that subclasses `Event`. Defining the class registers it under a name derived from the class name (`ServiceError` becomes `service_error`), or the name given explicitly, the same convention as artifactr's artifact types and pydantic-ai's `CustomEvent`:

```python
class ServiceError(Event, name="service.error"):
    service: str
    severity: int
    message: str


class Deploy(Event, name="deploy.finished"):
    service: str
    version: str
```

A stored event is an `Envelope`, which is also its wire shape:

```python
class Envelope(BaseModel):
    seq: int  # gap-free position in the workspace's log, from 1
    id: str  # the event's id: publishing the same id again is a no-op
    ts: datetime  # when it was appended; the clock rules use
    workspace_id: str
    actor: Actor  # who published it
    causation: Causation | None  # the firing and run that emitted it, and the chain's depth
    correlation_id: str  # the id of the first event in its causal chain
    event: Event  # discriminated by `type`
```

- **Publishing is idempotent** by event id within a workspace, so producers can retry, and a run that retries does not duplicate the events it emits.
- **Application events form an open family**: any registered `Event` subclass. reflexr's own facts form a **closed** union, so pyright checks `match` blocks for exhaustiveness: `RuleFired`, `RuleErrored`, `RuleReset`, `RunStarted`, `RunProgressed`, `RunRetrying`, `RunSucceeded`, `RunDeadLettered`, `RunCancelled`, `RunSkipped`, and `Tick` from schedules. An event type from a newer version validates as `UnknownEvent` and round-trips unchanged.
- **Actors** match artifactr's kinds: `UserActor`, `AgentActor` (an agent or graph run, by rule and run), `ExternalAgentActor` (an MCP client) and `SystemActor` (the reactor and schedules), plus `SourceActor` for systems that publish events, such as a monitoring service.
- **Type allowlist.** `Workspaces(storage, events=[ServiceError, Deploy, Heartbeat])` rejects publishing any other type, even one registered elsewhere in the process.

## Rules

A rule is a Pydantic model: what to watch, how to partition it, and what to run ([ADR-0006](adr/0006-rules-as-typed-serializable-data.md)).

```python
from datetime import timedelta

from reflexr import F, Rule, by, on, run

error_spike = Rule(
    name="error-spike",
    when=on(ServiceError)
    .where(F.severity >= 7)
    .count(at_least=3, within=timedelta(minutes=1))
    .at_most(1, per=timedelta(minutes=15)),
    scope=by(F.service),
    then=run(triage),
)
```

Serialized, the same rule is plain JSON, with a schema generated from the models:

```json
{
  "name": "error-spike",
  "when": {
    "filter": {"kind": "all", "of": [
      {"kind": "on", "types": ["service.error"]},
      {"kind": "where", "field": "severity", "op": "ge", "value": 7}
    ]},
    "pattern": {"kind": "count", "at_least": 3, "within": "PT1M"},
    "throttle": {"at_most": 1, "per": "PT15M"}
  },
  "scope": {"fields": ["service"]},
  "then": {"action": "triage"}
}
```

### Conditions

A condition is a pipeline of stages, evaluated per scope:

```mermaid
graph LR
    envelope --> filter["filter<br/>(stateless)"]
    filter --> dedupe["dedupe<br/>(optional)"]
    dedupe --> pattern["pattern<br/>(stateful)"]
    pattern --> throttle["throttle<br/>(optional)"]
    throttle --> firing
```

| Stage | Kinds | State |
|---|---|---|
| **filter** | `on(types)`, `where(field op value)`, `all`, `any`, `not`, and `predicate(name)`, a registered Python function as an escape hatch | None |
| **dedupe** | Drop an event whose key was seen within a window | Recent keys |
| **pattern** | `each` (the default: every event that gets through fires), `count(at_least, within)`, `sequence(steps, within)` (A then B), `absence(within)` (nothing matched for a while) | Per pattern |
| **throttle** | At most N firings per period, a cooldown that bounds spend | Recent firings |

Field references (`F.severity`, `F.labels.env`) are checked against the event types the filter admits when the condition is built, and again by `Rule.check` when rules are registered, so a typo fails at startup rather than silently never matching. Operators are `eq`, `ne`, `lt`, `le`, `gt`, `ge`, `in`, `contains`, `matches` and `exists`. Ordering compares numbers with numbers and strings with strings, and a missing field matches nothing but `exists=False`. Equality is spelled `F.service.eq("auth")`, or `.where(service="auth")`, because `==` must return a bool.

The builder names each stage: `.where(...)`, `.distinct(key, within=)` for dedupe, `.count(at_least=, within=)`, `.absent(within=)`, `sequence(step, step, within=)`, and `.at_most(n, per=)` for the throttle. Their semantics:

- **count** fires when enough envelopes pass within the window, and the firing consumes them.
- **sequence** fires when its steps match in order within the window of the first; a new first step while it waits restarts it, so the latest start counts.
- **absence** fires once when a scope that has been seen goes quiet for the window, and rearms with the next matching envelope.

### Scopes

`scope=by(F.service)` gives a rule independent state and ordering per service: three errors from `auth` and two from `billing` are two counts, and a slow run for `auth` never delays `billing`. The default scope is the whole workspace. An envelope that passes a rule's filter but lacks its scope fields is an evaluation error for that rule.

### Time

Rules measure time by the log: an envelope's `ts`, assigned when it is appended ([ADR-0007](adr/0007-rule-state-as-pure-reducers.md)). Evaluation never reads a clock, so replaying a log reproduces its firings exactly.

Every envelope advances a rule's clock, including envelopes its filter rejects: time is a separate input to the stateful stages, not an event they have to match. When the clock passes a deadline, such as the end of an `absence` window, it applies to every scope that already has state. So a `Tick`, which matches no heartbeat filter and has no `service` field, still lets `absence` fire for each service the rule has seen. A [schedule](#schedules) appending a `Tick` every so often guarantees that time keeps moving in a quiet workspace.

## Deciding and acting

Processing a workspace's log has two halves with different guarantees ([ADR-0005](adr/0005-per-rule-cursors.md)):

```mermaid
sequenceDiagram
    participant P as Producer
    participant S as Workspace log
    participant E as Evaluator (per workspace, leased)
    participant C as core.evaluate
    participant X as Executor (per run, leased)
    participant A as Action
    P->>S: publish(event) assigns seq
    E->>S: read after the rule's cursor
    E->>C: evaluate(rule, states, envelopes)
    C-->>E: states', firings
    E->>S: one transaction: states', cursor, RuleFired, pending runs
    X->>S: claim a runnable run (lease)
    X->>A: run(action, Reaction)
    alt succeeds
        X->>S: RunSucceeded
    else raises
        X->>S: RunRetrying with next_attempt_at, or RunDeadLettered
    end
    A-->>S: emitted events (causation = firing and run)
```

**Deciding** is exact. One evaluator holds each workspace's lease at a time and evaluates every rule in one pass over new envelopes. For each rule it loads the state of the scopes involved, calls `core.evaluate`, and saves the new state, the advanced cursor, the `RuleFired` envelopes and the pending runs in a single transaction. A crash before the commit means the same envelopes are evaluated again against the same state, so each envelope affects each rule exactly once. An evaluation error (a predicate that raises, a missing scope field) is recorded as `RuleErrored` and dead-lettered for that rule alone; the rule moves on.

Each rule has its **own cursor**. A rule added later, or reset for replay, catches up on its own without holding back the others.

**Acting** is at least once. Executors claim runnable runs under leases that they renew while working, so a crashed executor's run is picked up again when its lease lapses. With the default `ordering="scope"`, runs of one rule and scope execute in firing order: a run that fails and is waiting to retry holds back later runs of the same scope, and nothing else. A run that exhausts its retry policy is dead-lettered; later runs of its scope continue (`on_dead_letter="continue"`, the default) or wait for someone to retry or skip it (`"block"`).

The firing id doubles as the run id and the action's **idempotency key**, and events a run emits get ids derived from it, so a retried run does not publish duplicates.

## Actions

An action is what a firing runs ([ADR-0008](adr/0008-actions-agents-graphs-and-functions.md)). Every kind receives the same `Reaction`: the workspace (bound to the run's actor), the firing and its matched events, the run id and attempt, and the application's deps.

```python
# A function
async def page(reaction: Reaction[AppDeps]) -> None:
    await reaction.deps.pager.notify(reaction.firing.scope, reaction.run_id)


# A pydantic-ai agent: plain Agent, reflexr's capability adds the firing and event tools
triage_agent = Agent(
    "anthropic:claude-sonnet-5-5",
    deps_type=Reaction[AppDeps],
    output_type=Triage,
    capabilities=[EventContext(emit=[IncidentOpened], lookback=timedelta(hours=1))],
)
triage = AgentAction(triage_agent, name="triage")

# A pydantic-graph graph, checkpointed after every step
runbook = GraphAction(runbook_graph, name="runbook", state=RunbookState, inputs=from_triage)
```

- **Agents** are plain pydantic-ai `Agent`s with `deps_type=Reaction[...]`. The `EventContext` capability renders the firing (the rule, the scope, the matched events) into the instructions, and gives the agent tools to read back through the workspace's log and to emit events of the types it is allowed. The run's output can be emitted as an event, or handled by the action.
- **Graphs** are pydantic-graph graphs built with `GraphBuilder`. reflexr drives them step by step and saves the graph state and pending tasks to the run after every step. A retry, or another executor after a crash, resumes from the last completed step instead of starting over ([ADR-0009](adr/0009-graph-checkpoints.md)).
- **Functions** are `async def` over a `Reaction`.

Rules refer to actions by name (`{"action": "triage"}`), because functions and agents are not data. `run(triage)` takes the action's name. The application gives the reactor its actions, and `Rule.check` confirms at startup that every rule's action, event types, fields and predicates exist.

## Safety

LLM workflows triggered by events can loop and can spend ([ADR-0010](adr/0010-loop-and-spend-safety.md)):

- **No self-reaction.** A rule never sees reflexr's facts about itself (its own firings, errors and runs), though they still move its clock, so it cannot fire on its own activity.
- **Causation depth.** An event emitted by a run carries the depth of its causal chain. Publishing beyond the workspace's limit (8 by default) is rejected, so a rule whose action triggers itself stops instead of running away.
- **Throttles** on rules cap how often a rule can fire per scope.
- **Emit allowlists** on the `EventContext` capability limit which event types an agent can publish.
- **Concurrency limits** per reactor, per rule and per workspace bound how many runs execute at once. Agent actions accept pydantic-ai `UsageLimits`.
- **Tenancy**: every handle is scoped to one tenant and workspace.

## Tenancy and concurrency

- **Scoped handles.** `await workspaces.open(tenant_id, workspace_id, actor=...)` returns a `Workspace` bound to that tenant, workspace and actor, exactly as in artifactr. Nothing below it accepts a raw tenant id ([ADR-0016](adr/0016-tenants-and-workspaces-like-artifactr.md)).
- **Sequencing.** `seq` is assigned inside the append transaction, which holds the workspace's lock (in SQL, the workspace row, locked `FOR UPDATE`), giving a gap-free total order per workspace. `ts` is assigned under the same lock as the later of the clock and the previous envelope's `ts`, so it never decreases within a workspace, whatever the clock skew between processes. Appends to one workspace are serialized; many workspaces scale out.
- **Leases.** One evaluator per workspace, one executor per run, and one schedule runner per schedule, each a storage lease with a time-to-live that its holder renews and that lapses if it dies. Any number of reactor processes can share the work.

## Schedules

A schedule appends events on a timetable: `Schedule(name="heartbeat-check", every=timedelta(seconds=30), workspaces=[...])`, or a cron expression with a time zone. Each tick's event id is derived from the schedule and the tick's time, so replicas that race publish it once, and a lease keeps them from racing in the first place. Missed ticks are skipped or caught up, per schedule.

## Surfaces

Every surface is a thin adapter over a `Workspace` handle ([ADR-0011](adr/0011-surfaces.md)). The wire formats are in [`protocol.md`](protocol.md).

| Surface | Package | What it offers |
|---|---|---|
| HTTP ingest and REST | `reflexr.fastapi` | Publish events (one or a batch, idempotent). Read the log. Inspect and administer rules (cursor, lag, replay), runs (retry, skip, cancel) and dead letters. |
| WebSocket | `reflexr.fastapi` | A resumable subscription to the log with the same `hello`, replay and close-code shape as artifactr's thread protocol, plus publish and administration commands. |
| Schedules | `reflexr.workspace` | Cron and interval schedules that publish into workspaces. |
| MCP | `reflexr.mcp` | Publishing, reading and administration as MCP tools, so external agents can feed and operate workspaces. |

Authentication is the host's: each surface takes a resolver that returns the tenant and actor for a request.

## Storage protocol

`reflexr.workspace` defines the storage protocol, and the same behaviour suite runs against every implementation:

- **`InMemoryStorage`**, for tests, examples and single-process prototypes.
- **`SqlStorage`** (`reflexr.sql`), on PostgreSQL and SQLite with SQLAlchemy 2's asyncio extension, with Alembic migrations shipped in the package.

A transaction is scoped to one workspace. Within it the host appends envelopes, loads and saves rule cursors and states, creates and updates runs, and dead-letters evaluation errors. Outside transactions, storage serves reads (`read`, `subscribe`, runs, dead letters), leases with an injectable clock, and schedule state.

## Data model

```mermaid
erDiagram
    TENANT ||--o{ WORKSPACE : owns
    WORKSPACE ||--o{ EVENT : "log ordered by seq"
    WORKSPACE ||--o{ RULE_CURSOR : "one per rule"
    RULE_CURSOR ||--o{ RULE_STATE : "one per scope"
    WORKSPACE ||--o{ RUN : "one per firing"
    RUN }o--|| EVENT : "fired at"
    WORKSPACE ||--o{ DEAD_LETTER : "per rule"
```

- **Rules live in code** (v0.1) and are identified by name. A rule's cursor row stores a hash of its definition; changing the definition resets its state and is recorded in the log.
- **Rule state** is a JSON document per rule and scope, owned by the rule's condition, saved with its cursor.
- **Runs** hold their status, attempts, next attempt time, lease, and for graphs the latest checkpoint.

## Aligned with artifactr

| Convention | artifactr | reflexr |
|---|---|---|
| Tenancy | Tenants own workspaces; `workspaces.open(tenant, workspace, actor=)` | The same: tenants own workspaces, opened the same way |
| Log | One per workspace, envelopes with a gap-free `seq` | The same |
| Actors | user, agent, external agent, system | The same, plus source |
| Types | `Artifact` subclasses registered by name | `Event` subclasses registered by name |
| Core | Sans-IO `commit`, conformance fixtures | Sans-IO `evaluate`, conformance fixtures |
| Storage | Protocol, in-memory and SQL, leases, one behaviour suite | The same |
| pydantic-ai | A capability plus a deps type (`ArtifactWorkspace`, `Session`) | A capability plus a deps type (`EventContext`, `Reaction`) |
| WebSocket | `hello`, replay, `replay_complete`, close codes | The same shape |
| MCP | Tenant in resource URIs | The same |

## Dependencies

Python 3.12+. Runtime: `pydantic` (core); `pydantic-ai-slim` and `pydantic-graph` (agent). Extras: `sql` (`sqlalchemy[asyncio]`, `alembic`), `postgres` and `sqlite` (drivers), `fastapi`, `mcp`. Tooling: uv, ruff, pyright in strict mode, pytest, and Zensical with mkdocstrings for the documentation site.

## Testing

- **Core:** the conformance fixtures, plus property tests that replaying any log reproduces its firings.
- **Workspace:** one behaviour suite (publishing, evaluation, runs, retries, leases, schedules) against every storage.
- **Agent:** scripted models with pydantic-ai's `FunctionModel` and `TestModel`; graph runs interrupted and resumed.
- **Surfaces:** contract tests for every frame and endpoint, and an MCP client round trip.
- **Reference implementation:** the real server and client end to end, with a scripted model.

Coverage is 100% of lines and branches, and pyright runs in strict mode with no suppressions ([ADR-0013](adr/0013-quality-gates.md)).

## Decisions

| ADR | Decision |
|---|---|
| [0001](adr/0001-library-with-a-sans-io-core.md) | A library with a sans-IO core, replacing the template |
| [0002](adr/0002-the-name-reflexr.md) | The name reflexr |
| [0003](adr/0003-independent-sibling-of-artifactr.md) | An independent sibling of artifactr, with aligned conventions |
| [0004](adr/0004-tenant-scoped-streams.md) | Tenant-scoped streams with one log each (superseded by 0016) |
| [0005](adr/0005-per-rule-cursors.md) | Per-rule cursors: decide exactly, act at least once |
| [0006](adr/0006-rules-as-typed-serializable-data.md) | Rules as typed, serializable data |
| [0007](adr/0007-rule-state-as-pure-reducers.md) | Rule state as pure reducers, with the log as the clock |
| [0008](adr/0008-actions-agents-graphs-and-functions.md) | Actions: agents, graphs and functions over one `Reaction` |
| [0009](adr/0009-graph-checkpoints.md) | Graph checkpoints at step boundaries |
| [0010](adr/0010-loop-and-spend-safety.md) | Loop and spend safety |
| [0011](adr/0011-surfaces.md) | Surfaces: ingest, REST, WebSocket, schedules and MCP |
| [0012](adr/0012-trunk-based-development-with-rfcs-and-adrs.md) | Trunk-based development with RFCs, ADRs and evergreen docs |
| [0013](adr/0013-quality-gates.md) | Quality gates |
| [0014](adr/0014-mit-license.md) | MIT license |
| [0015](adr/0015-reference-implementation-oncall.md) | Reference implementation: incident response |
| [0016](adr/0016-tenants-and-workspaces-like-artifactr.md) | Tenants and workspaces, like artifactr |
| [0017](adr/0017-the-cores-evaluation-contract.md) | The core's evaluation contract |
| [0018](adr/0018-opentelemetry-observability-with-langfuse.md) | OpenTelemetry-native observability, with Langfuse primary |
| [0019](adr/0019-typed-feedback-as-events.md) | Typed feedback as events, mirrored to Langfuse |
| [0020](adr/0020-evalr-shared-eval-kit.md) | evalr, a shared eval kit |
| [0021](adr/0021-contributor-compose-and-dev-containers.md) | Contributor Compose and dev containers here, infrastructure in stackr |
| [0022](adr/0022-litellm-proxy-first.md) | LiteLLM, proxy first, for routing and guardrails |
| [0023](adr/0023-libraries-and-the-stackr-template.md) | Libraries, and stackr as the infrastructure template |

## Open questions

- **Managing rules at runtime.** Rules are data from day one, but v0.1 registers them in code. Storing versioned rules per tenant, and installing them through the API, is the next step, and what lets an agent's proposed rule go live once a person accepts it.
- **Pausing runs for a person.** A workflow that needs approval mid-way could pause as pydantic-ai deferred tools do. Until then, a run can emit an event and a second rule can continue when the answer arrives.
- **Live output from runs.** Token-level streaming of agent runs over the WebSocket, as artifactr streams its runs.
- **Resuming inside parallel branches.** Graph checkpoints are proven for sequential steps; a run that crashes inside a fork restarts from the last checkpoint before it.
- **Retention.** Compacting old envelopes, and what a rule replaying past the retained log starts from.
- **Hot workspaces.** A workspace's appends are serialized. Partitioning one workspace's log by key may be needed for high-volume sources.
