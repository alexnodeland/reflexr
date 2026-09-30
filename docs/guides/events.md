# Events and envelopes

Everything in reflexr starts with an event: a fact that something happened, such as a service error, a finished deploy or a heartbeat. Applications publish events into a workspace's log, rules watch the log, and reflexr records its own facts (a rule fired, a run succeeded) in the same log. This page covers defining event types, publishing them, the envelope every stored event travels in, the actors events are attributed to, reflexr's own events, and events of types a process does not know.

## Defining event types

An event type is a Pydantic model that subclasses `Event`, through a base that declares its owner's namespace ([ADR-0039](../adr/0039-namespaced-event-types.md)). Defining the class registers it under the namespace, a `:`, and the name given with `name=`, as artifactr names its artifact types:

```python
from reflexr import Event


class OpsEvent(Event, abstract=True, event_namespace="ops"):
    """Every event of this application, in the ops namespace."""


class ServiceError(OpsEvent, name="service.error"):  # ops:service.error
    service: str
    severity: int
    message: str = ""


class Deploy(OpsEvent, name="deploy.finished"):  # ops:deploy.finished
    service: str
    version: str


class Heartbeat(OpsEvent, name="heartbeat"):  # ops:heartbeat
    service: str


class IncidentOpened(OpsEvent, name="incident.opened"):  # ops:incident.opened
    service: str
    summary: str
```

Without `name=`, the local name is derived from the class name: `class ServiceRestarted(OpsEvent)` is registered as `ops:service_restarted`. Dotted local names such as `service.error` read well on the wire and in rules.

- **Every name is qualified.** A name is a namespace, a `:` and a local name, in lowercase: `ops:service.error`. Applications, libraries, plugins and bridges each declare their own namespace, so their types never clash, and reflexr's own facts are in `reflexr`. A concrete type with no namespace fails when it is defined.
- **A namespace belongs to one base**, declared once per process with `event_namespace=` on an `abstract=True` base, and its types inherit it. Declaring it again elsewhere raises `TypeError`, naming both; importing the same module again is fine.
- **Events are facts**, so they are frozen, and unknown fields are rejected: a producer's typo, such as `sevrity=9`, fails validation instead of being stored and never matching.
- **Fields are ordinary Pydantic fields**: defaults, constraints, nested models and lists all work. Rules refer to nested fields with dotted paths, such as `F.labels.env`.
- **A name belongs to one class.** Registering a second class under a name that is taken raises `TypeError`, so two modules cannot silently share a type.
- **A shared base class** that should not be a type of its own is declared with `abstract=True`:

```python
class ServiceEvent(OpsEvent, abstract=True):  # a shared base, not registered itself
    service: str
    region: str = "eu"


class ServiceDegraded(ServiceEvent, name="service.degraded"):  # ops:service.degraded
    reason: str
```

A name without a namespace fails wherever it appears, in a rule, a publish or a filter, with a hint that names the qualified type: `event type 'service.error' has no namespace; did you mean 'ops:service.error'?`. `check_event_name(name)` makes the same check, and `EventName` is the annotated type that makes it in a model.

### Registries

An `EventRegistry` is a set of namespaces, read as a mapping of their types by name. `DEFAULT_REGISTRY` holds every namespace whose base names no other registry, so `DEFAULT_REGISTRY["ops:service.error"]` is `ServiceError`. reflexr's own namespace is in every registry.

A registry keeps applications in one process apart. A base's `registry=` puts its namespace in a registry of its own, `registry.add(*bases)` includes another's, such as a library's, and `Workspaces(registry=...)` then accepts, and checks rules against, only that registry's types:

```python
from reflexr.core import EventRegistry
from reflexr.workspace import InMemoryStorage, Workspaces

BILLING = EventRegistry()


class BillingEvent(Event, abstract=True, event_namespace="billing", registry=BILLING):
    """Another application's events, kept out of the default registry."""


class InvoicePaid(BillingEvent, name="invoice.paid"):
    invoice: str


BILLING.add(OpsEvent)  # and the ops application's types too
billing = Workspaces(InMemoryStorage(), registry=BILLING)
```

There is still one table of types in the process, so a name means the same type in every registry, and every surface parses it one way; a registry only chooses which of them a `Workspaces` accepts. What a *workspace* accepts is narrowed further by the allowlist below.

## Publishing

Events are published through a workspace handle, which is bound to a tenant, a workspace and the actor publishing ([Workspaces and the log](workspaces.md)):

```python
from reflexr import SourceActor
from reflexr.workspace import InMemoryStorage, Workspaces

workspaces = Workspaces(
    InMemoryStorage(),
    events=[ServiceError, Deploy, Heartbeat],  # what clients may publish
    emitted=[IncidentOpened],  # what only runs may publish
)
workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="monitoring"))

published = await workspace.publish(
    ServiceError(service="auth", severity=8, message="token check failed"), id="alert-7"
)
print(published.envelope.seq, published.duplicate)  # 1 False

again = await workspace.publish(
    ServiceError(service="auth", severity=8, message="token check failed"), id="alert-7"
)
print(again.envelope.seq, again.duplicate)  # 1 True
```

- **Publishing is idempotent by id.** Publishing an id that is already in the log appends nothing and returns the logged envelope with `duplicate=True`, so a producer can retry safely. Use an id the source already has, such as the monitoring system's alert id; without one, reflexr generates an `evt_…` id. The logged event wins: a second event published with the same id is not compared with the first.
- **`events=` is an allowlist.** Publishing a type the workspaces were not given is rejected with `NotFound`, even if the type is registered elsewhere in the process, so clients cannot publish arbitrary types. Leave `events` out to accept every type in the workspaces' registry.
- **`emitted=` lists the types only runs may publish**, such as the incident a triage agent opens. A run's actions can publish them; a client, over REST, the WebSocket, MCP or a handle of your own, is refused with `Forbidden` ([ADR-0041](../adr/0041-executing-runs.md)).
- **`publish_many`** publishes several events atomically and in order: all of them are logged, or none.

Producers outside the process publish over HTTP, the WebSocket or MCP, which all end in the same `publish` ([Serving over REST and WebSocket](serving.md), [External agents over MCP](mcp.md)).

## Envelopes

A stored event travels in an `Envelope`, which is also its shape on the wire. The envelope of the error above, as JSON:

```json
{
  "seq": 1,
  "id": "alert-7",
  "ts": "2026-09-28T09:00:00Z",
  "workspace_id": "prod",
  "actor": {
    "kind": "source",
    "name": "monitoring"
  },
  "causation": null,
  "correlation_id": "alert-7",
  "traceparent": null,
  "event": {
    "type": "ops:service.error",
    "service": "auth",
    "severity": 8,
    "message": "token check failed"
  }
}
```

| Field | Meaning |
|---|---|
| `seq` | The event's position in the workspace's log: gap-free, from 1 |
| `id` | The event's id, unique in the workspace; publishing it again is a no-op |
| `ts` | When it was appended. Rules measure time with it, and it never decreases within a workspace |
| `workspace_id` | The workspace it belongs to |
| `actor` | Who published it |
| `causation` | For an event a run emitted, and for reflexr's facts about a firing or run: the `firing_id`, the `run_id` and the causal chain's `depth`. `null` otherwise |
| `correlation_id` | The id of the first event in its causal chain ([Causal chains](workspaces.md#causal-chains)) |
| `traceparent` | The W3C trace context of the span that published it, so the runs it causes can link back to it, or `null` |
| `event` | The event itself, with its `type` first |

Envelopes are frozen. `envelope.event_type` is the event's type name, `envelope.depth` its depth in its causal chain (0 for an event nothing caused), and `envelope.data` its fields as JSON-compatible values, which is what conditions read.

## Actors

Every envelope is attributed to exactly one actor. The kinds match artifactr's, plus `source`:

| Actor | Who |
|---|---|
| `UserActor(id, name=None)` | A person, identified by your application's user id |
| `SourceActor(name)` | A system that publishes events, such as a monitoring service or a webhook |
| `AgentActor(rule, run_id, name)` | An action running for one firing of a rule. The reactor gives each run a handle acting as its agent, named after the action |
| `ExternalAgentActor(client_id, name=None)` | An agent connected over MCP |
| `SystemActor(name="reflexr")` | reflexr itself (`"reactor"` for rule and run facts, `"scheduler"` for ticks), or your application acting on its own behalf |
| `EvaluatorActor(name, version)` | A judge whose verdicts are recorded as feedback ([Feedback and evaluation](evaluation.md)) |

On the wire an actor is an object with a `kind`. Each actor has a `participant` key that is the same for everything one participant does: `source:monitoring` for the source above, and `agent:ops:error-spike` for every run of the `ops:error-spike` rule's action, whichever run it is. `display_name` is how the actor is named to people and models.

## reflexr's own events

reflexr records what it decides and does in the same log, as events of its own, in the `reflexr` namespace. Applications cannot publish them: trying is rejected with `Forbidden`. Rules can watch them like any other event.

| `type` | Class | Fields | Recorded when |
|---|---|---|---|
| `reflexr:rule_fired` | `RuleFired` | `rule`, `scope`, `scope_key`, `firing_id`, `matched` | A rule's condition held for a scope; `matched` lists the `seq` of every envelope that made it hold |
| `reflexr:rule_errored` | `RuleErrored` | `rule`, `seq`, `error` | A rule could not evaluate an envelope, which is dead-lettered for that rule alone |
| `reflexr:rule_reset` | `RuleReset` | `rule`, `generation`, `reason`, `from_seq`, `silent_through` | A rule's definition changed (`"changed"`) or someone replayed it (`"replayed"`) |
| `reflexr:rule_installed` | `RuleInstalled` | `rule`, `version`, `spec`, `provenance` | Someone installed or updated a [stored rule](rules.md#stored-rules); `spec` is the rule, as JSON |
| `reflexr:rule_archived` | `RuleArchived` | `rule`, `version`, `reason`, `cancelled` | Someone archived a stored rule; `cancelled` counts the unfinished runs it cancelled |
| `reflexr:run_started` | `RunStarted` | `run_id`, `rule`, `scope`, `scope_key`, `attempt` | An attempt of a run began; `scope` holds the scope's field values, as on `reflexr:rule_fired` |
| `reflexr:run_progressed` | `RunProgressed` | `run_id`, `rule`, `step` | A graph run completed a step and saved a checkpoint |
| `reflexr:run_retrying` | `RunRetrying` | `run_id`, `rule`, `attempt`, `error`, `next_attempt_at`, `reason` | An attempt failed, and the run will be tried again. `reason` is a stable code for why, such as `timeout`, when there is one |
| `reflexr:run_succeeded` | `RunSucceeded` | `run_id`, `rule`, `output` | A run finished |
| `reflexr:run_dead_lettered` | `RunDeadLettered` | `run_id`, `rule`, `attempts`, `error`, `reason` | A run exhausted its retries, or failed in a way retrying cannot fix, such as a `guardrail_blocked` failure ([ADR-0036](../adr/0036-typed-run-failures.md)) |
| `reflexr:run_cancelled`, `reflexr:run_skipped` | `RunCancelled`, `RunSkipped` | `run_id`, `rule`, `reason` | Someone cancelled or skipped a run |
| `reflexr:run_requeued` | `RunRequeued` | `run_id`, `rule` | Someone made a run runnable again |
| `reflexr:feedback_given` | `FeedbackGiven` | `feedback_type`, `target`, `value` | A person or an evaluator judged a run, a firing or a chain |
| `reflexr:tick` | `Tick` | `schedule`, `at` | A [schedule](schedules.md) ticked |

A rule that pages someone on every severe error, after one error is published and the reactor has run, leaves this log (sequence, actor kind and type):

```text
1 source ops:service.error
2 system reflexr:rule_fired
3 system reflexr:run_started
4 system reflexr:run_succeeded
```

The rule and run facts form a closed union, `SystemEvent`, so code that handles them can `match` on their classes and pyright checks the cases. `about_rule(event)` returns the rule one of them is about, and `SYSTEM_EVENTS` lists every type reflexr appends, including `FeedbackGiven` and `Tick`. Following the log, for example:

```python
from reflexr.core import RunDeadLettered, RunSucceeded

async for envelope in workspace.subscribe():  # follows the log until cancelled
    match envelope.event:
        case RunSucceeded(run_id=run_id, output=output):
            print(f"{run_id} succeeded: {output}")
        case RunDeadLettered(run_id=run_id, error=error):
            print(f"{run_id} dead-lettered: {error}")
        case _:
            pass
```

A rule never sees reflexr's facts about itself, so it cannot fire on its own activity ([Loop and spend safety](safety.md#no-self-reaction)).

## Unknown events

Stored envelopes can outlive the code that defined their types, and a newer producer can use a type this process does not know. Data of an unregistered type validates as an `UnknownEvent`, which keeps its fields and round-trips unchanged:

```python
from reflexr.core import dump_event, load_event, type_of

event = load_event({"type": "pager:acknowledged", "incident": "inc-42"})
print(type(event).__name__, type_of(event), dump_event(event))
```

```text
UnknownEvent pager:acknowledged {'type': 'pager:acknowledged', 'incident': 'inc-42'}
```

`type_of(event)` gives the type name of any event, known or not, and `dump_event(event)` its data with the `type` first, as stored. An unknown event can be read, but not published: publishing one is rejected with `NotFound`, as a type outside the allowlist is. Within the `reflexr.v1` protocol, new event types are an additive change, so clients should keep an event of an unknown type as an opaque envelope ([Stream protocol](../protocol.md)).
