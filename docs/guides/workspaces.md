# Workspaces and the log

A workspace is where a tenant's events live: one append-only log with a gap-free `seq`, and the progress, runs and dead letters of the rules that watch it. It is the unit of ordering, isolation and scale, exactly as in artifactr ([ADR-0016](../adr/0016-tenants-and-workspaces-like-artifactr.md)). Every read and write goes through a `Workspace` handle. This page covers opening handles, publishing, causal chains, reading the log, operating runs and rules, and the ways a command can be rejected.

## Opening a workspace

`Workspaces` holds everything the workspaces share, checks it once, and opens handles over one storage:

```python
from reflexr import SourceActor, UserActor
from reflexr.workspace import InMemoryStorage, Workspaces

workspaces = Workspaces(
    InMemoryStorage(),
    events=[ServiceError, Deploy, Heartbeat, IncidentOpened],
    rules=[error_spike, deploy_regression, heartbeat_lost],
)

monitoring = await workspaces.open("acme", "prod", actor=SourceActor(name="monitoring"))
ada = monitoring.as_actor(UserActor(id="ada", name="Ada"))
```

| Argument | Default | Meaning |
|---|---|---|
| `storage` | required | Where workspaces are kept ([Storage](storage.md)) |
| `events` | every registered type | The event types clients may publish: an allowlist ([Events and envelopes](events.md#publishing)) |
| `emitted` | none | Event types only runs may publish, such as an incident an agent opens; clients are refused them |
| `rules` | none | The [rules](rules.md) every workspace evaluates, checked when `Workspaces` is built |
| `predicates` | none | The Python predicates rules refer to, by name |
| `schedules` | none | The [schedules](schedules.md) that publish ticks into the workspaces |
| `clock` | UTC system time | The time for run transitions; tests pass a clock they control |
| `max_depth` | 8 | The deepest causal chain an event may extend ([Causation depth](safety.md#causation-depth)) |
| `tracer_provider`, `meter_provider` | OpenTelemetry's global ones | Where spans and metrics go ([Observability](observability.md)) |

`open(tenant_id, workspace_id, actor=...)` is the only place a tenant id enters. The handle it returns is bound to that tenant, that workspace and one actor, and nothing on it can reach another tenant. Workspaces need no creating: opening one that has never been written to gives an empty workspace, and it appears to the reactor once it has a log. Handles are cheap, so open one per request or connection, for the actor making it. `as_actor(actor)` returns a handle on the same workspace acting as someone else, and every write through a handle is attributed to its actor.

## Publishing events

`publish` appends one event, and `publish_many` several, atomically and in order:

```python
published = await monitoring.publish(
    ServiceError(service="auth", severity=8, message="token check failed"), id="alert-7"
)
batch = await monitoring.publish_many(
    [Deploy(service="auth", version="2.4.1"), Heartbeat(service="auth")],
    ids=["deploy-91", None],  # None: generate an id
)
```

Both return `Published` results: the `envelope` that is in the log, and whether the id was a `duplicate`, so nothing was appended. Publishing is idempotent by id ([Events and envelopes](events.md#publishing)). Before appending, a handle checks that the event's type is in the allowlist (`NotFound` otherwise), that it is not one of reflexr's own events (`Forbidden`), that a handle no run caused is not publishing a run-only type from `emitted` (`Forbidden`), and, for a run's handle, that the causal chain is not too deep (`DepthExceeded`). Each publish is traced as a `reflexr.publish {type}` span, whose trace context is stored on the envelope ([Observability](observability.md#spans)).

The same `publish` is available to producers outside the process over [REST and the WebSocket](serving.md) and [MCP](mcp.md). Every surface hands commands to one handler, `execute(workspace, command)`, which you can call too:

```python
from reflexr.core import Publish
from reflexr.workspace import execute

outcome = await execute(ada, Publish(event=Heartbeat(service="billing"), id="hb-1"))
print(outcome.type, outcome.id, outcome.duplicate)  # published hb-1 False
```

A command either returns an outcome or raises a [rejection](#rejections).

## Causal chains

Every envelope belongs to a causal chain, named by its `correlation_id`: the id of the chain's first event. The chain is how reflexr connects a cause with everything it led to, and it is the session a run's traces are filed under ([Sessions and causal chains](observability.md#sessions-and-causal-chains)).

- **An event nothing caused starts a chain** with its own id.
- **A firing joins the chain of the latest envelope it matched,** and its run carries that chain ([ADR-0024](../adr/0024-causal-chains-and-operator-actions.md)). For an absence, that is the last envelope the rule saw.
- **Events a run publishes continue its chain.** The reactor gives each run a handle made with `caused_by(causation, correlation_id=...)`, so its events record the firing and run that caused them, one step deeper in the chain.
- **reflexr's facts about a firing or run,** operators' actions on a run, and feedback on it all join the run's chain.
- **A producer can join an existing chain** with `publish(..., correlation_id="alert-7")`, naming the chain by the id of its first event. An id that is not in the log is rejected with `NotFound`, and the id of a later event in a chain with `ValidationFailed`, whose message names the chain that event belongs to, so the producer can join it by that id. Feedback on a `ChainTarget` names its chain the same way.

With a rule that runs `triage` on every severe error, and a `triage` action that opens an incident, the whole story of one alert is one chain:

```python
from reflexr import F, Rule, by, on, run
from reflexr.workspace import Reaction, Reactor


async def triage(reaction: Reaction[None]) -> str:
    await reaction.emit(IncidentOpened(service=str(reaction.scope["service"]), summary="errors"))
    return "incident opened"


rule = Rule(
    name="error-spike",
    when=on(ServiceError).where(F.severity >= 7),
    scope=by(F.service),
    then=run("triage"),
)
workspaces = Workspaces(InMemoryStorage(), events=[ServiceError, IncidentOpened], rules=[rule])
workspace = await workspaces.open("acme", "prod", actor=SourceActor(name="monitoring"))

await workspace.publish(ServiceError(service="auth", severity=8), id="alert-7")
await Reactor(workspaces, actions={"triage": triage}).settle()

for envelope in await workspace.read():
    print(
        envelope.seq,
        envelope.event_type,
        envelope.actor.kind,
        envelope.correlation_id,
        envelope.depth,
    )
```

```text
1 service.error source alert-7 0
2 rule_fired system alert-7 1
3 run_started system alert-7 1
4 incident.opened agent alert-7 1
5 run_succeeded system alert-7 1
```

The incident's `causation` names the firing and run behind it, and its depth counts the runs between it and the alert. A rule that fired on `incident.opened` would run at depth 2, and so on up to the workspace's `max_depth`, beyond which publishing is rejected, so workflows that trigger each other stop ([Loop and spend safety](safety.md#causation-depth)).

## Reading the log

| Method | Returns |
|---|---|
| `read(after_seq=0, limit=None)` | The envelopes after `after_seq`, in order |
| `subscribe(after_seq=0)` | An async iterator of the envelopes after `after_seq`, then each new one as it commits |
| `head_seq()` | The latest `seq`, or 0 if the log is empty |
| `run(run_id)` | A run, or raises `NotFound` |
| `runs(rule=None, status=None, scope_key=None, limit=None)` | Runs, newest first, optionally of one rule, status or scope |
| `dead_letters(rule=None)` | The envelopes rules could not evaluate, oldest first |
| `rule_progress()` | Each rule's `RuleProgress`: its cursor, generation and pending `absence` deadlines |
| `schedule_ticks()` | When each [schedule](schedules.md) last ticked in the workspace |

`subscribe` yields the stored envelopes and then the live ones on one iterator, so nothing falls between catching up and following along:

```python
async for envelope in monitoring.subscribe(after_seq=last_seen_seq):
    handle(envelope)
```

A `Run` records its rule, `scope` and `scope_key`, the `seq` it fired at and the envelopes it `matched`, its chain, its `status` and `attempts`, the last `error` and its `reason` code, its `output`, a graph's latest `checkpoint`, and the trace id of each attempt. Its id is the firing's id. The [reactor](reactor.md) explains how runs move through their statuses.

## Operating runs and rules

People and operators steer runs and rules through the same handles. Each operation is one transaction that applies the core's transition and appends the resulting event, attributed to the handle's actor and in the run's chain, so the log shows who intervened:

| Method | Applies to | Does | Appends |
|---|---|---|---|
| `retry_run(run_id)` | Any run that has not succeeded and is not running | Makes it runnable now; a dead-lettered, cancelled or skipped run gets a fresh retry budget | `run_requeued` |
| `skip_run(run_id, reason=None)` | Pending, retrying or dead-lettered runs | Gives up on it, unblocking later runs of its scope | `run_skipped` |
| `cancel_run(run_id, reason=None)` | Pending, running, retrying or dead-lettered runs | Cancels it; a running attempt is stopped when its executor next renews its lease, and its result is discarded | `run_cancelled` |
| `replay_rule(rule, from_seq=0, mode="rebuild")` | A registered rule | Resets the rule to evaluate the log again after `from_seq` ([Replaying a rule](reactor.md#replaying-a-rule)) | `rule_reset` |

```python
[stuck] = await ada.runs(rule="error-spike", status="dead")
await ada.retry_run(stuck.id)  # try again, with a fresh budget
await ada.skip_run(stuck.id, reason="billing is being migrated")  # or give up on it
```

An operation that does not fit the run's status is rejected with `InvalidState`: retrying a run that succeeded, for example. `checkpoint_run` belongs to actions, which call it through `Reaction.checkpoint` ([Graphs](actions.md#graphs)); `give_feedback` records judgements of runs, firings and chains ([Feedback and evaluation](evaluation.md#giving-feedback)).

## Rejections

A command that cannot be carried out raises a `Rejection` and changes nothing. Each has a stable `code`, which the surfaces send to clients, a `message`, and `payload()` with its typed details:

```python
from reflexr.core import NotFound

try:
    await ada.publish(Deploy(service="auth", version="2.4.2"), correlation_id="nope")
except NotFound as rejection:
    print(rejection.payload())
```

```text
{'type': 'not_found', 'message': 'chain nope does not exist', 'entity': 'chain', 'id': 'nope'}
```

| Rejection | `code` | When | HTTP |
|---|---|---|---|
| `NotFound` | `not_found` | Something the command names does not exist, such as a run, a rule or a chain, or an event type is not accepted (`entity`, `id`) | 404 |
| `InvalidState` | `invalid_state` | The command does not fit the current state, such as skipping a run that succeeded | 409 |
| `ValidationFailed` | `validation_failed` | The data does not validate, such as feedback that does not fit its type, a replay beyond the head of the log, or a `correlation_id` naming a later event of a chain rather than its first (`errors`) | 422 |
| `Forbidden` | `forbidden` | The actor may not do this, such as publishing one of reflexr's own events | 403 |
| `DepthExceeded` | `depth_exceeded` | A run's event would extend its causal chain beyond `max_depth` (`depth`, `limit`) | 422 |
| `UnsupportedProtocol` | `unsupported_protocol` | A client asked for another protocol version | 400 |

The HTTP column is how [REST](serving.md) reports each one; over the WebSocket and MCP the rejection's payload travels in the command's result ([Stream protocol](../protocol.md)). A rule that refers to something that does not exist is not a rejection but an `InvalidRule` error, raised when the application starts ([Checking rules](rules.md#checking-rules)).
