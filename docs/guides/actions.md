# Actions

An action is what a rule runs when it fires: a plain async function, a pydantic-ai agent, or a pydantic-graph graph. Every kind receives the same `Reaction`, is registered with the [reactor](reactor.md) under a name, and is referred to by that name in rules, because rules are data and actions are code ([ADR-0008](../adr/0008-actions-agents-graphs-and-functions.md)). This page covers the `Reaction`, each kind of action, the params a rule can pass its action, and how to keep actions safe to run more than once.

## The Reaction

An action is called once per attempt of a run, with a `Reaction[D]`, where `D` is the type of the dependencies you gave the reactor:

| Attribute | What it is |
|---|---|
| `workspace` | The workspace, acting as the run's `AgentActor`. Events published through it record the run as their cause and join the run's causal chain. |
| `run` | The `Run`: its id, rule, scope, the `seq`s it matched, its attempts, chain and latest checkpoint |
| `rule` | The `Rule` that fired |
| `events` | The envelopes that made the rule fire, in order |
| `deps` | Your application's dependencies, as given to `Reactor(deps=...)` |
| `run_id` | The run's id: the firing's id, and the action's idempotency key |
| `attempt` | Which attempt this is, from 1 |
| `scope` | The values of the rule's scope fields for this firing, such as `{"service": "auth"}` |
| `params` | The rule's params, validated as the action's params model, or `None` if it declares none ([Parameters](#parameters)) |
| `params_as(Model)` | The params, typed as `Model` |
| `emit(event)` | Publishes an event caused by this run ([Idempotency](#idempotency)) |
| `checkpoint(step, state)` | Saves the run's progress, so the next attempt can resume after `step` |

Dependencies are whatever your actions need, usually clients for the systems they act on:

```python
from dataclasses import dataclass


@dataclass
class AppDeps:
    pager: Pager  # your paging client
    deploys: Deploys  # your deploy tool's client


reactor = Reactor(
    workspaces,
    actions={"triage": triage, "runbook": runbook, "page": page},
    deps=AppDeps(pager=pager, deploys=deploys),
)
```

## Functions

The simplest action is an `async def` over a `Reaction`:

```python
from reflexr.workspace import Reaction


async def page(reaction: Reaction[AppDeps]) -> dict[str, str]:
    service = str(reaction.scope["service"])
    await reaction.deps.pager.notify(service, key=reaction.run_id)
    return {"paged": service}


heartbeat_lost = Rule(
    name="ops:heartbeat-lost",
    when=on(Heartbeat).absent(within=timedelta(minutes=5)),
    scope=by(F.service),
    then=run("page"),
)
```

What an action returns is the run's output, recorded on the run and in its `reflexr:run_succeeded` event: JSON-compatible data, a Pydantic model (stored as its JSON), or `None`. A function has no name of its own that reflexr reads, so rules refer to it by the name you register it under: `run("page")`.

Raising fails the attempt, which is retried under the rule's retry policy ([Retries and dead letters](reactor.md#retries-and-dead-letters)). To say why, raise `RunFailure` with a reason code, which is recorded on the run and counted in metrics; make it `permanent=True` when retrying cannot help, and the run is dead-lettered at once ([ADR-0036](../adr/0036-typed-run-failures.md)):

```python
from reflexr.workspace import RunFailure

if service == "legacy":
    raise RunFailure(f"{service} has no on-call rotation", reason="no_rotation", permanent=True)
```

`RunFailure` works from an agent's tools and capabilities too, since pydantic-ai lets exceptions other than `ModelRetry` end the agent run.

A function can save progress too. What it passes to `checkpoint` is `reaction.run.checkpoint` on the next attempt, so a retry can skip what is done:

```python
async def notify_owners(reaction: Reaction[AppDeps]) -> None:
    done = set(reaction.run.checkpoint or [])
    for owner in ("ada", "grace"):
        if owner not in done:
            await reaction.deps.pager.notify(owner, key=f"{reaction.run_id}:{owner}")
            done.add(owner)
            await reaction.checkpoint(owner, sorted(done))
```

Each checkpoint appends `reflexr:run_progressed` with the step's name. If the attempt is no longer the run's current one (it was cancelled, or another executor took it over), `checkpoint` raises `InvalidState`, and the action should stop.

## Agents

An agent action is an ordinary pydantic-ai `Agent` whose `deps_type` is `Reaction[...]`, wrapped in an `AgentAction`. The `EventContext` capability makes it a response to a firing: it gives the agent a tool to read back through the log and, if you allow it, a tool to publish events.

```python
from typing import Literal

from pydantic import BaseModel
from pydantic_ai import Agent, RunContext
from pydantic_ai.usage import UsageLimits

from reflexr.agent import AgentAction, EventContext


class Triage(BaseModel):
    severity: Literal["low", "high", "critical"]
    summary: str


triage_agent = Agent(
    "anthropic:claude-sonnet-5-5",
    deps_type=Reaction[AppDeps],
    output_type=Triage,
    instructions="You are the first responder for an on-call team.",
    capabilities=[EventContext(emit=[IncidentOpened])],
)


@triage_agent.tool
async def latest_deploy(ctx: RunContext[Reaction[AppDeps]], service: str) -> str:
    """Return the version of a service that is deployed now."""
    return await ctx.deps.deps.deploys.latest(service)


triage = AgentAction(triage_agent, name="triage", usage_limits=UsageLimits(request_limit=10))

error_spike = Rule(
    name="ops:error-spike",
    description="Three severe errors from one service within a minute.",
    when=on(ServiceError).where(F.severity >= 7).count(at_least=3, within=timedelta(minutes=1)),
    scope=by(F.service),
    then=run(triage),
)
```

`run(triage)` takes the action's name, which is `name=` or else the agent's own name; an `AgentAction` needs one or the other. The agent's own tools reach your dependencies through `ctx.deps.deps`, and the reaction's other parts through `ctx.deps`. The model can be any pydantic-ai model; to route every request through a LiteLLM proxy, with the tenant, the chain and the rule's guardrails attached, see [The LLM gateway](gateway.md).

By default the prompt describes the firing: the rule, its description, the scope, the attempt and the matched events, each rendered as a line the model can read:

```text
Rule "ops:error-spike" fired for {"service": "auth"}. This is attempt 1.
The rule: Three severe errors from one service within a minute.
The events that made it fire, oldest first:
<event seq="2" type="ops:service.error" at="2026-01-01T00:00:00+00:00" by="source">{"message": "token check failed", "service": "auth", "severity": 8, "type": "ops:service.error"}</event>
<event seq="3" type="ops:service.error" at="2026-01-01T00:00:00+00:00" by="source">{"message": "token check failed", "service": "auth", "severity": 8, "type": "ops:service.error"}</event>
<event seq="4" type="ops:service.error" at="2026-01-01T00:00:00+00:00" by="source">{"message": "login timeout", "service": "auth", "severity": 8, "type": "ops:service.error"}</event>

Respond to this firing.
```

Pass `prompt="..."` for a fixed prompt, or a function of the reaction, such as `prompt=lambda reaction: f"Triage {reaction.scope['service']}."`. `reflexr.agent.firing_text(reaction)` and `envelope_text(envelope)` render the same text for prompts of your own.

`EventContext` adds instructions saying the agent responds to a firing, and which event types it may emit, and these tools:

| Tool | What it does |
|---|---|
| `read_events(after_seq=0, before_seq=None, types=None, limit=None, last=None)` | Reads envelopes from the workspace's log, oldest first, at most `read_limit` per call: the first `limit` in the window `after_seq < seq < before_seq`, or the latest `last`, of `types`. Without either, the first 20. The model reads further back by calling again with `before_seq` set to the oldest `seq` it has. |
| `emit_event(type, fields)` | Publishes an event of an allowed type, validated against its schema. Only present when `emit` names types. |

| Option | Default | Meaning |
|---|---|---|
| `emit` | none | The event types the agent may publish. Without any, it can only read. |
| `max_event_chars` | 2000 | How much of each event `read_events` shows the model |
| `read_limit` | 50 | The most envelopes one `read_events` call returns |
| `max_retries` | 3 | How many times the model may retry a refused tool call |

A call to emit a type that is not allowed, or fields that do not match the type, is refused with a retry prompt that tells the model why, and nothing is published. What the agent emits is attributed to the run's `AgentActor` and joins the run's causal chain.

The agent's output is the run's output; a structured `output_type` is stored as JSON. `usage_limits` bound each attempt, and a run that exceeds them fails like any other attempt ([Loop and spend safety](safety.md#usage-limits-and-timeouts)). Each run joins its causal chain's conversation (pydantic-ai's `conversation_id` is the chain's correlation id), and the capability attributes pydantic-ai's `invoke_agent` span to the tenant, workspace, rule, run and attempt, so every agent run one incident led to shows up together in traces ([Observability](observability.md)).

## Graphs

A graph action runs a pydantic-graph graph, built with `GraphBuilder`, whose deps are the `Reaction`. reflexr drives it step by step and saves a checkpoint after each step, so a retry resumes after the last completed step instead of starting over ([ADR-0043](../adr/0043-graph-checkpoints.md)):

```python
from dataclasses import dataclass, field

from pydantic import BaseModel
from pydantic_graph import GraphBuilder, StepContext

from reflexr.agent import GraphAction


@dataclass
class Runbook:
    notes: list[str] = field(default_factory=list)


class Finding(BaseModel):
    service: str
    version: str


g = GraphBuilder(
    name="runbook",
    state_type=Runbook,
    deps_type=Reaction[AppDeps],
    input_type=str,
    output_type=str,
)


@g.step
async def diagnose(ctx: StepContext[Runbook, Reaction[AppDeps], str]) -> Finding:
    version = await ctx.deps.deps.deploys.latest(ctx.inputs)
    ctx.state.notes.append(f"{ctx.inputs} runs {version}")
    return Finding(service=ctx.inputs, version=version)


@g.step
async def roll_back(ctx: StepContext[Runbook, Reaction[AppDeps], Finding]) -> str:
    await ctx.deps.deps.deploys.rollback(ctx.inputs.service, key=ctx.deps.run_id)
    ctx.state.notes.append(f"rolled back {ctx.inputs.version}")
    return ctx.inputs.service


@g.step
async def report(ctx: StepContext[Runbook, Reaction[AppDeps], str]) -> str:
    await ctx.deps.emit(IncidentOpened(service=ctx.inputs, summary="; ".join(ctx.state.notes)))
    return "; ".join(ctx.state.notes)


g.add(
    g.edge_from(g.start_node).to(diagnose),
    g.edge_from(diagnose).to(roll_back),
    g.edge_from(roll_back).to(report),
    g.edge_from(report).to(g.end_node),
)
runbook = GraphAction(g.build(), inputs=lambda reaction: str(reaction.scope["service"]))

deploy_regression = Rule(
    name="ops:deploy-regression",
    when=sequence(on(Deploy), on(ServiceError), within=timedelta(minutes=10)),
    scope=by(F.service),
    then=run(runbook),
    retry=RetryPolicy(max_attempts=3, backoff=timedelta(seconds=30)),
)
```

| Option | Default | Meaning |
|---|---|---|
| `name` | the graph's name | The name rules refer to the action by |
| `inputs` | `None` | Builds the graph's inputs from the reaction; without it the graph gets `None` |
| `state` | the state type's constructor | Builds the graph's initial state from the reaction |
| `input_types` | `{}` | Input types by node id, for the steps and forks whose type reflexr cannot read or infer, such as stream steps and forks after a transform ([below](#input-types)); an explicit type always wins |
| `params` | `None` | The model of the params rules pass the action ([Parameters](#parameters)) |

After each step, the action saves the graph's state and the next task to the run, and appends `reflexr:run_progressed` with the step's name. If `roll_back` fails the first time, the run waits to retry with `diagnose` as its last saved step, and the retry starts at `roll_back`: `diagnose` does not run again. The run's `reflexr:run_progressed` events show every saved boundary, across both attempts:

```text
['__start__', 'diagnose', 'roll_back', 'report', '__end__']
```

The graph's output is the run's output, and it is checkpointed too, so a run that finished its graph but failed to record the result does not run it again. Each step runs in an `execute_step {node}` span.

What is saved has limits:

- **What is saved must read back as it was.** The state, the graph's inputs and the next node's inputs are stored as JSON through a `TypeAdapter` of their types. A boundary is saved only if each of them serializes as its type and reads back equal to what it was, so a resumed run gets what the interrupted one would have. Otherwise the boundary is not saved, and a retry resumes from the one before it. A model in a state field typed `Any`, for example, would read back as a dict.
- **Types come from annotations.** A step's input type is read from its `StepContext[State, Deps, Input]` annotation, and the end node's is the graph's output type. A fork runs no code of its own, so its input type is inferred ([below](#input-types)). A stream step (`g.stream`) and a `BaseNode` class in the graph read as taking `Any`, since pydantic-graph wraps them: a model given to one is not saved before it, unless `input_types` names its type.
- **Nothing is saved before a decision.** A decision runs no code either, so the boundary after it saves the same progress, and the step it routes to is where a retry resumes.
- **Nothing is saved inside a fork.** Between a fork and its join, several branches are in flight, so a run that fails there resumes from the checkpoint before the fork, and branches that had finished run again. After the join, boundaries are saved again.
- **A changed graph starts over.** A checkpoint from a graph whose nodes have changed since, or whose next node's input type is no longer known, or whose values no longer validate as their types (a step's input model gained a required field, say), or from another version of the format, is ignored, and the retry runs the graph from the start. The attempt's `invoke_workflow` span gets a `reflexr.checkpoint.discarded` event that says why ([Observability](observability.md#spans)).

### Input types

A fork passes on what the edges into it carry, so reflexr infers its input type from them. When every edge into it comes, with no transform, from a step with a return annotation or from the graph's start, and they all carry the same type, that is its input type. A step returning `list[str]` into a map fork gives the fork that type, and the boundary before it, the last one outside the fork, is saved like any other.

`input_types` is needed only where inference cannot decide, and for stream steps given something other than plain JSON values, such as a model. Without it, the boundary before the node is not saved, and a retry resumes from the one before. Inference cannot decide when:

- an edge into the fork has a transform, since reflexr does not infer through one
- an edge comes from a step with no return annotation, or one that returns `Any` or a type variable
- an edge comes from a decision, a join, whose reduced value has no annotation to read, or another fork
- the edges carry different types, such as one step returning an `int` and another a `str`
- pydantic has no schema for the inferred type; the action is still built, without that checkpoint

```python
def hosts_of(ctx: StepContext[Runbook, Reaction[AppDeps], Finding]) -> list[str]:
    return ctx.inputs.hosts


# The edge transforms diagnose's Finding, so the fork's input type is given.
g.add(g.edge_from(diagnose).transform(hosts_of).map(fork_id="each_host").to(check_host))
runbook = GraphAction(g.build(), input_types={"each_host": list[str]})
```

An explicit type always wins over an annotation or an inferred one. `input_types` names only steps and forks: `GraphAction` raises `ValueError` for a decision, since nothing is saved before one, and for a join or an id that is not in the graph. One more rule keeps a checkpoint from changing what a resumed run does:

- **A one-shot iterator is never saved.** Writing a generator down would use it up, so a step that returns one into a map fork has no checkpoint before the fork.

## Parameters

One action can serve many rules that differ in a value, such as the chat thread to post in. The action declares a params model, and each rule passes values for it with `run` ([RFC-0003](../rfcs/0003-managing-rules-at-runtime.md)):

```python
from pydantic import BaseModel

from reflexr.workspace import with_params


class NotifyParams(BaseModel):
    thread_id: str


async def notify(reaction: Reaction[AppDeps], params: NotifyParams) -> None:
    await reaction.deps.bridge.notice(params.thread_id, reaction)


reactor = Reactor(workspaces, actions={"notify": with_params(notify, NotifyParams)}, deps=deps)

severe_errors = Rule(
    name="ops:severe-errors",
    when=on(ServiceError).where(F.severity >= 9).at_most(1, per=timedelta(minutes=15)),
    then=run("notify", thread_id="thr_4"),
)
```

In JSON the params are the action reference's `params`, a JSON object: `"then": {"action": "notify", "params": {"thread_id": "thr_4"}}`. They are not part of the rule's definition, so changing them never resets its state.

An action declares its model with a `params` attribute:

- **A function** is adapted by `with_params(fn, Model)`, which passes the validated params as its second argument. pyright checks that the function's second parameter takes a `Model`.
- **An agent or a graph** takes `params=Model`: `AgentAction(agent, params=NotifyParams)`, `GraphAction(graph, params=NotifyParams)`. Its prompt function and tools, or its steps and its `state` and `inputs` builders, read the params with `reaction.params_as(NotifyParams)`, which names the model a second time. A mismatch is a `TypeError` when the action runs, not a type error, since `Reaction` has one type parameter, the deps.

A function without `with_params` declares no model, and a rule may pass it no params.

The params are validated as JSON, since that is what they are, so a strict model takes a date as a string, as a rule written as JSON gives it. They are validated as the action's model twice:

- **When the reactor is built.** A rule whose params do not validate, or that passes params to an action that declares no model, raises `InvalidRule`, which lists every problem by its path in the rule's JSON, as for a missing action: `then.params.thread_id: Field required`, or `then.params must be empty`.
- **At each attempt,** into `reaction.params`. An attempt whose params no longer validate fails permanently with the reason `invalid_params`, and one whose action is not registered with the reason `unknown_action` ([Retries and dead letters](reactor.md#retries-and-dead-letters)). The reactor has already checked its rules, so these are for rules it could not check when it was built: RFC-0003's stored rules, installed at runtime.

## Idempotency

Runs are executed at least once, so any part of an action may run again: after a failure, a timeout, or a crash of the process that ran it ([ADR-0041](../adr/0041-executing-runs.md)). reflexr gives actions what they need to make that harmless:

- **The run's id is an idempotency key.** It is the firing's id, derived from the rule, its generation, the scope and the envelope it fired at, so it is the same on every attempt. Pass it to the systems you call (`key=reaction.run_id` above), so that a repeated call is recognised as one.
- **`emit` does not publish twice.** Each event's id is derived from the run, the number of checkpoints it has saved (`run.checkpoints`), and its position among the events emitted since the last one, and publishing an id that is already in the log appends nothing. A retried attempt that resumes from the same checkpoint and emits the same events in the same order adds nothing to the log, while a graph that emits, checkpoints, and emits again after resuming gets fresh ids for the later events rather than losing them as duplicates ([ADR-0041](../adr/0041-executing-runs.md)).
- **Your own ids work too.** `reaction.workspace.publish(event, id=...)` with an id derived from the run is just as safe, and suits events whose order may change between attempts.
- **Checkpoints narrow what repeats.** A graph repeats at most the step in flight, and a function that checkpoints repeats at most the work since its last checkpoint.

Everything an action publishes through the reaction is attributed to `AgentActor(rule=..., run_id=..., name=...)`, carries the run's causation (the firing, the run, and a depth one greater than what fired), and joins the run's causal chain. That depth is what stops workflows that trigger themselves ([Causation depth](safety.md#causation-depth)).
