# Loop and spend safety

Rules run workflows, and workflows publish events that rules watch, so a mistaken rule can trigger itself forever. The workflows call models, so a busy rule can also spend without bound. reflexr stops loops at a known depth and gives you several ways to bound how often, how long and how much a rule's actions run ([ADR-0010](../adr/0010-loop-and-spend-safety.md)). This page covers each safeguard, what it enforces, and what it leaves to you.

| Safeguard | Bounds | Where it is set |
|---|---|---|
| No self-reaction | A rule reacting to reflexr's facts about itself | Always on |
| Causation depth | Chains of runs triggering runs | `Workspaces(max_depth=8)` |
| Throttles and dedupe | How often a rule fires per scope | The rule's condition |
| Emit allowlists | Which events an agent can publish | `EventContext(emit=[...])`, `Workspaces(events=[...])` |
| Usage limits and timeouts | What one attempt may use, and for how long | `AgentAction(usage_limits=)`, `Rule(timeout=)` |
| Retry policy and permanent failures | How many attempts a run gets | `Rule(retry=RetryPolicy(...))`, `RunFailure(permanent=True)` |
| The LLM gateway | Spend, rate limits and guardrails across every run | A LiteLLM proxy ([The LLM gateway](gateway.md)) |
| Concurrency | How many runs execute at once | `Reactor(concurrency=10)` |

## No self-reaction

A rule never sees reflexr's facts about itself: its own `rule_fired`, `rule_errored`, `rule_reset` and run events. They still move its clock, since every envelope does, but they never match its filter. So a rule that watches `run_succeeded`, for example to judge finished runs, cannot fire on its own runs' success, only on other rules'.

This does not stop two rules that watch each other's facts, or a rule whose action publishes the kind of event it watches. Causation depth does.

## Causation depth

Every event records the causal chain it belongs to, and events that runs publish record how deep in the chain they are. An event nothing caused is at depth 0. A run's events, and reflexr's facts about the firing and run, are one step deeper than the deepest envelope the firing matched. `Workspaces(max_depth=8)` sets the limit, 8 by default.

Here a rule's action publishes the very kind of event its rule watches, with the limit lowered to 3:

```python
echo = Rule(name="echo", when=on(ServiceError), scope=by(F.service), then=run("echo"))


async def echo_action(reaction: Reaction[None]) -> None:
    # a mistake: the action publishes the kind of event its own rule watches
    await reaction.emit(ServiceError(service="auth", severity=8, message="still failing"))


workspaces = Workspaces(InMemoryStorage(), rules=[echo], max_depth=3)
reactor = Reactor(workspaces, actions={"echo": echo_action})
monitor = await workspaces.open("acme", "prod", actor=SourceActor(name="monitor"))
await monitor.publish(ServiceError(service="auth", severity=8))
await reactor.settle()

for envelope in await monitor.read():
    print(envelope.seq, envelope.depth, envelope.actor.kind, envelope.event_type)
```

```text
1 0 source service.error
2 1 system rule_fired
3 1 system run_started
4 1 agent service.error
5 1 system run_succeeded
6 2 system rule_fired
7 2 system run_started
8 2 agent service.error
9 2 system run_succeeded
10 3 system rule_fired
11 3 system run_started
12 3 agent service.error
13 3 system run_succeeded
14 3 system rule_errored
```

The loop ran three times and stopped. The fourth firing would have been at depth 4, so the evaluator refused it: it recorded `rule_errored` and dead-lettered the envelope for that rule, with the error "the firing would be at causation depth 4, beyond the limit of 3" in `workspace.dead_letters()` and the `reflexr.rule.errors` metric. The same bound applies when two rules trigger each other through `rule_fired` or `run_succeeded`, since those facts are one step deeper too ([ADR-0026](../adr/0026-the-reactors-evaluation.md)). An error about another rule's error is dead-lettered without becoming a new fact, so errors cannot feed on each other.

Publishing checks the depth as well: a handle whose events would be deeper than the limit refuses to publish with `DepthExceeded` (`depth_exceeded` on the wire). With the reactor, refusing the firing comes first, so this check is a backstop, for handles you derive yourself with `workspace.caused_by(...)`.

Legitimate chains that are longer than 8 steps need a higher `max_depth`, set explicitly.

## Throttles

A throttle caps how often a rule fires per scope. It is the last stage of the condition, after the pattern:

```python
error_spike = Rule(
    name="error-spike",
    when=on(ServiceError)
    .where(F.severity >= 7)
    .count(at_least=3, within=timedelta(minutes=1))
    .at_most(1, per=timedelta(minutes=15)),
    scope=by(F.service),
    then=run("triage"),
)
```

Nine severe errors from `auth` within 90 seconds make three spikes, but only the first fires; the others are dropped, and the rule fires again for `auth` only once 15 minutes have passed since it last did. Each scope is throttled separately, so a noisy service does not silence the others. `.distinct(F.message, within=...)` goes further upstream: it drops an event whose key was already seen within the window, before it counts ([Dedupe and throttle](rules.md#dedupe-and-throttle)).

Throttles are the main spend control for agent actions: a rule that runs an agent should usually have one.

## Emit allowlists

An agent can publish only the event types its `EventContext` names. With none, it has no tool to publish at all:

```python
EventContext(emit=[IncidentOpened])  # emit_event accepts incident.opened, and nothing else
EventContext()  # read_events only
```

A call to emit another type, or fields that do not validate, is refused with a message the model can act on, and nothing is published ([Agents](actions.md#agents)). Separately, `Workspaces(events=[...])` is an allowlist for everyone, runs included: publishing a type that is not listed raises `NotFound`, even if the type is registered elsewhere in the process. `Workspaces(emitted=[...])` goes the other way, for the types only workflows should produce: runs may publish them, and a client that tries is refused with `Forbidden`, so nobody outside a workflow can forge the incident an agent opens. reflexr's own events can never be published by anyone; trying raises `Forbidden`.

## Usage limits and timeouts

`AgentAction(usage_limits=UsageLimits(...))` passes pydantic-ai's usage limits to every attempt, bounding requests and tokens. An attempt that would exceed them fails with pydantic-ai's `UsageLimitExceeded`, and the run is retried or dead-lettered like any other failure:

```python
triage = AgentAction(triage_agent, name="triage", usage_limits=UsageLimits(request_limit=5))

error_spike = Rule(
    name="error-spike",
    when=on(ServiceError).where(F.severity >= 7).count(at_least=3, within=timedelta(minutes=1)),
    scope=by(F.service),
    then=run(triage),
    retry=RetryPolicy(max_attempts=2),
    timeout=timedelta(minutes=2),
)
```

The limits apply per attempt, so the most one run can use is `max_attempts` times them: with the rule above, two attempts of at most five requests each. A rule's `timeout` bounds each attempt's time: an action that runs longer is cancelled and the attempt fails with the reason `timeout`. The retry policy bounds attempts for every kind of action, including attempts abandoned because their process crashed ([Retries and dead letters](reactor.md#retries-and-dead-letters)).

Retrying a failure that cannot succeed only spends more. An action, tool or capability that knows this raises `RunFailure(..., reason=..., permanent=True)`, and the run is dead-lettered at once instead of using up its attempts ([ADR-0036](../adr/0036-typed-run-failures.md)).

Spend across all runs, per tenant, key or model, belongs in the LLM gateway: a LiteLLM proxy in front of every model call holds the budgets, rate limits and guardrails, and a request a guardrail blocks fails its run permanently, with the reason `guardrail_blocked` ([The LLM gateway](gateway.md), [ADR-0022](../adr/0022-litellm-proxy-first.md)).

## Concurrency

Each reactor executes at most `concurrency` runs at once (10 by default), and each call of `execute` attempts at most `limit` due runs (100). With the default `ordering="scope"`, a rule runs one run per scope at a time, so a burst of firings for one service is worked through in order rather than all at once, and takes one place in `limit` while it waits ([Ordering](reactor.md#ordering)).

Those are the limits today. ADR-0010 also plans concurrency limits per rule and per workspace; they are not built yet, so to hold an expensive rule back, throttle it, or give its actions a semaphore of your own in `deps`.
