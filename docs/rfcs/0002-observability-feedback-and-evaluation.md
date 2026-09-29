# RFC-0002: Observability, feedback, evaluation and the LLM gateway

**Status:** Accepted
**Author:** Alex Nodeland
**Created:** 2026-09-28
**Discussion:** accepted on 2026-09-28
**Siblings:**
- [artifactr RFC-0002](https://github.com/alexnodeland/artifactr/blob/main/docs/rfcs/0002-observability-feedback-and-evaluation.md) makes the same changes in artifactr.
- [evalr RFC-0001](https://github.com/alexnodeland/evalr/blob/main/docs/rfcs/0001-v0.1-implementation-plan.md) builds the shared eval kit.
- [stackr RFC-0001](https://github.com/alexnodeland/stackr/blob/main/docs/rfcs/0001-v0.1-implementation-plan.md) builds the infrastructure template.

## Summary

Make reflexr deeply observable and evaluable, with the same conventions as artifactr:

- **Tracing:** publishing, evaluation and every run attempt are OpenTelemetry spans with GenAI attributes. A **causal chain** is a session, and a run's trace links back to the event that caused it.
- **Metrics:** OpenTelemetry metrics from a registry, and Grafana dashboards shipped in the repository.
- **Typed feedback** on runs, firings and chains, recorded in the log and mirrored to Langfuse scores.
- **Evaluation:** an `[evals]` extra over evalr. Evaluators can run **as rules** on finished runs.
- **LLM gateway:** a `[litellm]` extra that routes agent and graph actions through the LiteLLM proxy, with guardrail policies per rule.
- **Dev environment:** a contributor Compose file and a dev container. The shared infrastructure lives in stackr.

Because reflexr is still being built (RFC-0001), tracing, metrics and feedback land inside RFC-0001's phases 2 to 5 rather than as a retrofit.

## Design

### Tracing

- **API only.** The scope is `reflexr` at the package version. `Workspaces` and the `Reactor` accept optional tracer and meter providers. reflexr never configures the SDK.
- **Spans:**

| Span | Name | Key attributes |
|---|---|---|
| Publishing | `reflexr.publish {type}` | `reflexr.event.type`, `reflexr.event.id` |
| Evaluating a batch of the log | `reflexr.evaluate` | `reflexr.workspace.id`, the rules evaluated, the firings made |
| A run attempt | `invoke_workflow {rule}` | `gen_ai.operation.name=invoke_workflow`, `gen_ai.workflow.name`, `reflexr.run.id`, `reflexr.attempt` |
| An agent action | pydantic-ai's `invoke_agent`, inside the run | Attributes added by the `EventContext` capability |
| A graph step | `execute_step {node}` | reflexr emits these itself: pydantic-graph's own node spans need Logfire |

- **Sessions are causal chains.** `session.id` and `gen_ai.conversation.id` are the chain's `correlation_id`. So one incident is one Langfuse session: the triage agent, the runbook graph and the page all appear together. Agent actions pass `conversation_id=correlation_id` to pydantic-ai.
- **Trace context crosses the log.** An envelope records the W3C trace context of the span that published it. A run's span **links** to the traces of the envelopes that made its rule fire. The log is asynchronous, so a run cannot be a child of its cause, but a link lets a trace viewer follow the causal chain across traces.
- **Attribution:**
  - `reflexr.tenant.id`, `reflexr.workspace.id`, `reflexr.rule`, `reflexr.scope`, `reflexr.run.id`
  - `user.id` when a person published or acted
  - the actor's kind
- **Trace links on runs:** `Run.trace_ids` records each attempt's trace. It is added to the core `Run` model before phase 2 builds storage.

### Metrics and dashboards

- **A metric registry** (`reflexr.telemetry.metrics`) declares every metric:
  - `reflexr.events.published` (by type)
  - `reflexr.evaluation.lag` (the envelopes a rule is behind, by rule) and `reflexr.evaluation.duration`
  - `reflexr.firings` and `reflexr.rule.errors` (by rule)
  - `reflexr.runs` (by rule and status), `reflexr.run.duration` and `reflexr.run.attempts`
  - `reflexr.dead_letters` (by rule)
  - `reflexr.schedule.ticks`
  - `reflexr.feedback`
- **Cardinality policy:** run, firing, event, chain and scope values are never metric attributes. Rule names are allowed, since code bounds them. Tenant and workspace are configurable, as in artifactr.
- **Dashboards** in `deploy/grafana/dashboards/`: Overview, Tenant, Workspace, Rules (lag, firings, errors), Runs (durations, retries, dead letters), Agent and LLM, Schedules. A test checks every query against the registry, and releases publish the dashboards for stackr.

### Typed feedback

- **`Feedback`** subclasses register by name and declare their targets:
  - `RunTarget(run_id)`: a workflow execution
  - `FiringTarget(firing_id)`: whether the rule should have fired
  - `ChainTarget(correlation_id)`: the whole incident
- `Workspace.give_feedback(...)` records `feedback_given` through the one write path. REST, WebSocket and MCP expose it as a command.
- **Evaluator verdicts are feedback** from `EvaluatorActor(name, version)`, as in artifactr.

### Langfuse (the `[langfuse]` extra)

The same parts as artifactr's:
- a span filter that keeps reflexr's spans and the database, HTTP and graph spans
- a context helper setting the session to the chain, tags for the rule and tenant, and the trace name to the rule
- a feedback mirror (run feedback to the run's trace, chain feedback to the session, firing feedback to the evaluation trace)
- score config sync

### Evaluation (the `[evals]` extra)

- **Datasets from the log:** feedback with its target's context (the firing's matched events, the run's inputs, outputs, steps and emitted events, the chain's whole history).
- **Experiment tasks** replay a firing's run against a new agent, graph or model in an isolated workspace.
- **Online evaluators as rules:** an evaluator is an action, so `Rule(when=on(RunSucceeded).where(rule="triage"), then=run(evaluate_triage))` judges every triage run. Its verdicts are recorded as feedback, sampled and throttled like any rule.
- **End-to-end measures for workflows:**

| Measure | Definition | How |
|---|---|---|
| Resolution | Whether the chain achieved its goal (the incident was handled) | Judged over the chain: a DSPy judge or a Jev evaluator |
| Dead-letter and retry rate | Share of runs dead-lettered or retried, by rule | Deterministic |
| Operator intervention rate | Share of runs a person retried, skipped or cancelled | Deterministic |
| Time to resolution | From the chain's first event to its resolving event | Deterministic, when the application marks resolution |

### LLM gateway (the `[litellm]` extra)

As in artifactr:
- `litellm_model(...)` through pydantic-ai's `LiteLLMProvider`
- per-request metadata: tenant team key, rule, run, chain session, trace id
- guardrail policies, set per **rule** in reflexr
- a guardrail block fails the attempt with a `guardrail_blocked` reason, which the rule's retry policy treats as permanent: retrying the same input would be blocked again

### Dev environment

A contributor `compose.yaml` (PostgreSQL by default; the reference app under `app`) and a dev container built on it, which join stackr's network when it is running. The full infrastructure is in stackr.

### Shared conventions

These match artifactr's RFC-0002 exactly, except that reflexr's session is the causal chain and its workflow span is named after the rule.

## Drawbacks

- The same as artifactr's: more dependencies at the edges, and more repositories to keep in step.

## Alternatives

- **A run as a child span of its cause:** runs start long after the envelope that caused them, from another process, so parent-child would stretch traces over hours. Span links keep traces short and still connect them.

## Unresolved questions

- **Chains that span libraries:** when the combined system turns an artifactr event into a reflexr event, whether the chain continues the thread's session. A decision for the combined system; the envelope's trace context makes either possible.

## Tracking

- [x] B1: telemetry core: spans, attribution, trace context on envelopes, `Run.trace_ids`, metric registry, `[otel]` helper. Landed with RFC-0001 phases 2 and 3.
- [ ] B2: typed feedback: `Feedback`, targets, `feedback_given`, `EvaluatorActor`. Core in phase 2; surfaces in phase 5.
- [x] B3: `[langfuse]` extra ([ADR-0029](../adr/0029-metric-detail-through-sdk-views.md))
- [ ] B4: dev environment and dashboards, with the reference implementation (phase 6)
- [x] B5: `[evals]` extra over evalr: feedback sources, evaluators as rules, experiment tasks and the end-to-end measures
- [x] B6: `[litellm]` extra (typed run failures in [ADR-0036](../adr/0036-typed-run-failures.md); the gateway in [ADR-0022](../adr/0022-litellm-proxy-first.md))
