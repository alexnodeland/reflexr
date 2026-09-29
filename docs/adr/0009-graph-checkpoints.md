# ADR-0009: Graph checkpoints at step boundaries

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Graph actions ([ADR-0008](0008-actions-agents-graphs-and-functions.md)) can take minutes and call models at several steps. Re-running a whole graph after a crash, or after one failing step, repeats work and spend, and may repeat side effects.

pydantic-graph 2.51 has no persistence of its own, but it can be driven step by step: `Graph.iter` yields the pending tasks between steps, and `GraphRun.next(requests)` continues a run from given tasks. A spike on 2026-09-28 interrupted a three-step graph after its first step, saved the state and pending tasks, and resumed them in a new run: the remaining two steps ran, the first did not run again, and the state was restored.

## Decision

- `GraphAction` drives graphs with `Graph.iter`. After each completed step it saves a **checkpoint** on the run (the graph state, serialized through a `TypeAdapter`, and the pending tasks: node id, inputs and fork stack) and records `run_progressed`.
- A retry, or another executor after a crash, **resumes from the latest checkpoint** with `GraphRun.next(saved_tasks)`, so completed steps are not repeated.
- Graph state and step inputs must be serializable. `GraphAction` checks the state type when it is registered.
- **Parallel branches**: checkpoints are taken only when no fork is in flight. A run that fails inside a fork resumes from the checkpoint before the fork. Phase 3 includes a spike on fork and join, and relaxes this if it is safe.

### Amendment (2026-09-28): the spike's findings

A spike against pydantic-graph 2.51 settled the details:

- **Mid-fork resume is not possible with the public API.** A join's partial results and the tasks in flight are private to the run, and the saved state would already hold changes from branches that run again. The rule above stands: checkpoints are taken only at *quiescent* boundaries. A boundary is quiescent when exactly one task comes next and that task is not between a fork and its join (the join's region, per `graph.parent_forks`), and no fork without a join is in its stack. After the join, boundaries are safe again.
- **Input types.** A checkpoint stores the next task's inputs, validated back into their type on resume. A step's type comes from its `StepContext[State, Deps, Input]` annotation, and the end node's from the graph's output type. Forks and decisions run no code of their own, so the boundary before them is not saved unless `GraphAction(input_types={...})` names their input type. The same applies to a step without an annotation. Inputs that do not serialize as their type are not saved either.
- **Resuming.** A run resumes by starting a fresh `graph.iter` with the saved state and inputs and sending the saved `GraphTaskRequest`s. Their fork-stack ids are prefixed per attempt, since a new run numbers its node runs afresh. A checkpoint of another format version, or of a graph whose nodes changed, is ignored and the run starts over, as any at-least-once retry may. The graph's output is checkpointed too, so a run that finished its graph but not its recording does not run it again.
- **Tracing.** reflexr emits an `execute_step {node}` span per step; pydantic-graph's own spans need Logfire.

### Amendment (2026-09-29): decisions and forks infer their input type

The reference implementation's runbook needed `input_types={"decide": ...}` for the boundary after its first step to be saved, though that step's return annotation already said what the decision gets ([#47](https://github.com/alexnodeland/reflexr/issues/47)). The spike's inference replaces the "Input types" finding above for decisions and forks:

- **A decision's or a fork's input is what the edges into it carry.** When every such edge has no transform and comes from a step with a return annotation (its return type), from the graph's start (the graph's input type), or from a decision whose type is known, and all of them carry the same type, that is the node's input type. Decisions can follow decisions, so inference repeats until it learns nothing more. Return annotations are read without `Annotated` metadata, as input annotations are: the graph never validated the value against those constraints, so a resumed run must not either.
- **What inference cannot decide keeps the behaviour above**, no checkpoint before the node, unless `input_types` names its type:
  - a transform on an edge into it, whose output type reflexr cannot read
  - an edge from a step with no return annotation, or one that says `Any` or a type variable
  - an edge from a join (its reduced value) or from a fork (its items, or its copies inside the fork)
  - edges that carry different types
  - an inferred type pydantic has no schema for, which is dropped so the graph still registers; an explicit type pydantic cannot handle still raises
- **Explicit types win.** `input_types` overrides an annotation or an inference, and decisions after an explicitly typed one infer from the explicit type.
- **Two guards where writing an inferred type down would change what runs.** A decision routes by class, and pydantic writes a subclass down as the class it is declared as, so the boundary before a decision is saved only when its inputs read back as the class they had. A one-shot iterator, such as a generator a step returns into a map fork, is never written down, since serializing it would use it up before the fork ran.
- **A checkpoint whose next node has no known type starts over.** Input types now depend on the edges as well as the nodes: an edge into a decision that gains a transform leaves it untyped, with the same nodes. Such a checkpoint is ignored, as one from a changed graph is, rather than failing the attempt.
- **The format version stays 1.** What a checkpoint holds, and how, is unchanged. Every checkpoint written before resumes as it did: its next node had a type, from an annotation or `input_types`, and has the same one now, since those are read as before and explicit types win. Inference only adds checkpoints at boundaries that had none. A build from before this change would not know an inferred node's type, and would fail to resume a checkpoint saved before one, but no release predates the change; from now on, a build that meets such a checkpoint starts over.

### Amendment (2026-09-29): checkpoints read back as they were, or are not saved

Only decisions checked that their saved inputs read back as the class they had ([#58](https://github.com/alexnodeland/reflexr/issues/58), found while implementing [#47](https://github.com/alexnodeland/reflexr/issues/47)). Two kinds of node read their input type as `Any`, which pydantic writes a model down through as a dict, and reads back as that dict:

- **`g.stream` steps.** pydantic-graph wraps the stream function in a closure annotated with the builder's type variables, and pydantic takes a type variable for `Any`.
- **`BaseNode` classes in a `GraphBuilder` graph.** Their step, a `NodeStep`, is annotated `Any`.

A model given to one was saved as a dict and resumed as one. The stream step failed on it, and the `NodeStep` refused it as not its node class, on every retry until the run was dead-lettered. A state field typed `Any` holding a model did the same. Separately, a checkpoint whose values no longer validated, as when a type gains a required field between deploys, raised `ValidationError` on every retry.

Decided:

- **A checkpoint is saved only if what it restores reads back as it was.** The state, the graph's inputs and the next node's inputs are each written down and read back through their types, and the boundary is saved only if each reads back equal to what it was. Before a decision the inputs must also be of the same class, as before, since a decision routes by class. Otherwise the boundary is skipped, and a retry resumes from the one before, as for inputs that do not serialize. A value that reads back equal as another class is saved before any other node: `3` given to a step typed `float` resumes as `3.0`, as it did.
- **A checkpoint that does not validate starts over,** as one from a changed graph does, rather than failing every retry. Every checkpoint saved from now on reads back, so one that does not holds a type that changed since it was saved, which is the case a changed graph is. The alternative, a permanent `RunFailure` with a stable reason, would dead-letter the run at once for a change the graph can recover from, at the cost at-least-once execution already allows: repeating steps.
- **The attempt's span says why a checkpoint was not resumed.** Before, a discarded checkpoint left no trace. Now the `invoke_workflow` span gets a `reflexr.checkpoint.discarded` event, with `reflexr.checkpoint.reason`:
  - `format`: not a checkpoint, or another version of the format
  - `graph_changed`: the graph's nodes changed
  - `untyped`: the next node's input type is no longer known
  - `invalid`: a saved value no longer validates, with `reflexr.checkpoint.error` saying where and why, without the values, which may be private
- **Stream steps' input types are not inferred.** pydantic-graph 2.51 keeps the stream function only in the closure that wraps it, so its annotation is not exposed cleanly. `input_types` names the type, and a stream step given JSON values (strings, numbers, and lists and dicts of them) reads back without it. A `BaseNode`'s input is an instance of its own class, which pydantic-graph does expose (`NodeStep.node_type`); typing it is left until a graph needs it, since pydantic would have to write the node down.
- **The format version stays 1.** No release has written a checkpoint that this change stops writing.

## Options considered

| Option | Repeated work after a failure | Infrastructure |
|---|---|---|
| **Checkpoint after each step (chosen)** | At most the step that failed | None beyond storage |
| Re-run the whole graph | All completed steps | None |
| A durable execution backend (Temporal, DBOS, Prefect) | Minimal | An external service |

## Consequences

- Easier: long graph workflows survive crashes and flaky steps without repeating earlier model calls.
- Harder: steps must be written so that re-running the one in flight is safe, since a crash mid-step repeats it. Steps receive the run's idempotency key for that purpose.
- Revisit if pydantic-graph gains persistence of its own: reflexr's checkpoints should then adopt it.

## Action items

1. [x] Implement `GraphAction` checkpoints and the fork and join spike (RFC-0001 phase 3).
2. [x] Infer decisions' and forks' input types ([#47](https://github.com/alexnodeland/reflexr/issues/47)).
3. [x] Save only checkpoints that read back as they were, and start over from those that no longer validate ([#58](https://github.com/alexnodeland/reflexr/issues/58)).
