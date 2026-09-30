# ADR-0043: Graph checkpoints

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Alex Nodeland

## Context

Graph actions ([ADR-0008](0008-actions-agents-graphs-and-functions.md)) can take minutes and call models at several steps. Re-running a whole graph after a crash, or after one failing step, repeats work and spend, and may repeat side effects. pydantic-graph 2.51 has no persistence of its own, but it can be driven step by step: `Graph.iter` yields the pending tasks between steps, and `GraphRun.next(requests)` continues a run from given tasks.

[ADR-0009](0009-graph-checkpoints.md) decided to checkpoint at step boundaries, and its amendments changed how. This record states the decision as it stands, superseding it.

## Decision

- **`GraphAction` drives a graph with `Graph.iter`.** At each safe boundary it saves a checkpoint on the run, through `Reaction.checkpoint`, which appends `run_progressed`. The checkpoint holds:
  - the graph state and the graph's inputs, each through a `TypeAdapter` of its type
  - the next task: its node id, its inputs as the node's input type, and its fork stack
  - the graph's nodes, and the format's version, 1
- **A retry, or another executor after a crash, resumes from the latest checkpoint.** It starts a fresh `graph.iter` with the saved state and inputs and sends the saved `GraphTaskRequest`, whose fork-stack ids are prefixed per attempt, since a new run numbers its node runs afresh. The graph's output is checkpointed too, so a run that finished its graph but not its recording does not run it again.
- **A boundary is safe when nothing runs in parallel.** Exactly one task comes next; it is not between a fork and its join (the join's region, per `graph.parent_forks`); no fork without a join is in its stack; and the next node's input type is known. A run that fails inside a fork resumes from the checkpoint before the fork, and its branches run again, at least once as everything is. After the join, boundaries are safe again.
- **Input types come from annotations, and a fork's from the edges into it.**
  - A step's input type is its `StepContext[State, Deps, Input]` annotation, and the end node's is the graph's output type.
  - A fork runs no code, so its input is what the edges into it carry. When every such edge comes, with no transform, from a step with a return annotation or from the graph's start, and they all carry the same type, that is the fork's type. Return annotations are read without `Annotated` metadata, since the graph never validated the value against those constraints.
  - Otherwise the type is unknown, and the boundary before the node is not saved: an edge with a transform, from a step with no return annotation (or `Any`, or a type variable), from a decision, a join or another fork, edges of different types, and an inferred type pydantic has no schema for.
  - `GraphAction(input_types={...})` names a step's or a fork's type, and wins over an annotation or an inference. An explicit type pydantic cannot handle raises.
- **Nothing is saved before a decision.** A decision runs no code, so the boundary after it saves the same progress when the node it routes to has a known type, and that node is where a retry resumes. `input_types` names only steps and forks; `GraphAction` raises `ValueError` for a decision, a join or an id not in the graph.
- **A checkpoint is saved only if what it restores reads back as it was.** The state, the graph's inputs and the next node's inputs are each written down and read back through their types, and the boundary is saved only if each reads back equal. A model given to a stream step or a `BaseNode`, whose input types read as `Any`, would read back as a dict, so that boundary is skipped and a retry resumes from the one before. A one-shot iterator is never written down, since that would use it up.
- **A checkpoint that cannot be resumed starts over**, as any at-least-once retry may. The attempt's `invoke_workflow` span gets a `reflexr.checkpoint.discarded` event with `reflexr.checkpoint.reason`:
  - `format`: not a checkpoint, or another version of the format
  - `graph_changed`: the graph's nodes changed
  - `untyped`: the next node's input type is no longer known, as when an edge into a fork gained a transform, or the next node is a decision
  - `invalid`: a saved value no longer validates, with `reflexr.checkpoint.error` saying where and why, without the values
- **Each step is an `execute_step {node}` span.** pydantic-graph's own spans need Logfire.

## Options considered

| Option | Repeated work after a failure | Infrastructure |
|---|---|---|
| **Checkpoint at safe step boundaries (chosen)** | At most the step that failed, or a fork's branches | None beyond storage |
| Re-run the whole graph | All completed steps | None |
| A durable execution backend (Temporal, DBOS, Prefect) | Minimal | An external service |

Before a decision:

| Option | Assessment |
|---|---|
| **Never save it (chosen)** | Nothing to infer or check; the boundary after the decision saves the same progress |
| Infer its type through chains of decisions, and check its inputs keep their class | A fixpoint and a class check, for a boundary that saves nothing the next one does not |

## Consequences

- Easier: long graph workflows survive crashes and flaky steps without repeating earlier model calls.
- Easier: a decision needs no type; the only inferred types are forks', from the steps before them.
- Harder: steps must be written so that re-running the one in flight is safe, since a crash mid-step repeats it. Steps receive the run's idempotency key for that purpose.
- Harder: a fork after a decision, a join or a transform is not saved before unless `input_types` names its type.
- Harder: a decision that routes to an untyped step, or to a stream step, is saved at neither boundary, so a retry resumes from before the step that fed the decision, unless `input_types` names the routed-to step's type.
- Revisit if pydantic-graph gains persistence of its own: reflexr's checkpoints should then adopt it.

## Action items

1. [x] Checkpoints at safe boundaries, resumed after a crash or a failed step.
2. [x] Forks' input types from the edges into them, and nothing saved before a decision.
3. [x] Save only checkpoints that read back as they were, and start over from those that cannot be resumed.
