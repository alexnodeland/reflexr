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
