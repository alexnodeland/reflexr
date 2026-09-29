# ADR-0027: Executing runs

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

[ADR-0005](0005-per-rule-cursors.md) decided that acting is at least once, under leases, in firing order per scope. Building the executor (RFC-0001 phase 2c) had to settle what happens when executors crash, race, or are cancelled, and what an action is.

## Decision

- **The action is a port** ([ADR-0025](0025-ports-and-adapters.md)): `async (Reaction[D]) -> output`. Functions are actions as they are; agents and graphs are adapters in `reflexr.agent`. `Reaction` lives in `reflexr.workspace`, since function actions need it. It carries:
  - the workspace, acting as the run's `AgentActor` and caused by the run
  - the run, the rule, the matched envelopes, and the application's `deps`
  - `emit(event)`
- **The reactor takes the actions and deps:** `Reactor(workspaces, actions={...}, deps=...)`. It checks at construction that every rule's action is registered.
- **Each attempt is three transactions.** One claims the run, if it is still due and first in its scope, and appends `run_started`. The action then runs outside any transaction, while its lease is renewed. A final transaction records `run_succeeded`, `run_retrying` or `run_dead_lettered`.
- **A stale attempt is discarded.** The outcome is recorded only if the run is still `running` at the same attempt. An executor that lost its lease, or whose run was cancelled, cannot overwrite a newer state.
- **Abandoned attempts count as failures.** A running run whose lease lapsed is due again. Claiming it records a failed attempt ("abandoned"), so the retry policy bounds actions that keep crashing their executors.
- **Cancelling stops the action.** The lease keeper checks the run as it renews. If the run was cancelled, or the lease was lost, it cancels the action, and nothing more is recorded for that attempt.
- **Retries do not emit twice.** `Reaction.emit` derives each event's id from the run and its position within the attempt, so a retry that emits the same events adds nothing.
- **Outputs are JSON:** a Pydantic model is dumped, and anything else that is not JSON fails the attempt. A rule's `timeout` fails an attempt that runs too long.
- **Runs of rules no longer registered are cancelled,** with a reason, so they stop being due.

## Options considered

### Stale completions: compare status and attempt (chosen)

| Dimension | Assessment |
|---|---|
| Correctness | A stale executor can never succeed a newer attempt |
| Complexity | Low: one comparison in the completion transaction |

### Stale completions: compare status only

**Cons:** after a takeover the run is `running` again, and the stale executor would record its outcome over the new attempt's.

### Abandoned attempts: restart without counting

**Cons:** an action that crashes its process would be retried forever.

## Consequences

- Easier: any number of reactors can execute runs; a crash costs one attempt, not a stuck run.
- Easier: operators can cancel a running workflow and it stops within a third of the lease's time to live.
- Harder: actions must tolerate being cancelled at any await, which asyncio code does by default.

## Action items

1. [x] `Executor`, `Reaction`, the action port, and `Reactor.execute`, `settle` and `serve`.
2. [ ] `AgentAction` and `GraphAction` adapters (phase 3).
