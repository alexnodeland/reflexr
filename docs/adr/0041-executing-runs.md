# ADR-0041: Executing runs

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Alex Nodeland

## Context

[ADR-0027](0027-executing-runs.md) decided how the reactor executes runs, at least once, under leases, in firing order per scope ([ADR-0005](0005-per-rule-cursors.md)). Three amendments changed that decision: emitted event ids, the runs `due_runs` returns, and a graceful stop for `serve`.

The graceful stop kept the executor from cancelling an action inside a transaction, by counting each action's storage calls through a context variable (`OpenTransactions`). Storage now makes every cancellation safe instead ([ADR-0042](0042-cancel-safe-storage.md)), so the executor cancels actions plainly.

This record states the decision as it stands, superseding ADR-0027. It also corrects [ADR-0006](0006-rules-as-typed-serializable-data.md)'s amendment, which says `due_runs` takes the disabled rules.

## Decision

### Actions and attempts

- **The action is a port** ([ADR-0025](0025-ports-and-adapters.md)): `async (Reaction[D]) -> output`. Functions are actions as they are; agents and graphs are adapters in `reflexr.agent`. `Reaction` lives in `reflexr.workspace`, since function actions need it. It carries:
  - the workspace, acting as the run's `AgentActor` and caused by the run
  - the run, the rule, the matched envelopes, and the application's `deps`
  - `emit(event)` and `checkpoint(step, state)`
- **The reactor takes the actions and deps:** `Reactor(workspaces, actions={...}, deps=...)`. It checks at construction that every rule's action is registered.
- **Each attempt is three transactions.** One claims the run, if it is still due and first in its scope, and appends `run_started`. The action then runs outside any transaction, while its lease is renewed. A final transaction records `run_succeeded`, `run_retrying` or `run_dead_lettered`.
- **A stale attempt is discarded.** The outcome is recorded only if the run is still `running` at the same attempt. An executor that lost its lease, or whose run was cancelled, cannot overwrite a newer state.
- **Abandoned attempts count as failures.** A running run whose lease lapsed is due again. Claiming it records a failed attempt (`fail(..., reason="abandoned")`, "the attempt was abandoned: its executor stopped"), so the retry policy bounds actions that keep crashing their executors.
- **Cancelling a run stops its action.** The lease keeper checks the run as it renews, every third of the lease's time to live. If the run was cancelled, or the lease was lost, it cancels the action, and nothing more is recorded for that attempt. A renewal that fails is logged, and the lease lapses.
- **Outputs are JSON:** a Pydantic model is dumped, and anything else that is not JSON fails the attempt. A rule's `timeout` fails an attempt that runs too long, with the reason `timeout`.
- **Runs of rules no longer registered are cancelled,** with a reason, so they stop being due.
- **`serve` survives a failing pass.** A tick, evaluation or execution that raises, as when the database is briefly unreachable, is logged, and the next pass tries again.

### Emitted events

- **A retry does not emit twice, and a resumed graph loses nothing.** An emitted event's id derives from the run, the number of checkpoints it has saved (`Run.checkpoints`), and how many events it emitted since the last one. A retry that resumes from the same checkpoint derives the same ids, so publishing them again adds nothing. A graph that emits, checkpoints, and emits again after resuming gets new ids for the later events.
- **Some events only runs may publish.** `Workspaces(emitted=[...])` names event types that runs may publish and clients may not: REST, the WebSocket, MCP, and in-process handles not caused by a run. The incident a triage agent opens is one.

### Due runs

- **Due runs are the runs that can start.** A pending or retrying run of an ordered rule is due only if no earlier run of its scope holds the scope, in firing order (`fired_seq`, then creation). A run holds its scope while it is pending, retrying or running, or dead-lettered if its rule blocks. So a scope's backlog takes one place in the limit, and one claim per pass. A running run whose lease lapsed is due wherever it is, since its claim records the abandoned attempt without checking the order. The claim checks `runnable()` in its transaction, and remains the authority.
- **The storage port is told the rules' policy.** Storage does not see the rules, so `Storage.due_runs(now=, limit=, policy=)` takes a `RunPolicy`, which the executor builds with `RunPolicy.of(rules)`. It names the disabled rules, the rules that run their scopes in firing order, and those among them whose dead-lettered runs block their scope. A rule the policy does not name has all its due runs returned, so a policy that says too little costs declined claims, never a run that is not attempted.
- **SQL storage finds them in one query.** A `NOT EXISTS` looks for an earlier run of the same tenant, workspace, rule and scope, in a holding status, ahead in firing order (the row value `(fired_seq, position)`). The index `ix_reflexr_runs_scope_head` (migration `0002`) makes each check a seek per status that never reads the scope's succeeded runs.
- **Contention is not idleness.** An attempt that finds its run's lease held by another reactor reports it, and so does an evaluation that finds its workspace's lease held. `settle()` does not return after a round that did nothing but meet contention. It waits 2 ms, doubling up to 100 ms, and tries again; those rounds count towards `max_rounds`, so a lease that is never released makes `settle()` raise rather than hang.

### Stopping

- **`serve(stop=, grace=)`.** The caller owns an `asyncio.Event` and sets it, from a FastAPI lifespan or a signal handler (`loop.add_signal_handler(signal.SIGTERM, stop.set)`). Like `poll_interval`, it belongs to one serving loop, so the reactor keeps no state about being served. Without `stop`, `serve` runs until it is cancelled. It returns `None`.
- **Once `stop` is set:**
  - evaluation stops after its current batch, so the next reactor carries on from the cursors
  - attempts still waiting for a place (`concurrency`) do not start
  - running attempts have `grace` to end, 5 seconds by default
  - then a task in `serve` sets a second event, `grace_over`, and the executor cancels each action still running and records its attempt as abandoned, in a transaction of its own
  - every lease is released, and `serve` returns
- **Abandoned is abandoned, however the executor stopped.** A stopped reactor records the transition and error a claim records for an attempt whose executor died. Recording it at once means another reactor retries the run after the rule's backoff, rather than after its lease lapses. A graph resumes from its last checkpoint.
- **The default grace is 5 seconds**, within the 10 seconds Docker allows a container between `SIGTERM` and `SIGKILL`, leaving time for the rest of the application's shutdown. A platform that allows longer, such as Kubernetes' 30 seconds, can pass a longer grace.

## Options considered

### Stopping `serve`

| Option | Assessment |
|---|---|
| **`serve(stop=, grace=)`, with `grace_over` set by a task in `serve` (chosen)** | Two plain events; nothing to await but `serve` itself |
| `Reactor.stop()` | State on the reactor, and a second method to await for the stop to finish |
| A stopping object with a lazily started deadline (ADR-0027) | A shared deadline and public mutable flags, beside the plain event the reactor's evaluation took |

### Stale completions

| Option | Assessment |
|---|---|
| **Compare status and attempt (chosen)** | A stale executor can never record over a newer attempt; one comparison in the completion transaction |
| Compare status only | After a takeover the run is `running` again, and the stale executor would record its outcome over the new attempt's |

Restarting abandoned attempts without counting them was rejected: an action that crashes its process would be retried forever.

## Consequences

- Easier: the executor and the workspace handles know nothing of transactions; a stop is two events, and the executor cancels an action wherever it is.
- Easier: any number of reactors can execute runs; a crash costs one attempt, not a stuck run, and a cancelled run stops within a third of the lease's time to live.
- Harder: actions must tolerate being cancelled at any await, which asyncio code does by default.

## Action items

1. [x] `Executor`, `Reaction`, the action port, and `Reactor.execute`, `settle` and `serve`.
2. [x] `AgentAction` and `GraphAction` adapters.
3. [x] Due runs that can start, and a `settle()` that waits out contention ([#57](https://github.com/alexnodeland/reflexr/issues/57)).
4. [x] A graceful stop for `serve()` ([#63](https://github.com/alexnodeland/reflexr/issues/63)).
5. [x] `OpenTransactions` removed, since storage is cancel-safe ([ADR-0042](0042-cancel-safe-storage.md)).
