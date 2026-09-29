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

### Amendment (2026-09-29): what the reference implementation taught

- **Emitted ids restart per checkpoint.** An emitted event's id derives from the run, the number of checkpoints it has saved (`Run.checkpoints`), and how many events it emitted since the last one. Numbering from zero on every attempt made a graph that emitted in one step, checkpointed, and emitted again after resuming reuse the first id, so the later event was taken for a duplicate and lost. A retry that resumes from the same checkpoint still derives the same ids, so it still does not emit twice.
- **Events only runs may publish.** `Workspaces(emitted=[...])` names event types that runs may publish and clients (REST, the WebSocket, MCP, in-process handles not caused by a run) may not, such as the incident an agent opens.
- **`Reactor.serve` survives a failing pass.** A tick, evaluation or execution that raises, such as when the database is briefly unreachable, is logged and the next pass tries again; leases and transactions leave nothing half done.

### Amendment (2026-09-29): due runs are the runs that can start, and contention is not idleness

`Storage.due_runs` returned every pending, retrying or lapsed running run, oldest first, up to the executor's `limit`. Firing order was checked only in the claim, after the executor had taken the run's lease and opened a transaction ([#57](https://github.com/alexnodeland/reflexr/issues/57)). So:

- **A scope's backlog held up every other scope.** With 150 runs waiting in one scope and one run fired last in another, the other scope's run waited 52 passes, about 52 seconds at `serve()`'s default poll, until the backlog fell below the limit.
- **Every pass did work in proportion to the backlog.** Each waiting run cost a lease, a claim transaction and an `invoke_workflow` span, only for the claim to decline all but the first run of each scope.
- **`settle()` could stop early with two reactors.** One reactor leased runs of a scope that could not start yet, and declined them. Meanwhile the other finished the run ahead of them, found them leased, and took that for nothing to do. Both settled with runs pending, which `serve()` picked up on its next poll. On SQLite, where a reactor waiting for the write lock holds such leases for a long time, the two-process test failed 10 times in 300.

Decided:

- **Due runs are the runs that can start.** A pending or retrying run of an ordered rule is due only if no earlier run of its scope holds the scope, in firing order (`fired_seq`, then creation). A run holds its scope while it is pending, retrying or running, or dead-lettered if its rule blocks. So a scope's backlog takes one place in the limit, and one claim per pass. A running run whose lease lapsed is due wherever it is, since its claim records the abandoned attempt without checking the order. The claim still checks `runnable()` in its transaction, and remains the authority.
- **The storage port is told the rules' policy.** Storage does not see the rules, so `Storage.due_runs(now=, limit=, policy=)` takes a `RunPolicy`, which the executor builds with `RunPolicy.of(rules)`. A rule the policy does not name has all its due runs returned, as before, so a policy that says too little costs declined claims, never a run that is not attempted. The policy names:
  - the disabled rules, which replace the `disabled` argument
  - the rules that run their scopes in firing order
  - those among them whose dead-lettered runs block their scope
- **SQL storage finds them in one query.** A `NOT EXISTS` looks for an earlier run of the same tenant, workspace, rule and scope, in a holding status, ahead in firing order (the row value `(fired_seq, position)`). The blocking rules are checked apart from the other ordered rules, so each check lists its statuses plainly. A new index, `ix_reflexr_runs_scope_head` on tenant, workspace, rule, scope, status, `fired_seq` and position (migration `0002`), makes each check a seek per status that never reads the scope's succeeded runs. With 20,000 succeeded and 200 waiting runs in one scope, beside 50 other scopes, the query takes 5 ms on SQLite and 4 ms on PostgreSQL with the index, and 1.6 s and 146 ms without it.
- **Contention is not idleness.** An attempt that finds its run's lease held by another reactor reports it, and so does an evaluation that finds its workspace's lease held. `settle()` does not return after a round that did nothing but meet contention, since the other reactor may be about to let that work go. It waits 2 ms, doubling up to 100 ms, and tries again. Those rounds count towards `max_rounds`, so a lease that is never released makes `settle()` raise rather than hang. `serve()` is unchanged, apart from doing less wasted work, and `Reactor.execute()` and `evaluate()` still return counts.

The evidence, before and after:

- The starved scope's run executes on the first pass instead of the 52nd.
- Each pass claims one run per scope, and every claim is an attempt, whether a scope's backlog is 2 runs deep or 25.
- The two-process test, run 300 times: 10 failures on SQLite before, none after. PostgreSQL had none before or after.
- A test drives the race step by step, with two reactors on one storage: it left two runs pending every time before, and leaves none now.

### Amendment (2026-09-29): a graceful stop

`serve()` stopped only when cancelled, and a cancellation lands wherever the reactor is, inside a transaction too. SQLAlchemy takes a driver call cancelled part-way for a lost connection. On SQLite the connection could be dropped still holding the database's write lock, or not returned to the engine's one-connection pool. stackr's application template found it ([stackr#17](https://github.com/alexnodeland/stackr/pull/17)): the lease release in the reactor's cleanup failed with `database is locked` ([#63](https://github.com/alexnodeland/reflexr/issues/63)). Of 300 cancellations at random points in SQLite transactions, 33 left the database locked (cancelled in a query) and 16 left the pool without its connection (cancelled in the commit).

Decided:

- **`serve(stop=, grace=)`**, rather than `Reactor.stop()`. The caller owns an `asyncio.Event` and sets it, from a FastAPI lifespan or a signal handler (`loop.add_signal_handler(signal.SIGTERM, stop.set)`). Like `poll_interval`, it belongs to one serving loop, so the reactor keeps no state about being served, and nothing has to say which `serve` a `stop()` would stop. Neither reflexr nor artifactr had a stoppable loop to follow: their loops, such as the feedback mirror, stop by being cancelled. Without `stop`, `serve` runs until cancelled, as before, and is typed `Never`; with it, it returns `None`.
- **A stop never interrupts a transaction.** Once `stop` is set:
  - evaluation stops after its current batch, so the next reactor carries on from the cursor
  - attempts still waiting for a place (`concurrency`) do not start
  - running attempts have `grace` to end, 5 seconds by default
  - then each action is cancelled, and its attempt recorded as abandoned by the executor, in a transaction of its own, never by the cancelled task
  - every lease is released, and `serve` returns
- **Abandoned is abandoned, however the executor stopped.** The attempt is recorded with the transition and error a claim records for an attempt whose executor died (`fail(..., reason="abandoned")`, "the attempt was abandoned: its executor stopped"), so it counts as a failed attempt under the retry policy. Recording it at once means another reactor retries it after the rule's backoff, rather than after its lease lapses. A graph resumes from its last checkpoint.
- **Actions are cancelled between their storage calls.** Each action runs in a task whose workspace handles count the storage calls they have in flight (`OpenTransactions`, found through a context variable), and the executor cancels it only when none is. A write is a transaction, and on SQL so is a read, which on SQLite takes the write lock. A subscription is not counted, since it waits between reads for as long as nothing is logged. That covers every cancellation the executor makes: when a stop's grace is over, and when a run is cancelled or its lease lost. A rule's `timeout` still cancels the action wherever it is, and so does cancelling `serve`.
- **The default grace is 5 seconds**, within the 10 seconds Docker allows a container between `SIGTERM` and `SIGKILL`, leaving time for the rest of the application's shutdown. A platform that allows longer, such as Kubernetes' 30 seconds, can pass a longer grace.

Considered and rejected:

- **`Reactor.stop()`:** state on the reactor, and a second method to await for the stop to finish.
- **Making cancellation safe inside SQL storage,** by shielding each database call: a transaction would outlive the cancelled task that owns it, and every storage adapter would need the same care.

The evidence:

- Stopping a reactor with attempts running, on in-memory storage, SQLite and PostgreSQL, leaves no lease held, and a new transaction on the workspace commits at once. Attempts that end within the grace are recorded as they ended, and those that do not as abandoned.
- A test stops a reactor while its action saves a checkpoint, and again while it reads the log. It fails if the action is cancelled as soon as the grace is over, in the middle of the call.

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
3. [x] Due runs that can start, and a `settle()` that waits out contention ([#57](https://github.com/alexnodeland/reflexr/issues/57)).
4. [x] A graceful stop for `serve()` ([#63](https://github.com/alexnodeland/reflexr/issues/63)).
