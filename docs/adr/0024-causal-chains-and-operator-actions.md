# ADR-0024: Which chain a firing joins, and operator actions in the log

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

A causal chain (the `correlation_id` on every envelope) is a trace session in Langfuse and the unit of chain feedback and chain evaluation ([RFC-0002](../rfcs/0002-observability-feedback-and-evaluation.md)). An event nothing caused starts a chain; an event a run emits continues its run's chain. What was undecided is which chain a *firing*, and so its run, belongs to, when a rule matched several envelopes from different chains: three errors from three sources, a deploy followed by an alert, or an absence noticed at a tick.

Separately, RFC-0002's operator intervention rate (the share of runs a person retried, skipped or cancelled) needs every intervention in the log, with the actor who made it. Skipping and cancelling appended `run_skipped` and `run_cancelled`; retrying by hand appended nothing.

## Decision

- **A firing joins the chain of the latest envelope it matched.** Core records the chain on each `Match`, so `Firing.correlation_id` is decided by `evaluate` and pinned by conformance fixtures, and `Run.correlation_id` copies it. An absence firing joins the chain of the last envelope the rule saw, not the tick that passed its deadline.
- **Run operations and feedback join the run's chain,** so they appear in its session. Chain feedback joins the chain it is about.
- **Retrying a run by hand appends `run_requeued`,** attributed to whoever did it. `core.retry` returns it, like every other transition.

### Amendment (2026-09-29): a chain is joined by its first event's id

- **A `correlation_id` a client gives must name the start of a chain:** an event whose envelope's `correlation_id` is its own id. Publishing with the id of a later event of a chain, and feedback on a `ChainTarget` with one, is rejected with `validation_failed`, and the message names the chain that event belongs to, so the client can correct it. Accepting any existing event id made a new chain named by an event that was already in another, split the session in two, and left chain feedback about a chain that did not exist. An id not in the log is still `not_found`.
- The check runs before a publish's ids are looked up, so retrying a publish with such a `correlation_id` is rejected too. `errors` is empty, as for the other checks that are not Pydantic's.

## Options considered

### Option A: The latest matched envelope's chain (chosen)

| Dimension | Assessment |
|---|---|
| Complexity | Low: one field on `Match`, decided in core |
| Predictability | High: the envelope that completed the match |

**Pros:** the chain that made the condition hold continues; absence follows the thing that went quiet; decided once, in core, and stored on the run.
**Cons:** the earlier matched envelopes' chains do not include the run, though the firing's `matched` seqs still point to them.

### Option B: The earliest matched envelope's chain

**Pros:** follows the first cause.
**Cons:** for a sequence (a deploy, then errors) the run joins the deploy's chain, not the alert's; a long-running count keeps joining an old chain.

### Option C: A new chain per firing

**Pros:** simple.
**Cons:** breaks the session: a person's report and the agent it triggered would be separate sessions.

### Option D: Derive the chain in the host from the envelope at `fired_seq`

**Pros:** no core change.
**Cons:** wrong for absence (the tick's chain), and every reader of a run would re-read the log.

## Trade-off analysis

Option A is the only one that is right for all four patterns and keeps the decision in the pure core, where the fixtures specify it. The run carries its chain, so admin events, spans and feedback need no extra reads.

## Consequences

- Easier: a Langfuse session shows everything one incident led to, including operators' actions and feedback.
- Easier: the intervention rate is a query over `run_requeued`, `run_skipped` and `run_cancelled` by actor kind.
- Harder: `Match` states are slightly larger (one string per match held).

## Action items

1. [x] `Match.correlation_id`, `Firing.correlation_id`, `Run.correlation_id`, and `chains.json` conformance cases.
2. [x] `run_requeued` and `core.retry` returning it.
3. [x] The reactor appends `rule_fired` in the firing's chain (phase 2c).
