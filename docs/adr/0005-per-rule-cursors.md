# ADR-0005: Per-rule cursors: decide exactly, act at least once

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Reflex claimed each event as a whole, ran every matching trigger, and then acknowledged or rejected the event once. A failure in one trigger re-ran all of them, including those that had already succeeded. An event left `processing` by a crash was never reclaimed, one unparseable payload stopped the subscription, and stateful checks lived in process memory.

## Decision

Processing is split into deciding and acting, with different guarantees.

- **Deciding is exact.** One evaluator holds each stream's lease and evaluates every rule over new envelopes. Each rule has its **own cursor**. For each batch, the rule's per-scope state, its cursor, the `RuleFired` envelopes and the pending runs are saved in **one transaction**, so each envelope affects each rule exactly once, whatever crashes. A rule added or reset later catches up on its own.
- **Evaluation errors are per rule.** A predicate that raises, or a missing scope field, is recorded as `RuleErrored` and dead-lettered for that rule; the rule moves on.
- **Acting is at least once.** Executors claim runs under leases they renew while working; an expired lease makes the run claimable again. Failures retry with exponential backoff until the rule's retry policy is exhausted, then the run is dead-lettered.
- **Ordering is per scope.** By default (`ordering="scope"`), runs of one rule and scope execute in firing order, so a run waiting to retry holds back only its own scope. `ordering="none"` runs them in parallel. After a dead letter, later runs continue (`on_dead_letter="continue"`, the default) or wait for an operator (`"block"`).
- **The firing id is the run id and the idempotency key.** Events a run emits get ids derived from it, so retries do not duplicate them.

## Options considered

| Option | One rule's failure affects others | Crash recovery | Replay |
|---|---|---|---|
| **Log with per-rule cursors (chosen)** | No | Leases on evaluation and runs | Reset a cursor |
| Queue per event (Reflex) | Yes: all triggers retry together | Stuck `processing` rows | Separate code path |
| Log plus a work queue per event | Partly | Leases | Yes, with more moving parts |

## Trade-off analysis

Separating a pure, transactional decision from an impure, retried action is what makes each half's guarantee achievable. Exactly-once decisions are cheap because they touch only storage; exactly-once actions are impossible in general, so reflexr offers at-least-once with an idempotency key and says so.

## Consequences

- Easier: a failing or slow rule is contained to itself and its scope; replay after a fix is resetting a cursor.
- Harder: storage keeps more state (cursors, per-scope state, runs), and operators need tools to see it: the REST, WebSocket and MCP administration commands.

## Action items

1. [x] Implement evaluation and execution in the `Reactor` (RFC-0001 phase 2).
