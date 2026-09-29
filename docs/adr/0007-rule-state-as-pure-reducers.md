# ADR-0007: Rule state as pure reducers, with the log as the clock

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Rules like "three errors within a minute" or "no heartbeat for five minutes" need state. Reflex kept it in process memory, so it was wrong with more than one process and lost on restart, and it used the wall clock, so no run could be reproduced.

## Decision

- Every stateful stage has a **pure reducer**: `reduce(state, envelope) -> (state, output)`. State is a Pydantic model, stored as JSON per rule and scope, and saved in the same transaction as the rule's cursor ([ADR-0005](0005-per-rule-cursors.md)).
- **The log is the clock.** Windows are measured with envelopes' `ts`, assigned when they are appended. Evaluation never reads a clock.
- **Time advances with the log.** Every envelope advances a rule's clock, including envelopes its filter rejects: time is a separate input to the stateful stages. A deadline that passes (such as the end of an `absence` window) applies to every scope that already has state. Schedules append `Tick` events so that quiet streams still move time forward.
- **Log time never decreases** within a stream: `ts` is assigned under the stream's lock as the later of the clock and the previous `ts` ([ADR-0004](0004-tenant-scoped-streams.md)).
- **Replay is exact**: evaluating a log from the start reproduces its firings and states. Property tests check it.
- **Changing a rule's definition resets its state**, and the reset is recorded in the log.

## Options considered

| Option | Correct across processes | Reproducible | Database load |
|---|---|---|---|
| **Pure reducers stored with the cursor (chosen)** | Yes | Yes | Small state documents |
| Query the log on demand | Yes | Yes | Queries per event and rule |
| In memory (Reflex) | No | No | None |

| Clock | Deterministic | Absence detection latency |
|---|---|---|
| **Log time (chosen)** | Yes | Until the next envelope or tick |
| Wall clock | No | Immediate |

## Trade-off analysis

Pure reducers make stateful rules as testable as stateless ones and make replay trustworthy, which is what lets an operator fix a rule and re-evaluate history with confidence. The cost of log time is latency for absence detection in quiet streams, which a schedule bounds.

## Consequences

- Easier: conformance fixtures specify stateful behaviour exactly; replay after a fix is safe.
- Harder: reducers must stay pure and their state must stay small. State that grows with traffic (dedupe keys, count windows) is pruned by the window it serves.

## Action items

1. [ ] Implement the reducers and property tests (RFC-0001 phase 1).
