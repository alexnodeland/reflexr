# ADR-0010: Loop and spend safety

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Rules that run LLM workflows, whose output can be new events that other rules watch, can loop forever and can spend without bound. A misconfigured rule should fail safe, not run up a bill.

## Decision

- **Causation depth.** Every event records the causal chain it belongs to (`correlation_id`) and, when a run emitted it, the chain's depth. Publishing beyond the stream's limit (8 by default) is rejected with `depth_exceeded`, which fails the run with a clear error.
- **Throttles** on rules cap firings per scope and period.
- **Emit allowlists.** `EventContext(emit=[...])` limits the event types an agent can publish; the reactor enforces it.
- **Concurrency limits** per reactor, rule and stream bound the runs executing at once.
- **Timeouts** per action cancel a run that takes too long; it is then retried or dead-lettered like any failure.
- **Usage limits.** `AgentAction` passes pydantic-ai `UsageLimits` through.

## Options considered

| Option | Stops runaway loops | Bounds spend |
|---|---|---|
| **Depth limit, throttles, allowlists, limits and timeouts (chosen)** | Yes | Yes |
| Document the risk and leave it to applications | No | No |
| Forbid rules from reacting to events runs emit | Yes | Partly, and it rules out useful chains |

## Consequences

- Easier: a mistaken rule stops at a known depth and rate, with an error an operator can see.
- Harder: legitimate long chains need a higher limit, set explicitly per stream.

## Action items

1. [ ] Implement causation and depth checks (RFC-0001 phases 1 and 2), and allowlists and usage limits (phase 3).
