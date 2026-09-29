# ADR-0028: Schedules, with cronsim for cron expressions

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Schedules publish ticks into workspaces on a timetable, every interval or on a cron expression in a time zone. Rules watch ticks, and ticks move rules' clocks forward in quiet workspaces, so `absence` rules fire on time ([ADR-0007](0007-rule-state-as-pure-reducers.md)). RFC-0001 left the cron parser to phase 2. Replicas must never publish a tick twice, and a reactor that was down must not flood the log when it returns.

## Decision

- **cronsim parses cron expressions.** It is small, typed, dependency-free and BSD-licensed. It is maintained as part of Healthchecks.io, and it handles daylight saving through `zoneinfo`.
- **`Schedule(name, every= | cron=, timezone=, workspaces="all" | [(tenant, workspace)], catch_up="skip" | "all", max_catch_up=100)`** is validated data, registered on `Workspaces` like rules. `schedule.after(moment)` gives its next times, for listing them.
- **Each workspace remembers each schedule's last tick,** saved in the transaction that appends the tick, and a tick's id is derived from the schedule and its time. Replicas therefore cannot publish a tick twice, and no lease is needed.
- **A schedule's first check in a workspace starts its timetable;** its first tick comes at its next time after that. After downtime, `"skip"` publishes only the latest missed tick and `"all"` publishes each, up to `max_catch_up`. Interval schedules compute missed ticks arithmetically, so a long outage costs nothing.
- **The reactor publishes ticks:** `Reactor.tick()`, which `settle()` and `serve()` include. Ticks are attributed to `SystemActor(name="scheduler")`, and each tick starts its own causal chain.

## Options considered

### cronsim (chosen)

| Dimension | Assessment |
|---|---|
| Typing | Ships `py.typed`; works under pyright strict |
| Dependencies | None |
| Time zones | `zoneinfo`, correct across daylight saving |

### croniter

**Pros:** the most widely used, now maintained under pallets-eco.
**Cons:** not typed, and larger than reflexr needs.

### A cron parser of our own

**Cons:** cron semantics, and daylight saving in particular, are easy to get subtly wrong.

## Consequences

- Easier: heartbeat checks, nightly reports and business-hours rules are a schedule and a rule.
- Harder: ticks add envelopes to every targeted workspace's log; long-lived logs will want the retention work listed in the open questions.

## Action items

1. [x] `Schedule`, `tick_id`, schedule state in storage, `Reactor.tick`.
2. [ ] `GET /v1/schedules` with next ticks (phase 5).
