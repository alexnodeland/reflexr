# ADR-0001: A library with a sans-IO core, replacing the template

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Reflex was a template to copy with Copier, plus a package that the copy imported. Copies of a template diverge, and fixes never reach them. The package itself mixed decisions with I/O: filters, triggers, retries and locking were entangled with a PostgreSQL store and global settings, so its tests mocked the store and the most important code (the event loop, 13% covered) was barely tested.

artifactr faced the same choice and became a library with a pure core ([artifactr ADR-0001 and ADR-0013](https://github.com/alexnodeland/artifactr/tree/main/docs/adr)). reflexr should meet the same standard.

## Decision

- reflexr is a **library**: the `reflexr` distribution holds `core`, `stream` and `agent`, and extras add `sql`, `fastapi` and `mcp`. Applications extend it with event types, rules, actions and storage implementations.
- **`reflexr.core` is sans-IO**: pure, synchronous functions over immutable Pydantic values, depending on pydantic alone. It decides (`evaluate`, `next_attempt`, `due_ticks`); the host loads state, asks, and saves the result in its own transaction.
- A **conformance suite** of JSON fixtures specifies core's behaviour, independent of Python.
- A **reference implementation** in `examples/` is built on the public API only ([ADR-0015](0015-reference-implementation-oncall.md)).
- The **Copier template is removed**. There is no starter template for now; the reference implementation is the starting point.

## Options considered

| Option | Downstream maintenance | Testability | Effort |
|---|---|---|---|
| **Library with a sans-IO core (chosen)** | Low: fixes flow through a dependency | High: decisions tested without I/O | High: a rebuild |
| Repair the template in place | High: copies diverge | Low: decisions stay tied to I/O | Medium |
| Library with an async core that calls storage | Low | Medium: every test needs a storage | Medium |

## Trade-off analysis

The expensive failures in Reflex were decisions made wrong (thresholds never checked, one acknowledgement for many triggers) and hidden by I/O-heavy tests. A pure core makes every decision cheap to test exhaustively and replayable, and puts all I/O in one layer with one behaviour suite. The rebuild costs more up front than a repair, but a repair keeps the structure that produced the bugs.

## Consequences

- Easier: 100% branch coverage of the decision logic with fast tests; porting core or checking another implementation against the fixtures.
- Easier: new applications are event types, rules and actions, not a forked codebase.
- Harder: the public API needs care and semantic versioning. Template users have no automatic upgrade; the old code stays on its branch and in history.

## Action items

1. [ ] Remove the template and package, and lay the library's foundation (RFC-0001 phase 0).
2. [ ] Implement `reflexr.core` with the conformance suite (phase 1).
