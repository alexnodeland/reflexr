# ADR-0011: Surfaces: ingest, REST, WebSocket, schedules and MCP

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Events come from other services (over HTTP, in batches, from webhooks), from people and dashboards (live, over WebSocket), from timetables, and from external agents (over MCP). Operators need to see and fix what rules and runs are doing. Reflex offered HTTP and WebSocket ingest and a dead-letter endpoint, with rate limiting and health checks, each with its own handling code.

## Decision

v0.1 ships four surfaces, all thin adapters over one command handler on a `Stream` handle, so they behave identically:

- **HTTP ingest and REST** (`reflexr.fastapi`): publish one event or a batch, idempotent by id; read the log; inspect and administer rules (cursor, lag, replay), runs (retry, skip, cancel) and dead letters.
- **WebSocket** (`reflexr.fastapi`): a resumable subscription with artifactr's handshake (`hello`, replay, `replay_complete`), filters by event type, the same command frames, and the same close codes.
- **Schedules** (`reflexr.stream`): interval and cron schedules that publish into streams. Tick ids derive from the schedule and the tick's time, so racing replicas publish each tick once; a lease keeps them from racing.
- **MCP** (`reflexr.mcp`): publishing, reading and administration as tools, and runs as resources with tenant-qualified URIs and update notifications.

Authentication is the host's: each surface takes a resolver that returns the tenant and actor. Rate limiting and health checks are the application's (FastAPI middleware and its own routes), not the library's.

## Options considered

| Option | Consistency across surfaces | Scope for v0.1 |
|---|---|---|
| **All four over one command handler (chosen)** | By construction | Largest |
| HTTP ingest and REST only | Trivial | Smallest; operators and agents get no live view or tools |
| Per-surface handlers, as in Reflex | Drifts | Medium |

## Consequences

- Easier: a new surface is authentication plus translation into commands.
- Easier: one JSON Schema describes every frame, for clients in any language.
- Harder: four surfaces to test to 100% coverage, and a protocol to version.

## Action items

1. [ ] Implement the schedule runner (RFC-0001 phase 2), and `reflexr.fastapi` and `reflexr.mcp` with generated schemas (phase 5).
