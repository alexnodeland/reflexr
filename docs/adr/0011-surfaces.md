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

### Amendment (2026-09-29): every surface takes the authorize hook

- **`ReflexrMcp(authorize=...)`** takes the router's hook, `authorize(tenant_id, workspace_id, actor) -> bool`, and asks it on every tool call and resource read that names a workspace, before the workspace is opened. A refusal is a tool error, or a failed resource read, carrying the `forbidden` rejection's message, as every other rejection over MCP carries its message. `list_rules` names no workspace and is not asked. Without the hook, an MCP client could use any workspace of its tenant however the application restricted REST and the WebSocket.
- **The hook's type, `Authorize`, lives in `reflexr.workspace`,** beside `Workspaces.open`, since adapters may not import each other ([ADR-0025](0025-ports-and-adapters.md)). `reflexr.fastapi.Authorize` is the same alias, re-exported.

### Amendment (2026-09-29): MCP reports what REST reports, and checks subscriptions

MCP's `rule_status` computed its own listing from `rule_progress()`, so it disagreed with REST's; MCP had no schedule status; and a client could listen for any tenant's run notifications.

- **What a surface reports, the workspace computes.** `RuleStatus` and `ScheduleStatus` move from `reflexr.fastapi` to `reflexr.workspace`, and `Workspace.rule_statuses()` and `Workspace.schedule_statuses()` compute them from the registered rules and schedules. REST returns them unchanged, with the same JSON, and MCP formats them as text, so neither adapter computes a status itself ([ADR-0025](0025-ports-and-adapters.md)). `reflexr.fastapi` still exports both, as the same classes.
- **`rule_status` lists every registered rule,** as `GET /workspaces/{id}/rules` does: whether it is enabled, and its cursor, lag, generation and dead letters. It had listed every rule with a cursor, so it showed rules no longer registered and left out registered rules that had never evaluated the workspace, such as a disabled rule that had never run.
- **`schedule_status`,** a new tool, lists each schedule that targets the workspace with its last and next tick, as `GET /workspaces/{id}/schedules` does. It asks `authorize`, as every tool that names a workspace does, and a test fails if a new tool is not asked.
- **`list_runs` takes `scope_key`,** as `GET /workspaces/{id}/runs` does. It holds the scope's values, since the MCP SDK parses a JSON array given as a string before validation, and `reflexr.core.scope_key` builds the key from them.
- **A subscription is checked when it opens.** The MCP SDK serves `subscriptions/listen` itself, so a middleware, passed through `MCPServer(middleware=...)`, makes a resource read's checks for each run URI a listen request names: that the run is the client's tenant's, and then `authorize`. A refusal fails the request with `INVALID_PARAMS` and the read's message; a request that names no run is not checked, and `resolve` is not called for it. Until now any client could listen for notifications on any tenant's runs, learning which moved and when. URIs are matched as `run_uri` writes them, which is what notifications name. The SDK calls its middleware provisional; its version is pinned in `uv.lock`, and the tests exercise the check through the SDK's client. artifactr made the same change in its ADR-0012.
- **Deliberate differences remain.** MCP's `read_events` and `list_runs` return 50 and 20 by default, for a model's context, where REST returns everything unless given a `limit`. Batch publishing and command ids for deduplication are REST's and the WebSocket's; over MCP an event's `id` makes a publish idempotent. `GET /schedules` has no MCP tool yet: its narrowing of targets to the caller's tenant would move to the workspace layer first.

### Amendment (2026-09-29): windows, type filters and tails of the log, and joining at its head

`Workspace.read` had no type filter and no tail. So REST's `type=`, MCP's `read_events`, `EventContext.read_events` and oncall's runbook all read the whole log and filtered it in Python ([#46](https://github.com/alexnodeland/reflexr/issues/46)). A WebSocket client that wanted only what happens next still received the whole replay. artifactr shares the protocol's shape, and made the same additions with the same names in its ADR-0005.

- **A read is a window, a filter and a page.** `Storage.read`, `Workspace.read`, `GET /workspaces/{id}/events` and MCP's `read_events` take:
  - `after_seq` and a new `before_seq`, for the window `after_seq < seq < before_seq`;
  - `types`, which REST spells as a repeated `type`;
  - `limit` (the first so many) or a new `last` (the last so many, still oldest first).
- **Using the tail.** `last` reads the tail, and `last` with `before_seq` set to the oldest `seq` a client has pages backwards. Every read returns the log in order, whichever end the page comes from, so a client handles every page the same way.
- **Checking arguments.** A read with both `limit` and `last`, or with a negative number, is `validation_failed`. `Workspace.read` checks this once, so storage adapters don't have to.
- **Storage filters, not the handle.** The port takes the new parameters, so each adapter pushes them down. SQL storage keeps each event's type in a new `event_type` column, beside `event_id`. Migration 0003 fills the column from the stored envelopes and indexes it with the workspace and `seq`. SQL storage reads the last so many backwards from the end. `Transaction.read`, which evaluation uses, is unchanged.
- **Every reader passes its filter down:** REST, MCP, `EventContext.read_events` (which also takes `before_seq` and `last`, capped at `read_limit`) and the runbook's `diagnose`.
- **Defaults.** MCP still returns 50 when given neither `limit` nor `last`, and `EventContext.read_events` returns 20.
- **`hello` takes `from_head`.** With `from_head: true`, a connection starts at `head_seq`: nothing is replayed, and `replay_complete` follows `welcome` at once.
  - `core.resume` decides this. It refuses `from_head` with a nonzero `resume_after_seq`, and the stream closes with 4400.
  - A client that reconnects resumes from its position, so it cannot skip what it missed by asking for the head again.
  - It is a new optional field, so the protocol stays `reflexr.v1`.
- **The stream has no tail of its own.** A tail read over REST, followed by `hello` with `resume_after_seq` set to the tail's last `seq`, shows recent history and then everything after it, with no gap. So `hello` needs only a place to start, not a count.

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
