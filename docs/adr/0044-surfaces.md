# ADR-0044: Surfaces

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Alex Nodeland

## Context

Events come from other services (over HTTP, in batches, from webhooks), from people and dashboards (live, over a WebSocket), from timetables, and from external agents (over MCP). Operators need to see and fix what rules and runs are doing.

[ADR-0011](0011-surfaces.md) decided the surfaces, and its amendments changed them. This record states the decision as it stands, superseding it.

## Decision

- **Four surfaces, each a thin adapter over a `Workspace` handle.** Every command goes through one handler, `reflexr.workspace.execute`, so they behave identically:
  - **HTTP ingest and REST** (`reflexr.fastapi`): publish one event or a batch, idempotent by id; read the log; inspect and administer rules (cursor, lag, replay), runs (retry, skip, cancel) and dead letters.
  - **WebSocket** (`reflexr.fastapi`): a resumable subscription with artifactr's handshake (`hello`, replay, `replay_complete`), a filter by event type, the same command frames, and the same close codes.
  - **Schedules** (`reflexr.workspace`): interval and cron schedules that publish ticks into workspaces ([ADR-0028](0028-schedules-and-cronsim.md)).
  - **MCP** (`reflexr.mcp`): publishing, reading and administration as tools, and runs as resources with tenant-qualified URIs and update notifications.
- **Authentication is the host's.** Each surface takes a resolver that returns the tenant and the actor: `resolve_actor(connection)` for REST and the WebSocket, and `resolve(ctx)` for MCP, whose `ctx` is an `McpContext`, the SDK's `Context` with its request typed as Starlette's `Request`. Rate limiting and health checks are the application's too.
- **Authorization within a tenant is one hook, asked in one place.** The router and `ReflexrMcp` take the same `authorize(tenant_id, workspace_id, actor)` (`reflexr.workspace.Authorize`) and pass it to `Workspaces.open(..., authorize=)`, which raises `Forbidden` ("this workspace is not yours to use") before the handle exists. REST answers 403, the WebSocket closes with 4403, and MCP fails the tool call, resource read or run subscription with the message. `GET /rules`, `GET /schedules` and `list_rules` name no workspace, so they are not asked.
- **What a surface reports, the workspace layer computes.** `Workspace.rule_statuses()` lists every registered rule (enabled, cursor, lag, generation, dead letters), `Workspace.schedule_statuses()` each schedule that targets the workspace with its last and next tick, and `Workspaces.schedules_for(tenant_id)` the schedules as one tenant may see them, with their targets narrowed to its own workspaces. REST returns them as JSON and MCP formats them as text, so neither computes a status itself ([ADR-0025](0025-ports-and-adapters.md)).
- **A read is a window, a filter and a page.** `Storage.read`, `Workspace.read`, `GET /workspaces/{id}/events` and MCP's `read_events` take `after_seq` and `before_seq` for the window `after_seq < seq < before_seq`, `types` (REST's repeated `type`), and `limit` (the first so many) or `last` (the last so many, still oldest first). `Workspace.read` refuses both `limit` and `last`, or a negative number, as `validation_failed`, so storage adapters need not; each adapter filters in its store.
- **`hello` takes `from_head`.** A connection with `from_head: true` starts at `head_seq`, replaying nothing, and `core.resume` refuses it with a nonzero `resume_after_seq`. A tail read over REST, then `hello` with `resume_after_seq` at the tail's last `seq`, shows recent history and everything after it with no gap, so the stream needs no tail of its own.
- **A run subscription is checked when it opens.** The MCP SDK serves `subscriptions/listen` itself, so a middleware makes a resource read's checks for each run URI a request names: that the run is the client's tenant's, and `authorize`.
- **Command ids deduplicate per participant.** REST and the WebSocket remember recent results by tenant, workspace, the actor's `participant` and `command_id`, so a retried command is carried out once, whichever handle of the same participant sends it. Over MCP, an event's `id` makes a publish idempotent.
- **Deliberate differences:**
  - MCP's `read_events` and `list_runs` return 50 and 20 when given no `limit`, for a model's context, where REST returns everything.
  - Batch publishing and command ids are REST's and the WebSocket's.
  - MCP has no tool that lists schedules. `Workspaces.schedules_for` narrows them, so one would be a thin adapter when a client needs it.

## Options considered

| Option | Consistency across surfaces | Scope |
|---|---|---|
| **All four over one command handler (chosen)** | By construction | Largest |
| HTTP ingest and REST only | Trivial | Smallest; operators and agents get no live view or tools |
| Per-surface handlers | Drifts | Medium |

Refusing a workspace:

| Option | Assessment |
|---|---|
| **`Workspaces.open(authorize=)` raises `Forbidden` (chosen)** | One check and one message; each surface translates the rejection |
| Each surface asks the hook and words its own refusal | The same check twice, and two copies of the message |

## Consequences

- Easier: a new surface is authentication plus translation into commands.
- Easier: one JSON Schema describes every frame, for clients in any language.
- Harder: four surfaces to test to 100% coverage, and a protocol to version.

## Action items

1. [x] REST, the WebSocket, schedules and MCP over one command handler.
2. [x] `authorize` on every surface, through `Workspaces.open`.
3. [x] Statuses computed by the workspace layer, and windows, type filters and tails of the log.
