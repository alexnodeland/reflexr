# ADR-0016: Tenants and workspaces, like artifactr

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

[ADR-0004](0004-tenant-scoped-streams.md) made a tenant-scoped **stream** the unit of ordering and isolation. artifactr's unit is a tenant-scoped **workspace**, with one log. The system that will combine the two libraries works over a shared context, and that is easiest when both libraries divide tenants the same way, so the same tenant and workspace ids can name the same context in each.

## Decision

- reflexr has **tenants and workspaces, like artifactr**. A tenant owns workspaces; `Workspaces(storage, events=[...])` is the entry point, and `await workspaces.open(tenant_id, workspace_id, actor=...)` returns a `Workspace` bound to one tenant, workspace and actor. Nothing below it takes a raw tenant id.
- **A workspace has one log** with a gap-free `seq`: its event stream. Rules, their cursors and state, runs and dead letters all belong to the workspace. "Stream" remains the word for that log, as in the stream protocol, and means a workspace's log in earlier records.
- The workspace layer is the **`reflexr.workspace`** package (formerly planned as `reflexr.stream`), and envelopes carry `workspace_id`.
- Everything else in ADR-0004 stands: sequencing under the workspace's lock, idempotent publishing by event id, reflexr's own facts in the same log, and the event type allowlist.

### Amendment (2026-09-29): rules and schedules are shared by every tenant

- **Rule and schedule definitions are the application's code, not a tenant's data.** They belong to `Workspaces`, every tenant's workspaces evaluate the same ones, and every authenticated client of any tenant may read them all: `GET /rules`, `GET /schedules` and the MCP `list_rules` tool return them, and `authorize` is not asked, since they name no workspace. A schedule's targets are the exception, since they name tenants' workspaces: `GET /schedules` narrows them to the caller's tenant, and leaves out a schedule that targets none of its workspaces, so no tenant sees another's ids. This is by design, and the security guide and the protocol say so plainly; an application that must hide them does not mount those routes for some clients. What a rule does in a workspace (its cursor, state, runs and dead letters) stays the workspace's.

## Options considered

| Option | Matches artifactr | Ordering | Concepts |
|---|---|---|---|
| **Workspace is the unit, with one log (chosen)** | Exactly | Total, per workspace | No new ones |
| Workspaces that contain several streams | Partly | Per stream only | One more (stream) |
| Streams without workspaces (ADR-0004) | No | Per stream | Different from artifactr |

## Consequences

- Easier: an application or the combined system can open the same tenant and workspace in both libraries; the shared context has a natural key.
- Easier: one mental model and one set of names across the two libraries.
- Harder: a hot source cannot be split within a workspace yet; partitioning a workspace's log stays an open question.

## Action items

1. [x] Update the architecture, protocol, RFC-0001, contributing and security documents.
2. [ ] Build the workspace layer as `reflexr.workspace` (RFC-0001 phase 2).
