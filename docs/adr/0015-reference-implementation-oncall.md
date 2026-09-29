# ADR-0015: Reference implementation: incident response

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

A reference implementation keeps the public API honest and gives users a working starting point, replacing the Copier template ([ADR-0001](0001-library-with-a-sans-io-core.md)). Reflex had six examples, none of which reached a model: each called attributes that did not exist, inside `except` blocks that fell back silently. The reference should exercise every part of reflexr: windows, scopes, sequences, absence, agents, graphs, functions, schedules and every surface.

## Decision

`examples/oncall` is the reference implementation, an **incident response** application:

- **Events**: `service.error`, `deploy.finished`, `heartbeat`, and the events the workflows emit (`incident.opened`, `page.sent`, `rollback.proposed`).
- **Rules**:
  - `error-spike`: three severe errors from one service within a minute runs a **triage agent**, which opens an incident.
  - `deploy-regression`: a deploy followed by an error spike for the same service within ten minutes runs a **runbook graph** (diagnose, decide, then propose a rollback or escalate).
  - `heartbeat-lost`: no heartbeat from a service for five minutes runs a **paging function**.
- **Surfaces**: a FastAPI server with ingest, REST, WebSocket and MCP, a schedule that keeps time moving, and a terminal client that publishes events and watches the stream live.
- Like artifactr's reference implementation, it is a **uv workspace member** built on the public API only (enforced by the layering test), with SQL storage by configuration, tested end to end with scripted models inside the coverage gate.

It pairs with artifactr for the system that will combine them: an opened incident becomes a document to collaborate on in a chat.

## Options considered

| Option | Windows and scopes | Graphs | Pairs with artifactr |
|---|---|---|---|
| **Incident response (chosen)** | Yes: spikes per service, absence | Yes: a runbook | Yes: incident documents and postmortems |
| Support ticket triage | Light | Light | Yes |
| Fraud screening | Yes | Light | Less |

## Consequences

- Easier: every feature has a realistic example, and the tests prove it end to end.
- Harder: the example is a real application to maintain, held to the library's gates.

## Action items

1. [ ] Build `examples/oncall` (RFC-0001 phase 6).
