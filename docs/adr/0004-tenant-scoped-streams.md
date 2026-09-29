# ADR-0004: Tenant-scoped streams with one log each

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Reflex kept every event in one table with a processing status per row, shared by all producers and consumers, with no notion of tenant. Rules need a total order to be deterministic, applications need tenants kept apart, and the design should match artifactr's workspaces.

## Decision

- **A stream is the unit of ordering, isolation and scale**: a tenant-scoped, append-only log. `Streams(storage, events=[...])` is the entry point, and `await streams.open(tenant_id, stream_id, actor=...)` returns a `Stream` bound to one tenant, stream and actor. Nothing below it takes a raw tenant id.
- **One log per stream with a gap-free `seq`**, assigned inside the append transaction while holding the stream's lock.
- **Publishing is idempotent by event id** within a stream.
- **reflexr's own facts** (firings, run lifecycle, ticks) are appended to the same log, so one `seq` orders everything, one subscription sees everything, and rules can react to them (for example, to a dead-lettered run).
- **Event types are allowlisted** per `Streams`.

## Options considered

| Option | Deterministic order | Tenant isolation | Throughput |
|---|---|---|---|
| **One log per tenant-scoped stream (chosen)** | Per stream | By construction | Serialized per stream; scales with streams |
| One log per deployment | Global | By convention | One serialization point |
| A log per event type | Per type only: rules across types lose order | By convention | Higher |

## Consequences

- Easier: replay, resume and rule evaluation all rely on one `seq`; a stream can be moved or deleted as a unit.
- Harder: a very hot source needs splitting across streams. Partitioning is an open question.
- System events add appends per firing (a firing and at least two run events), which counts against a stream's throughput.

## Action items

1. [ ] Implement `Streams`, `Stream` and the storage protocol (RFC-0001 phase 2).
