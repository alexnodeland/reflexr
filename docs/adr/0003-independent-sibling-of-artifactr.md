# ADR-0003: An independent sibling of artifactr, with aligned conventions

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

artifactr serves live chats in which people and agents collaborate through shared artifacts. reflexr serves background workflows that rules start when events happen. A third system is planned that brings them together over a shared context: for example, a rule that notices an incident and opens a postmortem document in an artifactr workspace, or a chat agent that proposes a new rule for a person to accept.

The two could share code, depend on each other, or only share conventions.

## Decision

- **Neither library imports the other.** Each is released and versioned on its own.
- **Conventions are aligned** so the third system can bridge them with thin adapters:
  - tenant-scoped handles (`open(tenant, id, actor=)`)
  - one log per unit (workspace or stream) with a gap-free `seq`, and envelopes of the same shape
  - the same actor kinds (reflexr adds `source`)
  - a sans-IO core with JSON conformance fixtures
  - a storage protocol with in-memory and SQL implementations, leases with an injectable clock, and one behaviour suite
  - pydantic-ai integration as a capability plus a deps type
  - the same WebSocket handshake, replay and close codes, and tenant-qualified MCP URIs
  - the same quality gates, process and documentation structure
- **The shared context belongs to the third system.** Its adapters turn artifactr events into reflexr events and reflexr actions into artifactr commands.

## Options considered

| Option | Coupling | Duplication | Flexibility for the third system |
|---|---|---|---|
| **Independent, aligned siblings (chosen)** | None | Some: similar storage and protocol code | High: it defines the shared context |
| A shared kernel package both depend on | Three packages released together | Least | Low: the kernel's API freezes before the third system says what it needs |
| reflexr built on artifactr's workspace log | reflexr depends on artifactr | Low | Low: event processing tied to the chat model |

## Trade-off analysis

A shared kernel is attractive on paper, but its API would be designed before the only system that needs it exists. Aligned conventions deliver most of the interoperability, and duplication between two libraries of this size is cheap. A kernel can still be extracted later, from two proven implementations, if the third system shows it pays.

## Consequences

- Easier: each library evolves at its own pace, and neither drags in the other's dependencies.
- Harder: conventions must be kept aligned by review, not by the compiler. The architecture docs of both libraries list them.
- Revisit extracting shared code once the third system exists.

## Action items

1. [ ] Keep the "Aligned with artifactr" table in the architecture doc current.
