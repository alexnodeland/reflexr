# ADR-0008: Actions: agents, graphs and functions over one Reaction

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Reflex wrapped pydantic-ai in its own class hierarchy (`Agent`, `BaseAgent`, `SimpleAgent`). `BaseAgent` no longer worked with pydantic-ai 1.x, and every example that called a model used dependencies that did not exist. artifactr integrates with pydantic-ai as a capability on a plain `Agent`, which composes with everything else pydantic-ai offers. Workflows with several steps should be first-class too.

## Decision

- An **action** is anything the reactor can run for a firing, behind one protocol in `reflexr.stream`. Three kinds ship:
  - **Functions**: `async def action(reaction: Reaction[AppDeps]) -> None`.
  - **Agents** (`reflexr.agent.AgentAction`): a plain pydantic-ai `Agent` with `deps_type=Reaction[...]`, a prompt built from the firing, and an optional event to emit from its output.
  - **Graphs** (`reflexr.agent.GraphAction`): a pydantic-graph graph built with `GraphBuilder`, with typed state and inputs built from the firing, checkpointed after every step ([ADR-0009](0009-graph-checkpoints.md)).
- **`Reaction[AppDepsT]`** is the one dependencies type for every kind: the stream bound to the run's actor, the firing and its matched envelopes, the run id, attempt and idempotency key, `emit()` for follow-up events, and the application's deps.
- **`EventContext`** is a pydantic-ai capability. It renders the firing into the agent's instructions and adds tools to read back through the stream and to emit events of allowlisted types.
- reflexr has **no agent class hierarchy** of its own.

## Options considered

| Option | Composes with pydantic-ai | Multi-step workflows | Surface to maintain |
|---|---|---|---|
| **Plain agents with a capability, plus graphs and functions (chosen)** | Fully | Graphs, checkpointed | Small adapters |
| Keep reflexr's own agent classes | Partly: they wrap, and drift | Hand-written | Large |
| Functions only | Users wire agents themselves | Hand-written | Smallest |

## Consequences

- Easier: every pydantic-ai feature (models, toolsets, output types, usage limits, instrumentation) works unchanged in an action.
- Easier: one `Reaction` type to learn across all action kinds.
- Harder: graph state and step inputs must be serializable to be checkpointed; `GraphAction` validates this when it is registered where it can.

## Action items

1. [ ] Implement `Reaction`, function actions (RFC-0001 phase 2), and `AgentAction`, `EventContext` and `GraphAction` (phase 3).
