# ADR-0025: Ports and adapters

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

reflexr connects to many things that change independently: storage engines, model providers, the LiteLLM proxy, tracing backends, Langfuse, evalr's judges, and the surfaces clients use. Its siblings do too: artifactr, evalr, and stackr's infrastructure. Each repository should stay usable without the others, and each integration should be replaceable without touching the core. The sans-IO core ([ADR-0001](0001-library-with-a-sans-io-core.md)) and the storage protocol already follow the ports-and-adapters (hexagonal) pattern; this record makes it the rule for everything that follows, here and across the repositories.

## Decision

- **The core is the hexagon.** `reflexr.core` decides; it performs no I/O and knows no integration.
- **A port is a small `typing.Protocol`** (or a standard API) owned by the layer that needs it, named for what that layer needs, not for a vendor.
- **An adapter implements a port,** in its own module or extra package, and depends inward only. `tests/test_layering.py` enforces which packages each layer may import.
- **Every port has an in-memory or fake adapter,** and one contract suite that every adapter must pass.

| Port | Kind | Owned by | Adapters |
|---|---|---|---|
| `Storage` and `Transaction` | driven | `reflexr.workspace` | `InMemoryStorage`; `SqlStorage` (`reflexr.sql`, on PostgreSQL, Supabase's Postgres or SQLite) |
| `Clock` | driven | `reflexr.workspace` | `utc_now`; fake clocks in tests |
| Telemetry: the OpenTelemetry API | driven | `reflexr.telemetry` | Any SDK and exporter; the `[otel]` extra configures OTLP to stackr's Collector |
| Actions (an `ActionRef` resolved to something runnable) | driven | `reflexr.workspace` | Function actions; agent and graph actions (`reflexr.agent`); evaluator actions (`[evals]`) |
| Models: pydantic-ai's `Model` | driven | `reflexr.agent` | Any provider; the LiteLLM proxy (`[litellm]`) |
| The log, read by subscribers | driven | `reflexr.workspace` | The Langfuse feedback mirror (`[langfuse]`); evalr's `FeedbackSource` (`[evals]`) |
| `Workspace` handles and the `Reactor` | driving | `reflexr.workspace` | In-process calls; REST and WebSocket (`reflexr.fastapi`); MCP (`reflexr.mcp`); schedules |

**Across the repositories** the same rule holds:

- **evalr** owns the evaluation ports: evaluators and judges (DSPy and Jev as adapters of one port), dataset stores, score sinks, and a `FeedbackSource`. It never imports reflexr or artifactr.
- **reflexr and artifactr** implement evalr's `FeedbackSource` in their `[evals]` extras.
- **stackr** provides infrastructure behind protocol-level ports: OTLP, an OpenAI-compatible API through LiteLLM, a Postgres DSN, and S3-compatible storage. Applications depend on those contracts, not on the backends behind them.
- The combined system composes adapters from each; no library depends on it.

## Options considered

### Option A: Ports and adapters, enforced by layering tests (chosen)

| Dimension | Assessment |
|---|---|
| Complexity | Low: protocols and packages, no framework |
| Replaceability | High: an integration is one adapter |
| Testability | High: fakes and contract suites |

**Pros:** swapping storage, models or backends touches one package; tests run without infrastructure; each repository stands alone.
**Cons:** a protocol per integration, and the discipline to keep ports small.

### Option B: Integrations called directly where needed

**Pros:** fewer indirections.
**Cons:** vendors leak into the core; tests need services; one library's change ripples through the others.

### Option C: A dependency-injection framework

**Pros:** wiring is declarative.
**Cons:** a heavy dependency for what constructor arguments already do.

## Trade-off analysis

Constructor arguments and protocols give the benefits of Option C without the dependency, and the layering test keeps Option B's shortcuts from creeping back in.

## Consequences

- Easier: an application can run reflexr on SQLite with a fake model in tests, and on Supabase's Postgres with the LiteLLM proxy and Langfuse in production, with the same code.
- Easier: the storage behaviour suite is the storage port's contract; `SqlStorage` must pass it unchanged.
- Harder: new integrations need a port first, which makes their design explicit.

## Action items

1. [x] The storage port, `InMemoryStorage`, and the behaviour suite as its contract (phase 2b).
2. [ ] The action port with function, agent and graph adapters (phases 2c and 3).
3. [ ] `SqlStorage` passing the same suite (phase 4).
4. [ ] The `[otel]`, `[langfuse]`, `[litellm]` and `[evals]` adapters (RFC-0002).
