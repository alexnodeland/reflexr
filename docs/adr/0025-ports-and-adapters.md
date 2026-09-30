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

### Amendment (2026-09-29): evalr owns the score mapping and ports

reflexr's `reflexr.scores` carried the feedback-to-score mapping and two ports, `ScoreSink` and `ScoreConfigStore`, identical to artifactr's, beside evalr's own score type, sink and mapping for evaluators' verdicts. Three copies could drift and split one score into two in Langfuse. evalr now owns them (its ADR-0011, from its issue #17), as this record's rule across the repositories already said of score sinks.

- **evalr owns the mapping and the ports.** `reflexr.scores` calls evalr's `score_configs` and `score_values` with the feedback type's registered name as the `{type}`, and its `ScoreSink` and `ScoreConfigStore` are evalr's. evalr's mapping reproduces this one exactly for every way a feedback type declares a field, and a fixture in evalr pins it, so no score config in Langfuse changes.
- **reflexr keeps the mirror and the adapters.** `FeedbackMirror`, which knows which trace or session a run's, a firing's or a chain's feedback belongs on, stays here, with its own score id namespace and ids, so mirroring again replaces the scores already in Langfuse. So do `LangfuseScores` and `LangfuseScoreConfigs`, which now pass evalr's `check_score_sink` and `check_score_config_store`. `sync_score_configs(store, types=None)` keeps its signature and syncs through evalr's.
- **What changed for a score:**
  - A score is evalr's `Score`. Where the feedback came from is its `source`, not its `metadata`; its `metadata` still returns the same keys, and Langfuse receives the same metadata.
  - A sink has one method, `record(scores)`, in place of `send(score)`. The mirror records an envelope's scores at once.
  - A yes or no is a bool, which `LangfuseScores` sends as 1 or 0, as before.
- **The public names stay, re-exported.** `reflexr.scores` promised `Score`, `ScoreConfig`, `ScoreSink`, `ScoreConfigStore`, `ScoreDataType` and `MAX_TEXT` in its `__all__` and its reference, so it re-exports evalr's (`ScoreDataType` is evalr's `ScoreType`). `score_configs` and `score_values` stay reflexr's own, since they name scores by the registered name. A `ScoreConfig`'s `feedback_type` is now `type_name`.
- **`reflexr.scores` needs evalr.** The `[langfuse]` extra now depends on evalr, as `[evals]` does, pinned by git revision until evalr is published ([ADR-0020](0020-evalr-shared-eval-kit.md)). The core, telemetry and workspace never import evalr; `tests/test_layering.py` lets `reflexr.scores` and `reflexr.langfuse` import it, beside `reflexr.evals`. oncall uses the `[langfuse]` extra, so its image installs git for uv to fetch evalr, as docplan's does.

| Port | Kind | Owned by | Adapters |
|---|---|---|---|
| `ScoreSink` and `ScoreConfigStore` | driven | evalr | `LangfuseScores` and `LangfuseScoreConfigs` (`[langfuse]`); evalr's in-memory adapters |

artifactr made the same change to `artifactr.scores`, in its ADR-0038.

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
2. [x] The action port with function, agent and graph adapters (phases 2c and 3).
3. [x] `SqlStorage` passing the same suite (phase 4, [ADR-0030](0030-sql-storage.md)).
4. [x] The `[otel]`, `[langfuse]`, `[litellm]` and `[evals]` adapters (RFC-0002).
