# API reference

The reference is generated from the library's docstrings and type annotations. It documents each package's public API: the names in its `__all__`. Anything else is internal and may change without notice.

## Packages

The packages are layers. Each depends only on the ones below it, so each is usable without the ones above ([Architecture](../architecture.md#layers)).

| Package | What it holds | Install |
|---|---|---|
| [`reflexr.core`](core.md) | Every decision, as pure functions over immutable values: events and envelopes, actors, conditions, rules, evaluation, the run lifecycle, feedback, rejections and the protocol's frames | core |
| [`reflexr.telemetry`](telemetry.md) | Tracing and metrics through the OpenTelemetry API: attribute names, the metric registry and its cardinality policy | core |
| [`reflexr.workspace`](workspace.md) | Tenant-scoped workspace handles, the `Reactor`, the action port and `Reaction`, schedules, the storage protocol and in-memory storage | core |
| [`reflexr.agent`](agent.md) | pydantic-ai agents and pydantic-graph graphs as actions, with the `EventContext` capability and graph checkpoints | core |
| [`reflexr.scores`](scores.md) | Feedback as scores: the mirror that follows the log, on evalr's mapping and ports | `langfuse` or `evals` extra |
| [`reflexr.sql`](sql.md) | SQL storage on PostgreSQL and SQLite, and its migrations | `sql`, `postgres` or `sqlite` extra |
| [`reflexr.fastapi`](fastapi.md) | HTTP ingest, REST reads and administration, and the WebSocket stream, as a FastAPI router | `fastapi` extra |
| [`reflexr.mcp`](mcp.md) | An MCP server for external agents | `mcp` extra |
| [`reflexr.otel`](otel.md) | `configure_telemetry`: the OpenTelemetry SDK, OTLP export, instrumentations and metric views | `otel` extra |
| [`reflexr.langfuse`](langfuse.md) | Langfuse: whole traces, each run's session and trace attributes, and feedback as scores | `langfuse` extra |
| [`reflexr.litellm`](litellm.md) | The LLM gateway: models over a LiteLLM proxy, with tenancy, keys and guardrails on every request | `litellm` extra |
| [`reflexr.evals`](evals.md) | evalr for reflexr: feedback as examples, evaluators as rules, replay experiments and end-to-end measures | `evals` extra |

The wire formats have their own pages: the [stream protocol](../protocol.md), and the [JSON Schemas](schema.md) of rules and of the protocol's frames.

## The top-level package

`reflexr` re-exports the names most applications need to define events and rules, so `from reflexr import Event, Rule, on, by, run, F` works. Each is documented in its layer:

| Name | Documented in |
|---|---|
| `Event`, `Envelope` | [`reflexr.core`: Events](core.md#events) |
| `Rule`, `RetryPolicy`, `by`, `run` | [`reflexr.core`: Rules](core.md#rules) |
| `on`, `sequence`, `F`, `field` | [`reflexr.core`: Conditions](core.md#conditions) |
| `Actor`, `UserActor`, `AgentActor`, `ExternalAgentActor`, `SystemActor`, `SourceActor`, `EvaluatorActor` | [`reflexr.core`: Actors](core.md#actors) |
| `Feedback` | [`reflexr.core`: Feedback](core.md#feedback) |
| `Rejection`, `NotFound`, `InvalidRule` | [`reflexr.core`: Rejections](core.md#rejections) |
| `new_id` | [`reflexr.core`: Identifiers](core.md#identifiers) |

The runtime lives in its own layers: `Workspaces`, `Reactor`, `Reaction`, `Schedule` and `InMemoryStorage` in [`reflexr.workspace`](workspace.md), and `AgentAction`, `EventContext` and `GraphAction` in [`reflexr.agent`](agent.md).

`reflexr.__version__` is the installed version.
