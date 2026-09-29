# ADR-0029: Metric detail through SDK views, and the OpenTelemetry and Langfuse adapters

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland
**Amends:** [ADR-0018](0018-opentelemetry-observability-with-langfuse.md)

## Context

reflexr applied `metrics_detail` (keep tenant and workspace, the tenant only, or neither) when it recorded a metric, through `Workspaces(metrics_detail=...)`. artifactr decided to apply the same policy in the SDK instead, with views ([artifactr ADR-0036](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0036-metric-cardinality-through-sdk-views.md)), so a deployment chooses its detail where it configures telemetry, and the libraries record the same attributes everywhere. The siblings should share one policy and one place to set it.

## Decision

- **reflexr always records the attributes the registry declares**, tenant and workspace included, and nothing else; recording an undeclared attribute raises. `Workspaces` no longer takes `metrics_detail`.
- **Detail is applied in the SDK.** `reflexr.telemetry.kept_attributes(metric, detail)` states the policy, `reflexr.otel.metric_views(detail)` turns it into a view per metric, and `configure_telemetry(metrics_detail=...)` installs them.
- **The registry matches artifactr's:** each `Metric` carries its tenancy attributes, advises histogram buckets (quick evaluations; run attempts of seconds to minutes), and names its Prometheus series, which the dashboards will be tested against.
- **`reflexr.otel`** (the `[otel]` extra) is artifactr's `configure_telemetry`, ported: providers with the service's resource, OTLP/HTTP export, the views, a baggage processor that copies the session (the causal chain, which each run attempt places in baggage) onto every span, the FastAPI, SQLAlchemy, asyncpg and httpx instrumentations, pydantic-ai's instrumentation settings, and optionally Langfuse on the same tracer provider.
- **`reflexr.langfuse`** (the `[langfuse]` extra) is artifactr's adapter, ported: the span filter that keeps whole traces, the score sink and score-config store behind `reflexr.scores`' ports, and `langfuse_run`, a `RunContext` that files each run attempt under its chain's session, its rule's name, the person whose event made it fire, and tags for the tenant, workspace and rule.

## Consequences

- Easier: one policy across artifactr and reflexr, set once in `configure_telemetry`.
- Easier: dashboards can be tested against each metric's Prometheus series.
- Harder: an application that configures the SDK itself must pass `metric_views(detail)` to keep less detail than the default.

## Action items

1. [x] `kept_attributes`, `metric_views`, `configure_telemetry`, and the Langfuse adapter.
2. [x] Grafana dashboards tested against the registry (RFC-0002 B4, [ADR-0038](0038-dashboards-generated-tested-and-released.md)).
