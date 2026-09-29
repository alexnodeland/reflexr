# ADR-0038: Dashboards generated, tested and released

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Alex Nodeland

## Context

[RFC-0002](../rfcs/0002-observability-feedback-and-evaluation.md) ships Grafana dashboards in `deploy/grafana/dashboards/`, tested against the metric registry, and publishes them with each release for stackr to provision by version ([ADR-0021](0021-contributor-compose-and-dev-containers.md)). The registry already names each metric's Prometheus series ([ADR-0029](0029-metric-detail-through-sdk-views.md)). How the dashboards are written, what the test checks, and how they are published were left open. artifactr answered the same questions ([artifactr ADR-0041](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0041-dashboards-generated-tested-and-released.md)), and the siblings' dashboards sit side by side in stackr's Grafana.

Grafana's dashboard JSON is long and repetitive, and hand edits to it drift: a panel's data source, units or template variables end up differing between dashboards. Running the dashboards against stackr's Prometheus and Grafana also showed two things that metric names alone would not:

- The OpenTelemetry SDK pushes metrics once a minute by default, and stackr's data source leaves Grafana's scrape interval at 15 seconds, so `$__rate_interval` is one minute: most windows hold a single sample, and rates come out empty.
- pydantic-ai's metric names are not reflexr's alone: stackr's LiteLLM proxy records `gen_ai.client.token.usage` too. A `job` variable whose "All" is `.*` counts both.

## Decision

- **The dashboards are generated** by `scripts/grafana_dashboards.py` (`make dashboards`), with artifactr's helpers:
  - data source uid `prometheus`, as stackr provisions it
  - `$job` (the service, `service.name`) and `$environment` (`deployment_environment_name`) on every dashboard, then `$tenant` and `$workspace` down to each dashboard's scope. "All" is `.*` for every variable but `$job`, whose "All" is the services reflexr's metrics come from, not every job in Prometheus.
  - rates over `$__rate_interval`, with a one-minute min step on every query, so each rate spans at least four of the SDK's exports
  - exemplars on latency panels, which link to traces, and reflexr's red on single-number panels

  The JSON files are checked in, and a test fails if they differ from the script's output.

- **Eight dashboards:** Overview, Tenant, Workspace, Rules (lag, firings, errors), Runs (outcomes, durations, retries, dead letters, failure reasons, operator actions), Agent and LLM, Schedules, and Stream (WebSocket connections and close codes).
- **The failed share of run attempts** is computed from `reflexr.run.duration`'s count by how each attempt ended, not from attempts started over attempts failed, which happen minutes apart.
- **The registry lists the external metrics the dashboards read**, `EXTERNAL_METRICS`: pydantic-ai's `gen_ai.client.token.usage` and `operation.cost`, with `Metric.scope` naming who records them.
- **The test checks every query:**
  - its series are Prometheus names of registry metrics (reflexr's own, or the external ones), through `Metric.prometheus_series`
  - for reflexr's metrics, every label it matches or groups by is an attribute a deployment keeps at the most detail (`kept_attributes`), or a resource label
  - every legend names a label its query keeps, and every query has the one-minute min step
  - the query parser itself is tested against queries that must fail
- **Release assets:** when a release is published, a workflow attaches each dashboard file, and `reflexr-dashboards-<version>.tar.gz`, the archive stackr's `scripts/fetch-dashboards` downloads from the release tagged `v<version>`. It can also be run by hand for an existing release. It never creates releases or tags.
- CI checks the files are valid JSON without starting Grafana.

## Options considered

| Option | Drift between dashboards | Drift from the registry |
|---|---|---|
| **Generated from a script, tested against the registry (chosen)** | None: one set of helpers | Caught for metrics and labels |
| Hand-written JSON, tested for metric names | Likely | Caught for metric names only |
| Grafana's Foundation SDK, or Grafonnet | None | Needs the same test, and another toolchain |

## Consequences

- Easier: a new panel is a few lines; a renamed metric or attribute fails a test until the dashboards follow.
- Easier: stackr provisions the dashboards of the version it pins, next to artifactr's, in the same shape.
- Harder: dashboards edited in Grafana's UI must be carried back into the script.
- To revisit: a deployment that exports metrics less often than once a minute needs a longer min step; stackr's data source could declare the push interval instead, for every library's dashboards.

## Action items

1. [x] Eight dashboards, the external metrics in the registry, the tests, CI's JSON check and the release workflow.
