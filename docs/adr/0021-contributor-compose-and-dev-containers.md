# ADR-0021: Contributor Compose and dev containers here, infrastructure in stackr

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Working on reflexr needs PostgreSQL and the reference app. Evaluating and operating it also needs the observability and LLM infrastructure: an OpenTelemetry Collector, Grafana with Prometheus, Tempo, Loki and Pyroscope, self-hosted Langfuse, and a LiteLLM proxy. reflexr and evalr need the same infrastructure, and so will applications built on them. The existing dev container installs only the Python environment.

## Decision

- **This repository keeps what developing reflexr needs:** a `compose.yaml` with PostgreSQL by default and the reference app under an `app` profile, and a dev container built on it that sets the test environment.
- **The shared infrastructure lives in stackr** ([ADR-0023](0023-libraries-and-the-stackr-template.md)), in one place for every library and application. When it is running, the dev container joins its network and sends telemetry to its Collector.
- **Dashboards stay here,** next to the metric registry they are tested against, and are published with each release for stackr to provision by version.
- CI validates the Compose file and dashboard JSON without starting containers.

## Options considered

| Option | Copies of the infrastructure | Setup for a contributor |
|---|---|---|
| **Contributor Compose here, infrastructure in stackr (chosen)** | One | One command, plus stackr for observability |
| The full stack in every library's Compose file | Three, drifting | One command |
| Documentation only | None | Manual |

## Consequences

- Easier: the infrastructure is maintained once and used the same way everywhere.
- Harder: seeing traces while developing needs stackr's stack running as well.

## Action items

1. [ ] Implement RFC-0002 phase B4.
