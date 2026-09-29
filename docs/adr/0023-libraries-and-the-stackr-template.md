# ADR-0023: Libraries, and stackr as the infrastructure template

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

This records, for reflexr, the decision made in [artifactr ADR-0032](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0032-libraries-and-the-stackr-template.md). artifactr, reflexr and evalr are growing an ecosystem around them. It needs LLM gateway configuration (LiteLLM routing, budgets and guardrails), observability (an OpenTelemetry Collector, Grafana with Prometheus, Tempo, Loki and Pyroscope, Langfuse) and application scaffolding. It also needs a database platform with authentication; Supabase provides one that runs locally with its CLI. None of that is library code, and all of it is shared by the libraries, by applications built on them, and by the system that will combine them.

## Decision

- **The libraries stay libraries:** typed code with optional extras, released to PyPI. Each ships what is specific to it and must stay in step with its code: its dashboards (tested against its metric registry), a contributor Compose file and a dev container.
- **stackr is the infrastructure template,** in its own repository:
  - the LiteLLM proxy with routing, budget and guardrail configuration
  - the OpenTelemetry Collector
  - Grafana's LGTM stack with Pyroscope
  - self-hosted Langfuse
  - **local Supabase**, through the Supabase CLI: PostgreSQL for the libraries' SQL storage and for Langfuse and LiteLLM, Auth (whose JWTs carry the tenant that `resolve_actor` reads), Storage, Realtime and Studio
  - Redis
  - the libraries' dashboards, provisioned by version

  It starts with Docker Compose. Kubernetes (Helm) and Terraform follow when needed.
- **stackr also holds the application template:** a Copier template that scaffolds an application using artifactr, reflexr or both, wired to the stack, with telemetry, the LiteLLM gateway and evals already set up.
- **The combined system** is built from that template, as a product.

## Options considered

| Option | Duplication | Reusable by other applications |
|---|---|---|
| **Libraries plus one stack template (chosen)** | None | Yes |
| Infrastructure folded into each library | Three copies | Partly |
| Infrastructure only in the combined system | None | No |

## Consequences

- Easier: a new application starts from the template and runs on the same, tested infrastructure.
- Easier: the libraries' releases stay small and focused.
- Harder: stackr must track the libraries' versions and dashboards; it pins them, and its CI starts the stack to check them together.

## Action items

1. [ ] Create stackr with its RFC-0001, then build it.
