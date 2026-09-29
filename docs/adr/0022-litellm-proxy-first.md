# ADR-0022: LiteLLM, proxy first, for routing and guardrails

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Applications need to route agents across models and providers (model groups, fallbacks, load balancing), control spend per tenant, and apply guardrails (PII masking, prompt-injection and content checks) consistently. LiteLLM provides all of this as a proxy with an OpenAI-compatible API, or in process as the litellm package's `Router`. pydantic-ai 2.51 has a native `LiteLLMProvider`, and its model settings carry `extra_body` and `extra_headers` per request, which is how LiteLLM receives metadata, tags and guardrails.

## Decision

- **Proxy first.** The LiteLLM proxy runs in stackr and owns routing, budgets, rate limits and guardrails. reflexr reaches it through pydantic-ai's `LiteLLMProvider` and does not depend on the litellm package, as artifactr does ([artifactr ADR-0031](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0031-litellm-proxy-first.md)).
- **A `[litellm]` extra** provides `litellm_model(...)`, per-request metadata (tenant, workspace, rule, run, chain session, trace id), guardrail policies per rule, and typed handling of guardrail blocks, which fail an attempt permanently rather than retrying it.

### Amendment (2026-09-28): as built

The metadata, key and guardrails are added by their own capability, **`LiteLLMGateway`**, beside `EventContext`, as artifactr's is ([artifactr ADR-0043](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0043-the-litellm-adapter.md)). In `before_model_request` it adds LiteLLM `metadata` (tenant, workspace, rule, scope, run, the chain as `session_id`, the person whose event fired the rule as `trace_user_id`, the run's trace as `existing_trace_id`, and tags), the W3C trace context, the rule's `guardrails` from a `GuardrailPolicy(tenant, workspace, rule)`, and the tenant's key from a `TenantKey(tenant)`. A guardrail's HTTP 400 becomes `GuardrailBlocked`, a permanent `RunFailure` ([ADR-0036](0036-typed-run-failures.md)) with the reason `guardrail_blocked`, so the run is dead-lettered without retrying; it is recognised in `on_model_request_error` and, for streamed requests, in `wrap_run_event_stream`.
- **Each tenant is a LiteLLM team**, with virtual keys, budgets and rate limits. The application supplies the key for a tenant through a callback. Keys never appear in the log or on spans.

## Options considered

| Option | Library dependencies | Where routing lives | Per-tenant budgets |
|---|---|---|---|
| **Proxy first (chosen)** | None beyond pydantic-ai | The proxy's configuration, in stackr | Teams and virtual keys |
| In-process `Router` | The litellm package | Application code | Application code |
| Both, pluggable | Optional litellm | Either | Either |

## Consequences

- Easier: routing, fallbacks and guardrails change in configuration, without code changes, and apply to every library and application the same way.
- Easier: spend and guardrail hits break down by tenant, workspace and rule in LiteLLM, Langfuse and Grafana.
- Harder: a proxy to run; stackr provides it.

## Action items

1. [x] Implement RFC-0002 phase B6 (the `[litellm]` extra), and the proxy in stackr.
