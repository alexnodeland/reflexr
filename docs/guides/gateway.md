# The LLM gateway

Applications route their agents through a [LiteLLM](https://docs.litellm.ai/) proxy, which owns model routing (groups, fallbacks, load balancing), budgets, rate limits and guardrails ([ADR-0022](../adr/0022-litellm-proxy-first.md)). stackr runs the proxy. The `litellm` extra connects reflexr to it through pydantic-ai's LiteLLM provider, with no dependency on the litellm package. This page covers the model, what each request carries, tenants' keys, and guardrails.

## A model over the proxy

Install `reflexr[litellm]`, then give an agent a model from `litellm_model` and the `LiteLLMGateway` capability beside `EventContext`:

```python
from pydantic_ai import Agent

from reflexr.agent import AgentAction, EventContext
from reflexr.litellm import LiteLLMGateway, litellm_model
from reflexr.workspace import Reaction

triage_agent = Agent(
    litellm_model("claude-sonnet", api_base="http://litellm:4000", api_key=proxy_key),
    deps_type=Reaction[None],
    capabilities=[
        EventContext(emit=[IncidentOpened]),
        LiteLLMGateway(tenant_key=tenant_key, guardrails=guardrails, tags=["oncall"]),
    ],
)
triage = AgentAction(triage_agent, name="triage")
```

`litellm_model` names one of the proxy's model groups; which provider and model serve it, and what happens when one fails, is the proxy's configuration. `api_key` is the key used for requests that carry no tenant's key. Without `api_base` and `api_key`, the provider reads its environment variables.

Its `settings` are the model's default settings, as every pydantic-ai model takes them: a `temperature`, a `max_tokens`, or an `extra_body` the proxy reads. A smoke test can have the proxy answer without calling a provider, with LiteLLM's `mock_response`:

```python
from pydantic_ai.settings import ModelSettings

model = litellm_model(
    "claude-sonnet",
    api_base="http://litellm:4000",
    settings=ModelSettings(temperature=0, extra_body={"mock_response": "Done."}),
)
```

The agent's and each run's `model_settings` override these key by key, so an `extra_body` there replaces the model's. The gateway adds its metadata to whichever `extra_body` a request ends up with.

## What each request carries

`LiteLLMGateway` adds to every model request of a run attempt:

| Where | What | Used by LiteLLM for |
|---|---|---|
| `metadata.tenant_id`, `workspace_id`, `rule`, `scope`, `run_id` | reflexr's ids | Spend and logs by tenant, workspace and rule |
| `metadata.tags` | `reflexr`, `tenant:<id>`, `workspace:<id>`, `rule:<name>`, and yours | Tag-based spend tracking and routing |
| `metadata.session_id` | The run's causal chain | Grouping every request one incident caused into one session |
| `metadata.trace_user_id` | The person whose event made the rule fire, when a person published it | Users in its Langfuse logging |
| `metadata.existing_trace_id`, and the `traceparent` header | The run attempt's trace, when telemetry is configured | Joining the attempt's trace in Langfuse and OpenTelemetry |
| `guardrails` | The rule's guardrails in this workspace | Which guardrails check the request |
| `Authorization` | The tenant's key | Budgets, rate limits and allowed models per tenant |

The trace fields are there only when a tracer provider records spans ([Observability](observability.md)); without one there is no trace to join.

## Tenants and keys

Each tenant is a LiteLLM team with its own virtual keys, budgets and rate limits. The gateway asks your application for a tenant's key on each request, through `tenant_key`, a port your application implements. Keep keys in your secret store:

```python
async def tenant_key(tenant_id: str) -> str | None:
    return await secrets.get(f"litellm/{tenant_id}")  # None: use the model's own key
```

reflexr never records keys: not in the log, not on spans. Keep HTTP header capture off in your OpenTelemetry instrumentation, which is its default.

## Guardrails

A rule's policy names the guardrails its requests use, as the proxy configures them. The policy is called with the tenant, the workspace and the rule, so an application can guard some workflows more than others:

```python
async def guardrails(tenant_id: str, workspace_id: str, rule: str) -> list[str]:
    if rule == "ops:error-spike" and await is_regulated(tenant_id):
        return ["presidio-pii", "prompt-injection"]
    return []
```

When a guardrail blocks a request, the proxy answers HTTP 400. The gateway turns that into `GuardrailBlocked`, a permanent `RunFailure` ([ADR-0036](../adr/0036-typed-run-failures.md)), so the run is dead-lettered at once with the reason `guardrail_blocked`, whatever the rule's retry policy allows: retrying the same input would be blocked again. The message names the guardrail but never repeats what was blocked:

```json
{"type": "reflexr:run_dead_lettered", "run_id": "fir_823258f37c0bddf1", "rule": "ops:error-spike",
 "attempts": 1, "error": "The model request was blocked by the presidio-pii guardrail.",
 "reason": "guardrail_blocked"}
```

The `reflexr.runs` metric counts it with `reflexr.run.reason="guardrail_blocked"`, so a dashboard shows guardrail blocks beside other failures. A person can still `retry_run` it after changing the policy or the input ([Workspaces and the log](workspaces.md#operating-runs-and-rules)).

Your own actions and tools can fail a run the same way. Raise `RunFailure` with a stable reason code, and `permanent=True` when retrying cannot help ([The reactor](reactor.md#retries-and-dead-letters)).

## Testing

The gateway works with any pydantic-ai model, so the patterns in [Testing your application](testing.md) apply. To test against the proxy's wire format without a proxy, give `litellm_model` an `httpx2.AsyncClient` over an `httpx2.MockTransport` that answers chat completions, and read the requests it received, as reflexr's own tests do in [`tests/litellm/test_gateway.py`](https://github.com/alexnodeland/reflexr/blob/main/tests/litellm/test_gateway.py):

```python
import httpx2

client = httpx2.AsyncClient(transport=httpx2.MockTransport(proxy.handle))
model = litellm_model("claude-sonnet", api_base="http://litellm.test", http_client=client)
```

A handler that answers HTTP 400 with a body naming a guardrail reproduces a block.
