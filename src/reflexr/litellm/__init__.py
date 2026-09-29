"""LiteLLM as reflexr's model gateway: the ``[litellm]`` extra (ADR-0022).

An adapter (ADR-0025). The model is pydantic-ai's port: :func:`litellm_model` is a model that
calls a LiteLLM proxy through pydantic-ai's ``LiteLLMProvider``, and :class:`LiteLLMGateway`
is a capability that attaches the tenant, the causal chain, the trace, a tenant's key and the
rule's guardrails to every request::

    agent = Agent(
        litellm_model("claude-sonnet", api_base="http://litellm:4000"),
        deps_type=Reaction[AppDeps],
        capabilities=[
            EventContext(emit=[IncidentOpened]),
            LiteLLMGateway(tenant_key=keys.for_tenant, guardrails=policy.for_rule),
        ],
    )

Routing, fallbacks, budgets, rate limits and guardrails stay in the proxy's configuration.
"""

from reflexr.litellm.gateway import (
    GUARDRAIL_BLOCKED,
    GuardrailBlocked,
    GuardrailPolicy,
    LiteLLMGateway,
    TenantKey,
    guardrail_block,
    litellm_model,
)

__all__ = [
    "GUARDRAIL_BLOCKED",
    "GuardrailBlocked",
    "GuardrailPolicy",
    "LiteLLMGateway",
    "TenantKey",
    "guardrail_block",
    "litellm_model",
]
