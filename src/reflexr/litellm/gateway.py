"""LiteLLM as the model gateway: a model over the proxy, and what each request carries."""

import dataclasses
import json
import re
from collections.abc import AsyncIterable, Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast, override

import httpx2
from opentelemetry import propagate
from pydantic_ai import AgentStreamEvent, RunContext
from pydantic_ai.capabilities import AbstractCapability
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.messages import ModelResponse
from pydantic_ai.models import ModelRequestContext
from pydantic_ai.models.openai import OpenAIChatModel
from pydantic_ai.providers.litellm import LiteLLMProvider
from pydantic_ai.settings import ModelSettings

from reflexr.core import RuleName, TenantId, UserActor, WorkspaceId
from reflexr.telemetry import current_trace_id
from reflexr.workspace import Reaction, RunFailure

TenantKey = Callable[[TenantId], Awaitable[str | None]]
"""Returns the LiteLLM virtual key of a tenant's team, or None for the model's own key.

A port the application implements (ADR-0025): keys come from its secret store, and reflexr
never logs them or puts them on spans.
"""

GuardrailPolicy = Callable[[TenantId, WorkspaceId, RuleName], Awaitable[Sequence[str]]]
"""Returns the names of the LiteLLM guardrails a rule's requests use in a workspace."""

GUARDRAIL_BLOCKED = "guardrail_blocked"
"""The failure reason of a run attempt whose model request a guardrail blocked."""

_GUARDRAIL_NAME = re.compile(r"guardrail(?:_name)?[\"']?\s*[:=]\s*[\"']([\w.-]+)[\"']", re.I)


class GuardrailBlocked(RunFailure):
    """A model request that one of the gateway's guardrails blocked.

    The run is dead-lettered with the reason ``guardrail_blocked`` and this message, which names
    the guardrail but never repeats what was blocked. It is permanent: retrying the same input
    would be blocked again.

    Args:
        guardrail: The guardrail's name, when the gateway said.
    """

    def __init__(self, guardrail: str | None) -> None:
        which = f"the {guardrail} guardrail" if guardrail else "a guardrail"
        super().__init__(
            f"The model request was blocked by {which}.", reason=GUARDRAIL_BLOCKED, permanent=True
        )
        self.guardrail = guardrail


def guardrail_block(error: ModelHTTPError) -> GuardrailBlocked | None:
    """Return the guardrail block a model request's HTTP error reports, if it is one.

    LiteLLM answers a request a guardrail blocks with HTTP 400 and an error that names the
    guardrail; other errors are not guardrail blocks.
    """
    if error.status_code != 400:
        return None
    text = json.dumps(error.body, default=str)
    if "guardrail" not in text.lower():
        return None
    name = _GUARDRAIL_NAME.search(text)
    return GuardrailBlocked(name.group(1) if name else None)


def litellm_model(
    model: str,
    *,
    api_base: str | None = None,
    api_key: str | None = None,
    http_client: httpx2.AsyncClient | None = None,
) -> OpenAIChatModel:
    """Return a pydantic-ai model that calls a LiteLLM proxy's model group.

    Routing, fallbacks, budgets and guardrails are the proxy's configuration; the model only
    names the group.

    Args:
        model: The proxy's model group, such as ``claude-sonnet``.
        api_base: The proxy's URL, such as ``http://litellm:4000``. Defaults to the provider's
            environment variables.
        api_key: The proxy key used when a request carries no tenant's key.
        http_client: The HTTP client, for tests or a shared connection pool.
    """
    if http_client is None:
        provider = LiteLLMProvider(api_key=api_key, api_base=api_base)
    else:
        provider = LiteLLMProvider(api_key=api_key, api_base=api_base, http_client=http_client)
    return OpenAIChatModel(model, provider=provider)


@dataclass
class LiteLLMGateway(AbstractCapability[Reaction[Any]]):
    """Attach tenancy, the chain, the trace, a tenant's key and guardrails to each request.

    Give it to an agent beside ``EventContext``, with a :func:`litellm_model`. Before each
    model request it adds, through the request's ``extra_body`` and ``extra_headers``:

    - LiteLLM metadata: the tenant, workspace, rule, scope and run, the causal chain as the
      session, the person whose event fired the rule as the user, the trace id (so LiteLLM's
      own traces join the run's), and tags
    - the W3C trace context, as ``traceparent``
    - the guardrails the rule's policy names
    - the tenant's virtual key, as the request's ``Authorization``, from ``tenant_key``

    A request a guardrail blocks fails the attempt with a :class:`GuardrailBlocked`: the run is
    dead-lettered as ``guardrail_blocked``, never retried.

    Args:
        tenant_key: Returns a tenant's LiteLLM key; the model's own key is used without one.
        guardrails: Returns the guardrails a rule's requests use in a workspace.
        tags: More tags for every request, such as the application's name.
    """

    tenant_key: TenantKey | None = None
    guardrails: GuardrailPolicy | None = None
    tags: Sequence[str] = ()

    @override
    async def before_model_request(
        self, ctx: RunContext[Reaction[Any]], request_context: ModelRequestContext
    ) -> ModelRequestContext:
        """Add the gateway's metadata, guardrails and key to the request."""
        body, headers = await self.request_options(ctx.deps)
        settings: dict[str, Any] = dict(request_context.model_settings or {})
        settings["extra_body"] = {**_mapping(settings.get("extra_body")), **body}
        settings["extra_headers"] = {**_mapping(settings.get("extra_headers")), **headers}
        return dataclasses.replace(request_context, model_settings=cast(ModelSettings, settings))

    async def request_options(
        self, reaction: Reaction[Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Return the extra body and headers of a model request in a run attempt."""
        workspace = reaction.workspace
        tenant_id, workspace_id = workspace.tenant_id, workspace.workspace_id
        run = reaction.run
        metadata: dict[str, Any] = {
            "tenant_id": tenant_id,
            "workspace_id": workspace_id,
            "rule": run.rule,
            "scope": run.scope_key,
            "run_id": run.id,
            "session_id": run.correlation_id,
            "tags": [
                "reflexr",
                f"tenant:{tenant_id}",
                f"workspace:{workspace_id}",
                f"rule:{run.rule}",
                *self.tags,
            ],
        }
        cause = reaction.events[-1].actor if reaction.events else None
        if isinstance(cause, UserActor):
            metadata["trace_user_id"] = cause.id
        if (trace_id := current_trace_id()) is not None:
            metadata["existing_trace_id"] = trace_id
        body: dict[str, Any] = {"metadata": metadata}
        names = await self.guardrails(tenant_id, workspace_id, run.rule) if self.guardrails else ()
        if names:
            body["guardrails"] = list(names)
        headers: dict[str, str] = {}
        propagate.inject(headers)
        if self.tenant_key is not None and (key := await self.tenant_key(tenant_id)):
            headers["Authorization"] = f"Bearer {key}"
        return body, headers

    @override
    async def on_model_request_error(
        self,
        ctx: RunContext[Reaction[Any]],
        *,
        request_context: ModelRequestContext,
        error: Exception,
    ) -> ModelResponse:
        """Turn a guardrail's block into a typed run failure; let other errors through."""
        if isinstance(error, ModelHTTPError) and (blocked := guardrail_block(error)) is not None:
            raise blocked from error
        raise error

    @override
    async def wrap_run_event_stream(
        self, ctx: RunContext[Reaction[Any]], *, stream: AsyncIterable[AgentStreamEvent]
    ) -> AsyncIterable[AgentStreamEvent]:
        """Turn a guardrail's block into a typed run failure in a streamed run too.

        A streamed request is sent as its events are first read, so its HTTP error surfaces
        here rather than in :meth:`on_model_request_error`.
        """
        try:
            async for event in super().wrap_run_event_stream(ctx, stream=stream):
                yield event
        except ModelHTTPError as error:
            if (blocked := guardrail_block(error)) is None:
                raise
            raise blocked from error


def _mapping(value: object) -> dict[str, Any]:
    return dict(cast(Mapping[str, Any], value)) if isinstance(value, Mapping) else {}
