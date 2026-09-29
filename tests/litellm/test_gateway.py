"""The LiteLLM gateway: what each request carries, and guardrail blocks as permanent failures.

A fake proxy answers through an httpx2 mock transport, so no test calls the network.
"""

import json
from collections.abc import AsyncIterator, Sequence
from typing import Any

import httpx2
import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic_ai import Agent, ModelMessage, ModelResponse, TextPart
from pydantic_ai.capabilities import Instrumentation
from pydantic_ai.exceptions import ModelHTTPError
from pydantic_ai.models.function import AgentInfo, FunctionModel
from pydantic_ai.models.instrumented import InstrumentationSettings
from pydantic_ai.settings import ModelSettings

from reflexr import ExternalAgentActor, Rule, UserActor, on, run
from reflexr.agent import AgentAction, EventContext
from reflexr.core import RuleName, RunDeadLettered, TenantId, WorkspaceId
from reflexr.litellm import (
    GUARDRAIL_BLOCKED,
    GuardrailBlocked,
    LiteLLMGateway,
    guardrail_block,
    litellm_model,
)
from reflexr.workspace import InMemoryStorage, Reaction, Reactor, Workspace, Workspaces
from tests.event_types import Deploy

BLOCKED = {
    "error": {
        "message": "{'error': 'Violated guardrail policy', 'guardrail_name': 'presidio-pii'}",
        "type": "None",
        "param": "None",
        "code": "400",
    }
}
rule = Rule(name="review", when=on(Deploy), then=run("review"))


class FakeProxy:
    """A LiteLLM proxy's chat completions endpoint: records requests, answers as told."""

    def __init__(self, status: int = 200, error: dict[str, Any] | None = None) -> None:
        self.requests: list[tuple[dict[str, Any], httpx2.Headers]] = []
        self.status = status
        self.error = error

    def handle(self, request: httpx2.Request) -> httpx2.Response:
        body = json.loads(request.content)
        self.requests.append((body, request.headers))
        if self.error is not None:
            return httpx2.Response(self.status, json=self.error)
        assert body["stream"], "pydantic-ai streams every run"
        chunk = {"id": "chatcmpl-1", "object": "chat.completion.chunk", "created": 0}
        chunk["model"] = "claude-sonnet"
        text = {"index": 0, "delta": {"role": "assistant", "content": "Looks fine."}}
        stop = {"index": 0, "delta": {}, "finish_reason": "stop"}
        usage = {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
        events = [chunk | {"choices": [text]}, chunk | {"choices": [stop], "usage": usage}]
        stream = "".join(f"data: {json.dumps(event)}\n\n" for event in events) + "data: [DONE]\n\n"
        return httpx2.Response(200, text=stream, headers={"content-type": "text/event-stream"})

    def model(self, settings: ModelSettings | None = None) -> Any:
        client = httpx2.AsyncClient(transport=httpx2.MockTransport(self.handle))
        return litellm_model(
            "claude-sonnet",
            api_base="http://litellm.test",
            api_key="sk-proxy",
            http_client=client,
            settings=settings,
        )


async def acme_key(tenant_id: TenantId) -> str | None:
    return {"acme": "sk-acme-secret"}.get(tenant_id)


async def pii_for_review(
    tenant_id: TenantId, workspace_id: WorkspaceId, rule: RuleName
) -> Sequence[str]:
    return ["presidio-pii"] if rule == "review" else []


def review(model: Any, gateway: LiteLLMGateway, *extra: Any) -> AgentAction[None, str]:
    agent: Agent[Reaction[None], str] = Agent(
        model,
        deps_type=Reaction[None],
        capabilities=[EventContext(), gateway, *extra],
        name="review",
    )
    return AgentAction(agent)


async def settle(
    action: AgentAction[None, str], actor: Any, provider: TracerProvider | None = None
) -> Workspace:
    workspaces = Workspaces(InMemoryStorage(), rules=[rule], tracer_provider=provider)
    ws = await workspaces.open("acme", "prod", actor=actor)
    await ws.publish(Deploy(service="auth"))
    await Reactor(workspaces, actions={"review": action}).settle()
    return ws


async def test_each_request_carries_the_tenant_chain_trace_key_and_guardrails() -> None:
    proxy = FakeProxy()
    spans = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(spans))
    gateway = LiteLLMGateway(tenant_key=acme_key, guardrails=pii_for_review, tags=("oncall",))
    traced = Instrumentation(settings=InstrumentationSettings(tracer_provider=provider))
    ws = await settle(review(proxy.model(), gateway, traced), UserActor(id="ada"), provider)
    [done] = await ws.runs()
    assert (done.status, done.output) == ("succeeded", "Looks fine.")
    [(body, headers)] = proxy.requests
    assert body["metadata"] == {
        "tenant_id": "acme",
        "workspace_id": "prod",
        "rule": "review",
        "scope": "[]",
        "run_id": done.id,
        "session_id": done.correlation_id,
        "tags": ["reflexr", "tenant:acme", "workspace:prod", "rule:review", "oncall"],
        "trace_user_id": "ada",
        "existing_trace_id": done.trace_ids[-1],
    }
    assert body["guardrails"] == ["presidio-pii"]
    assert headers["authorization"] == "Bearer sk-acme-secret", "the tenant's own key"
    assert done.trace_ids[-1] in headers["traceparent"]
    recorded = json.dumps(
        [dict(s.attributes or {}) for s in spans.get_finished_spans()], default=str
    )
    log = json.dumps([e.model_dump(mode="json") for e in await ws.read()])
    assert "sk-acme-secret" not in recorded, "keys never reach spans"
    assert "sk-acme-secret" not in log, "or the log"
    provider.shutdown()


async def test_the_models_settings_reach_the_proxy_beside_the_gateways() -> None:
    proxy = FakeProxy()
    settings = ModelSettings(temperature=0.0, extra_body={"mock_response": "Looks fine."})
    gateway = LiteLLMGateway(tenant_key=acme_key)
    ws = await settle(review(proxy.model(settings), gateway), UserActor(id="ada"))
    [done] = await ws.runs()
    assert done.status == "succeeded"
    [(body, _)] = proxy.requests
    assert body["temperature"] == 0.0
    assert body["mock_response"] == "Looks fine.", "a reply the proxy mocks, for smoke tests"
    assert body["metadata"]["tenant_id"] == "acme", "beside the gateway's metadata"


async def test_without_a_person_tenant_key_guardrails_or_trace() -> None:
    proxy = FakeProxy()
    other = Rule(name="other", when=on(Deploy), then=run("review"))
    workspaces = Workspaces(InMemoryStorage(), rules=[other])
    ws = await workspaces.open("globex", "prod", actor=ExternalAgentActor(client_id="claude"))
    await ws.publish(Deploy(service="auth"))
    gateway = LiteLLMGateway(tenant_key=acme_key, guardrails=pii_for_review)
    await Reactor(workspaces, actions={"review": review(proxy.model(), gateway)}).settle()
    [(body, headers)] = proxy.requests
    assert "trace_user_id" not in body["metadata"], "only people are users"
    assert headers["authorization"] == "Bearer sk-proxy", "the proxy key"
    assert "guardrails" not in body
    assert "existing_trace_id" not in body["metadata"]
    assert "traceparent" not in headers


async def test_a_guardrail_block_dead_letters_the_run_without_retrying() -> None:
    proxy = FakeProxy(400, BLOCKED)
    ws = await settle(review(proxy.model(), LiteLLMGateway()), UserActor(id="ada"))
    [dead] = await ws.runs()
    assert (dead.status, dead.attempts, dead.reason) == ("dead", 1, GUARDRAIL_BLOCKED)
    assert dead.error == "The model request was blocked by the presidio-pii guardrail."
    assert len(proxy.requests) == 1, "a block is not retried"
    [fact] = [e.event for e in await ws.read() if isinstance(e.event, RunDeadLettered)]
    assert fact.reason == GUARDRAIL_BLOCKED


async def test_other_errors_are_retried_without_a_reason() -> None:
    proxy = FakeProxy(400, {"error": {"message": "context window exceeded", "code": "400"}})
    ws = await settle(review(proxy.model(), LiteLLMGateway()), UserActor(id="ada"))
    [waiting] = await ws.runs()
    assert (waiting.status, waiting.reason) == ("retrying", None)


async def test_errors_are_classified_before_a_stream_and_within_one() -> None:
    gateway = LiteLLMGateway()
    ctx: Any = None
    request: Any = None
    blocked = ModelHTTPError(400, "m", {"message": "guardrail_name: 'pii'"})
    with pytest.raises(GuardrailBlocked):
        await gateway.on_model_request_error(ctx, request_context=request, error=blocked)
    other = ModelHTTPError(429, "m", {"message": "rate limited"})
    with pytest.raises(ModelHTTPError):
        await gateway.on_model_request_error(ctx, request_context=request, error=other)

    async def failing(error: Exception) -> AsyncIterator[Any]:
        yield "started"
        raise error

    async def drain(error: Exception) -> list[Any]:
        return [event async for event in gateway.wrap_run_event_stream(ctx, stream=failing(error))]

    with pytest.raises(GuardrailBlocked):
        await drain(blocked)
    with pytest.raises(ModelHTTPError):
        await drain(other)


def test_classifying_errors() -> None:
    unnamed = guardrail_block(ModelHTTPError(400, "m", {"message": "Guardrail violated"}))
    assert unnamed is not None
    assert (unnamed.guardrail, str(unnamed), unnamed.permanent) == (
        None,
        "The model request was blocked by a guardrail.",
        True,
    )
    assert guardrail_block(ModelHTTPError(500, "m", {"message": "guardrail down"})) is None


def test_the_model_calls_the_proxy() -> None:
    model = litellm_model("claude-sonnet", api_base="http://litellm:4000", api_key="sk-proxy")
    assert (model.model_name, model.system) == ("claude-sonnet", "litellm")
    assert str(model.client.base_url).rstrip("/") == "http://litellm:4000"


async def test_any_model_gets_the_same_settings_merged_with_its_own() -> None:
    seen: list[Any] = []

    def respond(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
        seen.append(info.model_settings)
        return ModelResponse(parts=[TextPart("Done.")])

    async def stream(messages: list[ModelMessage], info: AgentInfo) -> AsyncIterator[str]:
        seen.append(info.model_settings)
        yield "Done."

    agent: Agent[Reaction[None], str] = Agent(
        FunctionModel(respond, stream_function=stream),
        deps_type=Reaction[None],
        capabilities=[LiteLLMGateway(tenant_key=acme_key)],
        model_settings={"extra_body": {"user": "ada"}, "extra_headers": {"x-app": "oncall"}},
        name="review",
    )
    ws = await settle(AgentAction(agent), UserActor(id="ada"))
    [settings] = seen
    assert settings["extra_body"]["user"] == "ada"
    assert settings["extra_body"]["metadata"]["tenant_id"] == "acme"
    headers = settings["extra_headers"]
    assert (headers["x-app"], headers["Authorization"]) == ("oncall", "Bearer sk-acme-secret")
    [done] = await ws.runs()
    assert done.status == "succeeded"
