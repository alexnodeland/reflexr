"""Running oncall in production: telemetry to OpenTelemetry and Langfuse, and a LiteLLM proxy."""

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import uvicorn
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic_ai.models.openai import OpenAIChatModel

import oncall.app
from conftest import FakeClock, Script, log, publish, triage
from oncall.app import Oncall, telemetry_from_environment
from oncall.triage import build_triage_agent
from reflexr.otel import TelemetryHandle, configure_telemetry


@pytest.fixture
def spans() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture
def telemetry(spans: InMemorySpanExporter) -> Iterator[TelemetryHandle]:
    """Telemetry into memory, leaving the global providers and libraries alone."""
    with configure_telemetry(
        service_name="oncall",
        instrument=(),
        logs=False,
        span_exporter=spans,
        metric_reader=InMemoryMetricReader(),
        set_global=False,
    ) as handle:
        yield handle


async def test_runs_requests_and_queries_are_traced(
    tmp_path: Path,
    script: Script,
    clock: FakeClock,
    telemetry: TelemetryHandle,
    spans: InMemorySpanExporter,
) -> None:
    script.steps += triage()
    url = f"sqlite+aiosqlite:///{tmp_path / 'oncall.db'}"
    system = Oncall(model=script.model, clock=clock, database_url=url, telemetry=telemetry)
    app = system.app(serve=False)
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://oncall") as client:
            for version in ("v1", "v2"):
                await publish(client, type="deploy.completed", service="api", version=version)
            for _ in range(3):
                await publish(client, type="alert.fired", service="api", severity=9, message="5xx")
            await system.reactor.settle()
            assert await log(client, "incident.resolved")
    telemetry.tracer_provider.force_flush()
    names = {span.name for span in spans.get_finished_spans()}
    assert {"invoke_workflow triage", "invoke_agent triage", "invoke_workflow runbook"} <= names
    assert "POST /v1/workspaces/{workspace_id}/events" in names, "requests are traced"
    assert any(name.startswith("INSERT") for name in names), "queries are traced"


def test_telemetry_is_configured_from_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    assert telemetry_from_environment() is None
    configured: dict[str, Any] = {}
    monkeypatch.setattr(
        oncall.app, "configure_telemetry", lambda **options: configured.update(options)
    )
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("LANGFUSE_PUBLIC_KEY", "pk-lf-1")
    telemetry_from_environment()
    assert configured == {
        "service_name": "oncall",
        "service_version": "0.1.0",
        "environment": "development",
        "langfuse": True,
    }


def test_main_shuts_telemetry_down(
    monkeypatch: pytest.MonkeyPatch, telemetry: TelemetryHandle
) -> None:
    shut: list[bool] = []
    monkeypatch.setattr(oncall.app, "telemetry_from_environment", lambda: telemetry)
    monkeypatch.setattr(telemetry, "shutdown", lambda: shut.append(True))
    monkeypatch.setattr(uvicorn, "run", lambda app, **kwargs: None)
    oncall.app.main()
    assert shut == [True]


def test_the_agent_can_call_a_litellm_proxy(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ONCALL_LITELLM_URL", "http://litellm:4000")
    monkeypatch.setenv("ONCALL_LITELLM_KEY", "sk-oncall")
    model = build_triage_agent().model
    assert isinstance(model, OpenAIChatModel)
    assert (model.model_name, model.base_url) == ("claude-sonnet", "http://litellm:4000")
    monkeypatch.setenv("ONCALL_LITELLM_MODEL", "claude-haiku")
    haiku = build_triage_agent().model
    assert isinstance(haiku, OpenAIChatModel)
    assert haiku.model_name == "claude-haiku"
    assert build_triage_agent("test").model == "test", "a model given wins"
