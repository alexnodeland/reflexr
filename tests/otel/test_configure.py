"""configure_telemetry: the SDK, exporters and instrumentations, and shutting them down."""

import importlib.util
import logging
import os
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry import baggage, metrics, trace
from opentelemetry import context as otel_context
from opentelemetry.instrumentation.httpx import HTTPX2ClientInstrumentor, HTTPXClientInstrumentor
from opentelemetry.sdk._logs.export import InMemoryLogRecordExporter
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from pydantic_ai.capabilities import Instrumentation
from sqlalchemy import text

from reflexr.otel import (
    INSTRUMENTED,
    TelemetryHandle,
    configure_telemetry,
    installed,
    metric_views,
)
from reflexr.otel.configure import _signal
from reflexr.sql import create_sqlite_engine
from reflexr.telemetry import SCOPE, Telemetry
from reflexr.telemetry.attributes import EVENT_TYPE, SESSION_ID, TENANT_ID, WORKSPACE_ID
from reflexr.telemetry.metrics import EVENTS_PUBLISHED


class Captured:
    """In-memory exporters and a reader, to pass to configure_telemetry."""

    def __init__(self) -> None:
        self.spans = InMemorySpanExporter()
        self.reader = InMemoryMetricReader()
        self.logs = InMemoryLogRecordExporter()

    def options(self) -> dict[str, Any]:
        return {
            "span_exporter": self.spans,
            "metric_reader": self.reader,
            "log_exporter": self.logs,
            "set_global": False,
        }

    def points(self, name: str) -> list[dict[str, Any]]:
        data = self.reader.get_metrics_data()
        return [
            dict(point.attributes or {})
            for resource in (data.resource_metrics if data else ())
            for scope in resource.scope_metrics
            for metric in scope.metrics
            if metric.name == name
            for point in metric.data.data_points
        ]


@pytest.fixture
def captured() -> Captured:
    return Captured()


@pytest.fixture
def handle(captured: Captured) -> Iterator[TelemetryHandle]:
    with configure_telemetry(
        service_name="oncall", service_version="1.2.3", environment="test", **captured.options()
    ) as handle:
        yield handle


def test_spans_carry_the_service_and_the_runs_session(
    handle: TelemetryHandle, captured: Captured
) -> None:
    tracer = handle.tracer_provider.get_tracer("test")
    token = otel_context.attach(
        baggage.set_baggage("user.id", "alice", baggage.set_baggage(SESSION_ID, "evt_1"))
    )
    try:
        with tracer.start_as_current_span("query"):
            pass
    finally:
        otel_context.detach(token)
    handle.tracer_provider.force_flush()
    [span] = captured.spans.get_finished_spans()
    assert (
        dict(span.resource.attributes).items()
        >= {
            "service.name": "oncall",
            "service.version": "1.2.3",
            "deployment.environment.name": "test",
        }.items()
    )
    assert dict(span.attributes or {}) == {SESSION_ID: "evt_1"}, "only the session is copied"


def test_more_span_processors_can_be_added(captured: Captured) -> None:
    extra = InMemorySpanExporter()
    with configure_telemetry(
        service_name="oncall",
        instrument=(),
        span_processors=[SimpleSpanProcessor(extra)],
        **captured.options(),
    ) as handle:
        with handle.tracer_provider.get_tracer("test").start_as_current_span("work"):
            pass
        assert [span.name for span in extra.get_finished_spans()] == ["work"]


@pytest.mark.parametrize(
    ("detail", "kept"),
    [
        ("workspace", {TENANT_ID, WORKSPACE_ID}),
        ("tenant", {TENANT_ID}),
        ("none", set[str]()),
    ],
)
def test_views_apply_the_level_of_detail(detail: Any, kept: set[str], captured: Captured) -> None:
    with configure_telemetry(
        service_name="oncall", metrics_detail=detail, instrument=(), **captured.options()
    ) as handle:
        telemetry = Telemetry(meter_provider=handle.meter_provider)
        telemetry.record(
            EVENTS_PUBLISHED,
            1,
            tenant_id="t1",
            workspace_id="w1",
            attributes={EVENT_TYPE: "deploy.finished"},
        )
        [point] = captured.points("reflexr.events.published")
    assert set(point) == kept | {EVENT_TYPE}


def test_metric_views_cover_every_reflexr_metric() -> None:
    views = metric_views("none")
    assert all(view._meter_name == SCOPE for view in views)
    assert all(TENANT_ID not in (view._attribute_keys or ()) for view in views)


def test_logs_are_exported_until_shutdown(captured: Captured) -> None:
    handle = configure_telemetry(service_name="oncall", instrument=(), **captured.options())
    root = logging.getLogger()
    before = list(root.handlers)
    with handle.tracer_provider.get_tracer("test").start_as_current_span("work") as span:
        logging.getLogger("oncall").warning("the pager is late")
    assert handle.logger_provider is not None
    handle.logger_provider.force_flush()
    [record] = captured.logs.get_finished_logs()
    assert record.log_record.body == "the pager is late"
    assert record.log_record.trace_id == span.get_span_context().trace_id, "logged in the trace"
    handle.shutdown()
    handle.shutdown()  # idempotent
    assert len(root.handlers) == len(before) - 1


def test_without_logs_there_is_no_logger_provider(captured: Captured) -> None:
    with configure_telemetry(
        service_name="oncall", logs=False, instrument=(), **captured.options()
    ) as handle:
        assert handle.logger_provider is None


def test_pydantic_ai_instrumentation_uses_the_providers(handle: TelemetryHandle) -> None:
    capability = handle.capability()
    assert isinstance(capability, Instrumentation)
    assert capability.settings is handle.instrumentation
    assert handle.instrumentation.include_content is False


def test_fastapi_apps_are_traced(handle: TelemetryHandle, captured: Captured) -> None:
    app = FastAPI()

    @app.get("/ping")
    async def ping() -> str:
        return "pong"

    handle.instrument_app(app)
    with TestClient(app) as client:
        assert client.get("/ping").json() == "pong"
    handle.tracer_provider.force_flush()
    assert [span.name for span in captured.spans.get_finished_spans()] == ["GET /ping"]


async def test_sqlalchemy_engines_are_traced(
    handle: TelemetryHandle, captured: Captured, tmp_path: Any
) -> None:
    engine = create_sqlite_engine(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    try:
        handle.instrument_engine(engine)
        handle.instrument_engine(engine.sync_engine)
        async with engine.connect() as connection:
            await connection.execute(text("select 1"))
    finally:
        await engine.dispose()
    handle.tracer_provider.force_flush()
    names = [span.name for span in captured.spans.get_finished_spans()]
    assert any(name.startswith("select") or name == "SELECT" for name in names), names


async def test_existing_engines_can_be_passed_in(captured: Captured, tmp_path: Any) -> None:
    engine = create_sqlite_engine(f"sqlite+aiosqlite:///{tmp_path / 'app.db'}")
    try:
        with configure_telemetry(
            service_name="oncall",
            engines=[engine],
            instrument=("sqlalchemy",),
            **captured.options(),
        ) as handle:
            async with engine.connect() as connection:
                await connection.execute(text("select 1"))
            handle.tracer_provider.force_flush()
    finally:
        await engine.dispose()
    assert captured.spans.get_finished_spans()


def test_installed_libraries_are_instrumented_until_shutdown(captured: Captured) -> None:
    with configure_telemetry(service_name="oncall", **captured.options()):
        assert HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
        assert HTTPX2ClientInstrumentor().is_instrumented_by_opentelemetry
    assert not HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
    assert not HTTPX2ClientInstrumentor().is_instrumented_by_opentelemetry


def test_only_installed_libraries_are_instrumented_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert installed() == set(INSTRUMENTED)
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util, "find_spec", lambda name: None if name == "asyncpg" else real(name)
    )
    assert installed() == {"fastapi", "sqlalchemy", "httpx"}


def test_httpx2_alone_is_instrumented(captured: Captured, monkeypatch: pytest.MonkeyPatch) -> None:
    real = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util, "find_spec", lambda name: None if name == "httpx" else real(name)
    )
    assert "httpx" in installed(), "pydantic-ai's providers use httpx2 without httpx"
    with configure_telemetry(service_name="oncall", instrument=("httpx",), **captured.options()):
        assert HTTPX2ClientInstrumentor().is_instrumented_by_opentelemetry
        assert not HTTPXClientInstrumentor().is_instrumented_by_opentelemetry
    assert not HTTPX2ClientInstrumentor().is_instrumented_by_opentelemetry


def test_stable_http_conventions_are_opted_into(
    captured: Captured, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("OTEL_SEMCONV_STABILITY_OPT_IN", raising=False)
    with configure_telemetry(service_name="oncall", instrument=(), **captured.options()):
        assert os.environ["OTEL_SEMCONV_STABILITY_OPT_IN"] == "http"


def test_global_providers_are_set(captured: Captured, monkeypatch: pytest.MonkeyPatch) -> None:
    set_globally: list[object] = []
    monkeypatch.setattr(trace, "set_tracer_provider", set_globally.append)
    monkeypatch.setattr(metrics, "set_meter_provider", set_globally.append)
    options: dict[str, Any] = {**captured.options(), "set_global": True}
    with configure_telemetry(service_name="oncall", instrument=(), **options) as handle:
        assert set_globally == [handle.tracer_provider, handle.meter_provider]


def test_otlp_is_the_default_export(monkeypatch: pytest.MonkeyPatch) -> None:
    with configure_telemetry(
        service_name="oncall",
        otlp_endpoint="http://127.0.0.1:9/",
        otlp_headers={"x-langfuse-ingestion-version": "4"},
        instrument=(),
        set_global=False,
    ) as handle:
        assert handle.logger_provider is not None
    assert _signal("http://collector:4318/", "traces") == "http://collector:4318/v1/traces"
    assert _signal(None, "traces") is None
