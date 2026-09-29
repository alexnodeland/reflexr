"""configure_telemetry(langfuse="traces"): traces go to the Collector and to Langfuse."""

import uuid

import httpx
from langfuse import Langfuse
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from reflexr.otel import configure_telemetry
from tests.langfuse.conftest import FakeLangfuseApi


def test_langfuse_is_configured_on_the_same_tracer_provider() -> None:
    collector, langfuse = InMemorySpanExporter(), InMemorySpanExporter()
    api = FakeLangfuseApi()
    with configure_telemetry(
        service_name="oncall",
        instrument=(),
        logs=False,
        set_global=False,
        span_exporter=collector,
        metric_reader=InMemoryMetricReader(),
        langfuse="traces",
        langfuse_options={
            "public_key": f"pk-lf-{uuid.uuid4()}",
            "secret_key": "sk-lf-test",
            "base_url": "http://langfuse.test",
            "httpx_client": httpx.Client(transport=httpx.MockTransport(api.handle)),
            "span_exporter": langfuse,
        },
    ) as telemetry:
        assert isinstance(telemetry.langfuse, Langfuse)
        tracer = telemetry.tracer_provider.get_tracer("reflexr")
        with tracer.start_as_current_span("reflexr.publish deploy.finished"):
            pass
        with telemetry.tracer_provider.get_tracer("some.library").start_as_current_span("noise"):
            pass
        telemetry.tracer_provider.force_flush()
        telemetry.langfuse.flush()
    assert [s.name for s in collector.get_finished_spans()] == [
        "reflexr.publish deploy.finished",
        "noise",
    ]
    assert [s.name for s in langfuse.get_finished_spans()] == ["reflexr.publish deploy.finished"]
