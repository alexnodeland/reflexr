"""Which spans are reflexr's, and untraced work: spans that are never sampled."""

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, Sampler

from reflexr.telemetry import TRACE_SCOPES, is_trace_scope, untraced


def test_reflexrs_scopes_and_their_sub_scopes_are_in_its_traces() -> None:
    assert "reflexr" in TRACE_SCOPES
    assert is_trace_scope("reflexr")
    assert is_trace_scope("opentelemetry.instrumentation.sqlalchemy")
    assert is_trace_scope("reflexr.workspace")
    assert not is_trace_scope("reflexr_like")
    assert not is_trace_scope("pydantic-ai"), "Langfuse keeps model calls by its own rule"
    assert not is_trace_scope("")


def recorded(sampler: Sampler | None = None) -> tuple[TracerProvider, InMemorySpanExporter]:
    spans = InMemorySpanExporter()
    tracer_provider = TracerProvider() if sampler is None else TracerProvider(sampler=sampler)
    tracer_provider.add_span_processor(SimpleSpanProcessor(spans))
    return tracer_provider, spans


def test_spans_started_untraced_are_not_recorded() -> None:
    tracer_provider, spans = recorded()
    tracer = tracer_provider.get_tracer("test")
    with tracer.start_as_current_span("request") as request:
        with untraced():
            with tracer.start_as_current_span("poll") as poll, untraced():
                assert not poll.is_recording()
                with tracer.start_as_current_span("nested") as nested:
                    assert not nested.is_recording()
            assert trace.get_current_span() is not request
        assert trace.get_current_span() is request, "the block's context is restored"
        with tracer.start_as_current_span("work"):
            pass
    assert [span.name for span in spans.get_finished_spans()] == ["work", "request"]


def test_a_sampler_that_ignores_the_parent_records_them() -> None:
    tracer_provider, spans = recorded(ALWAYS_ON)
    with untraced(), tracer_provider.get_tracer("test").start_as_current_span("poll"):
        pass
    assert [span.name for span in spans.get_finished_spans()] == ["poll"]
