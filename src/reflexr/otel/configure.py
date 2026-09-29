"""``configure_telemetry``: the OpenTelemetry SDK, exporters and instrumentations in one call.

The instrumentations import the libraries they instrument, so each is imported only when it is
used: an application that does not use FastAPI or SQLAlchemy does not need them installed.
"""

import importlib.util
import logging
import os
from collections.abc import Callable, Collection, Mapping, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Self

from opentelemetry import metrics, trace
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.logging.handler import LoggingHandler
from opentelemetry.processor.baggage import BaggageSpanProcessor
from opentelemetry.sdk._logs import LoggerProvider
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, LogRecordExporter
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import MetricReader, PeriodicExportingMetricReader
from opentelemetry.sdk.metrics.view import View
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter
from pydantic_ai.capabilities import Instrumentation
from pydantic_ai.models.instrumented import InstrumentationSettings

from reflexr.telemetry import METRICS, SCOPE, MetricsDetail, kept_attributes
from reflexr.telemetry.attributes import SESSION_ID

if TYPE_CHECKING:
    from fastapi import FastAPI
    from langfuse import Langfuse
    from sqlalchemy import Engine
    from sqlalchemy.ext.asyncio import AsyncEngine

Instrumented = Literal["fastapi", "sqlalchemy", "asyncpg", "httpx"]
"""A library ``configure_telemetry`` can instrument. ``httpx`` covers httpx and httpx2."""

INSTRUMENTED: tuple[Instrumented, ...] = ("fastapi", "sqlalchemy", "asyncpg", "httpx")
"""Every library ``configure_telemetry`` can instrument."""

BAGGAGE_KEYS: frozenset[str] = frozenset({SESSION_ID})
"""The baggage entries copied onto every span: the session, which a run attempt places in
baggage."""

_EXCLUDED_ASGI_SPANS: list[Literal["receive", "send"]] = ["receive", "send"]

_SKIP_SQLALCHEMY_VERSION_CHECK = True
"""The SQLAlchemy instrumentation declares support below 2.1, which reflexr requires, but
works with 2.1 (the tests check), so its version check is skipped."""


def metric_views(detail: MetricsDetail = "workspace") -> list[View]:
    """Return views that keep only the attributes reflexr's metrics may carry at ``detail``.

    Pass them to the SDK's ``MeterProvider(views=...)``. With ``"tenant"`` the workspace is
    dropped, and with ``"none"`` both tenant and workspace are, before aggregation, so a
    deployment with many workspaces keeps a bounded number of series.
    """
    return [
        View(
            instrument_name=metric.name,
            meter_name=SCOPE,
            attribute_keys=set(kept_attributes(metric, detail)),
        )
        for metric in METRICS.values()
    ]


def installed() -> set[Instrumented]:
    """Return the instrumentable libraries that are installed."""
    return {name for name in INSTRUMENTED if importlib.util.find_spec(name) is not None}


class TelemetryHandle:
    """What ``configure_telemetry`` set up, and the way to shut it down.

    Use it as a context manager, or call :meth:`shutdown` when the application stops.

    Args:
        tracer_provider: The SDK's tracer provider.
        meter_provider: The SDK's meter provider.
        logger_provider: The SDK's logger provider, if logs are exported.
        instrumentation: pydantic-ai's instrumentation settings.
        instrument: The libraries to instrument now.
        engines: SQLAlchemy engines to instrument now.
        log_handler: A handler to add to the root logger until shutdown.
        langfuse: A Langfuse client to shut down with the rest.
    """

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider,
        meter_provider: MeterProvider,
        logger_provider: LoggerProvider | None,
        instrumentation: InstrumentationSettings,
        instrument: Collection[Instrumented] = (),
        engines: "Sequence[AsyncEngine | Engine]" = (),
        log_handler: logging.Handler | None = None,
        langfuse: "Langfuse | None" = None,
    ) -> None:
        self.tracer_provider = tracer_provider
        """The SDK's tracer provider."""
        self.meter_provider = meter_provider
        """The SDK's meter provider."""
        self.logger_provider = logger_provider
        """The SDK's logger provider, when logs are exported."""
        self.instrumentation = instrumentation
        """pydantic-ai's instrumentation settings over these providers."""
        self.langfuse = langfuse
        """The Langfuse client, when traces also go to Langfuse."""
        self._cleanups: list[Callable[[], object]] = []
        if langfuse is not None:
            self._cleanups.append(langfuse.shutdown)
        self._engines: list[Engine] = []
        if log_handler is not None:
            logging.getLogger().addHandler(log_handler)
            self._cleanups.append(lambda: _remove(log_handler))
        self._instrument(instrument)
        for engine in engines:
            self.instrument_engine(engine)

    def capability(self) -> Instrumentation:
        """Return pydantic-ai's ``Instrumentation`` capability, for an agent's ``capabilities``."""
        return Instrumentation(settings=self.instrumentation)

    def instrument_app(self, app: "FastAPI") -> None:
        """Trace and measure a FastAPI application's requests.

        FastAPI's instrumentation only reaches applications whose class it patched first;
        pass an application here to instrument it whenever it was created.
        """
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(
            app,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            exclude_spans=_EXCLUDED_ASGI_SPANS,
        )
        self._cleanups.append(lambda: FastAPIInstrumentor.uninstrument_app(app))

    def instrument_engine(self, engine: "AsyncEngine | Engine") -> None:
        """Trace a SQLAlchemy engine's queries and measure its connection pool.

        ``create_async_engine`` is imported before telemetry is configured in most
        applications, so its engines are not instrumented automatically: pass them here, or
        to ``configure_telemetry(engines=...)``.
        """
        from sqlalchemy.ext.asyncio import AsyncEngine

        sync = engine.sync_engine if isinstance(engine, AsyncEngine) else engine
        if sync not in self._engines:
            self._engines.append(sync)
            self._instrument_sqlalchemy()

    def _instrument_sqlalchemy(self) -> None:
        from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor

        # The instrumentor is a singleton that instruments once: start it over with every engine.
        instrumentor = SQLAlchemyInstrumentor()
        if instrumentor.is_instrumented_by_opentelemetry:
            instrumentor.uninstrument()
        instrumentor.instrument(
            engines=self._engines,
            tracer_provider=self.tracer_provider,
            meter_provider=self.meter_provider,
            skip_dep_check=_SKIP_SQLALCHEMY_VERSION_CHECK,
        )
        self._cleanups.append(instrumentor.uninstrument)

    def _instrument(self, libraries: Collection[Instrumented]) -> None:
        tracer_provider, meter_provider = self.tracer_provider, self.meter_provider
        if "fastapi" in libraries:
            from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

            fastapi = FastAPIInstrumentor()
            fastapi.instrument(
                tracer_provider=tracer_provider,
                meter_provider=meter_provider,
                exclude_spans=_EXCLUDED_ASGI_SPANS,
            )
            self._cleanups.append(fastapi.uninstrument)
        if "sqlalchemy" in libraries:
            self._instrument_sqlalchemy()
        if "asyncpg" in libraries:
            from opentelemetry.instrumentation.asyncpg import AsyncPGInstrumentor

            asyncpg = AsyncPGInstrumentor()
            asyncpg.instrument(tracer_provider=tracer_provider)
            self._cleanups.append(asyncpg.uninstrument)
        if "httpx" in libraries:
            from opentelemetry.instrumentation.httpx import (
                HTTPX2ClientInstrumentor,
                HTTPXClientInstrumentor,
            )

            for client in (HTTPXClientInstrumentor(), HTTPX2ClientInstrumentor()):
                client.instrument(tracer_provider=tracer_provider, meter_provider=meter_provider)
                self._cleanups.append(client.uninstrument)

    def shutdown(self) -> None:
        """Remove the instrumentation, and flush and shut down the providers. Idempotent."""
        cleanups, self._cleanups = self._cleanups, []
        # Each cleanup runs once, even when an engine re-instrumented SQLAlchemy.
        for cleanup in dict.fromkeys(reversed(cleanups)):
            cleanup()
        self.tracer_provider.shutdown()
        self.meter_provider.shutdown()
        if self.logger_provider is not None:
            self.logger_provider.shutdown()
            self.logger_provider = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.shutdown()


def configure_telemetry(
    *,
    service_name: str,
    service_version: str | None = None,
    environment: str | None = None,
    otlp_endpoint: str | None = None,
    otlp_headers: Mapping[str, str] | None = None,
    instrument: Collection[Instrumented] | None = None,
    engines: "Sequence[AsyncEngine | Engine]" = (),
    include_content: bool = False,
    metrics_detail: MetricsDetail = "workspace",
    logs: bool = True,
    span_exporter: SpanExporter | None = None,
    metric_reader: MetricReader | None = None,
    log_exporter: LogRecordExporter | None = None,
    span_processors: Sequence[SpanProcessor] = (),
    langfuse: bool = False,
    langfuse_options: Mapping[str, Any] | None = None,
    set_global: bool = True,
) -> TelemetryHandle:
    """Set up OpenTelemetry for an application: providers, OTLP export and instrumentation.

    Call it once, as early as the application starts, before it creates its FastAPI app and
    agents. It sets up:

    - tracer, meter and logger providers, with ``service.name``, ``service.version`` and
      ``deployment.environment.name`` on their resource
    - OTLP over HTTP to ``otlp_endpoint``, or to the endpoint the ``OTEL_EXPORTER_OTLP_*``
      environment variables name (a local Collector by default)
    - views that apply reflexr's metric cardinality policy at ``metrics_detail``
    - a baggage processor that copies a run attempt's session, its causal chain, onto every
      span in it
    - the open instrumentations for FastAPI, SQLAlchemy, asyncpg, httpx and httpx2, with the
      stable HTTP semantic conventions (``OTEL_SEMCONV_STABILITY_OPT_IN=http``, unless it is
      set already)
    - pydantic-ai's instrumentation settings, which :meth:`TelemetryHandle.capability` wraps
      for an agent
    - with ``langfuse=True``, a Langfuse client on the same tracer provider, which keeps whole
      traces (``reflexr.langfuse``, the ``[langfuse]`` extra)

    Nothing in reflexr requires it: it is one way to configure the SDK, which reflexr
    only ever records to through the API.

    Args:
        service_name: The service's name, as it appears in every backend.
        service_version: The service's version.
        environment: The deployment environment, such as ``production``.
        otlp_endpoint: The OTLP/HTTP base URL, such as ``http://collector:4318``. Defaults to
            the ``OTEL_EXPORTER_OTLP_ENDPOINT`` environment variable.
        otlp_headers: Headers for every OTLP request, such as a backend's credentials.
        instrument: Which libraries to instrument. Defaults to those that are installed.
        engines: SQLAlchemy engines that already exist, to trace their queries.
        include_content: Whether pydantic-ai records prompts, completions and tool arguments.
        metrics_detail: How much tenancy detail reflexr's metrics keep.
        logs: Whether to export the ``logging`` module's records through OTLP.
        span_exporter: Where spans go instead of OTLP, such as an in-memory exporter in tests.
        metric_reader: How metrics are read instead of a periodic OTLP export.
        log_exporter: Where log records go instead of OTLP.
        span_processors: More span processors to add, such as a backend's own.
        langfuse: Whether to send traces to Langfuse too, with ``reflexr.langfuse``'s span
            filter. Its keys come from the ``LANGFUSE_*`` environment variables or
            ``langfuse_options``.
        langfuse_options: Passed to ``Langfuse(...)``.
        set_global: Whether to make the providers the global ones, which reflexr, pydantic-ai
            and the instrumentations default to.

    Returns:
        A handle that shuts it all down.
    """
    attributes = {
        "service.name": service_name,
        "service.version": service_version,
        "deployment.environment.name": environment,
    }
    resource = Resource.create({k: v for k, v in attributes.items() if v is not None})
    headers = dict(otlp_headers or {})

    tracer_provider = TracerProvider(resource=resource)
    tracer_provider.add_span_processor(BaggageSpanProcessor(lambda key: key in BAGGAGE_KEYS))
    exporter = span_exporter or OTLPSpanExporter(
        endpoint=_signal(otlp_endpoint, "traces"), headers=headers
    )
    tracer_provider.add_span_processor(BatchSpanProcessor(exporter))
    for processor in span_processors:
        tracer_provider.add_span_processor(processor)

    reader = metric_reader or PeriodicExportingMetricReader(
        OTLPMetricExporter(endpoint=_signal(otlp_endpoint, "metrics"), headers=headers)
    )
    meter_provider = MeterProvider(
        resource=resource, metric_readers=[reader], views=metric_views(metrics_detail)
    )

    logger_provider: LoggerProvider | None = None
    log_handler: logging.Handler | None = None
    if logs:
        logger_provider = LoggerProvider(resource=resource)
        logger_provider.add_log_record_processor(
            BatchLogRecordProcessor(
                log_exporter
                or OTLPLogExporter(endpoint=_signal(otlp_endpoint, "logs"), headers=headers)
            )
        )
        log_handler = LoggingHandler(logger_provider=logger_provider)

    if set_global:
        trace.set_tracer_provider(tracer_provider)
        metrics.set_meter_provider(meter_provider)

    os.environ.setdefault("OTEL_SEMCONV_STABILITY_OPT_IN", "http")
    client = None
    if langfuse:
        from reflexr.langfuse import langfuse_client

        client = langfuse_client(tracer_provider=tracer_provider, **dict(langfuse_options or {}))
    return TelemetryHandle(
        tracer_provider=tracer_provider,
        meter_provider=meter_provider,
        logger_provider=logger_provider,
        instrumentation=InstrumentationSettings(
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            include_content=include_content,
            include_binary_content=include_content,
        ),
        instrument=installed() if instrument is None else instrument,
        engines=engines,
        log_handler=log_handler,
        langfuse=client,
    )


def _remove(handler: logging.Handler) -> None:
    logging.getLogger().removeHandler(handler)
    # Closed and unreferenced, so the logging module's exit hook no longer flushes it.
    handler.close()


def _signal(endpoint: str | None, signal: str) -> str | None:
    """The OTLP/HTTP URL for one signal, or None to let the exporter read the environment."""
    return None if endpoint is None else f"{endpoint.rstrip('/')}/v1/{signal}"
