"""The tracer and instruments a :class:`~reflexr.workspace.Workspaces` records with.

reflexr uses the OpenTelemetry API only. Without an SDK configured every span and measurement
is a no-op, and applications choose the SDK, exporters and backends (ADR-0018).
"""

from collections.abc import Callable, Mapping
from importlib.metadata import version

from opentelemetry import metrics, trace
from opentelemetry.metrics import Meter, MeterProvider
from opentelemetry.trace import Tracer, TracerProvider
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.util.types import AttributeValue

from reflexr.core import Actor, TenantId, UserActor, WorkspaceId
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import METRICS, Metric, MetricsDetail

SCOPE_NAME = "reflexr"
"""The instrumentation scope of every span and metric reflexr records."""

type Attributes = Mapping[str, AttributeValue]

type _Recorder = Callable[[float, Attributes], None]

_W3C = TraceContextTextMapPropagator()


class Telemetry:
    """reflexr's tracer, and an instrument for every metric in the registry.

    Args:
        tracer_provider: Where spans go. Defaults to the global provider.
        meter_provider: Where metrics go. Defaults to the global provider.
        metrics_detail: Which of tenant and workspace become metric attributes.
    """

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        metrics_detail: MetricsDetail = "workspace",
    ) -> None:
        release = version("reflexr")
        self.tracer: Tracer = trace.get_tracer(SCOPE_NAME, release, tracer_provider)
        meter = metrics.get_meter(SCOPE_NAME, release, meter_provider)
        self._recorders = {name: _recorder(meter, metric) for name, metric in METRICS.items()}
        self._detail: MetricsDetail = metrics_detail

    def record(
        self,
        metric: Metric,
        value: float,
        *,
        tenant_id: TenantId,
        workspace_id: WorkspaceId,
        attributes: Attributes | None = None,
    ) -> None:
        """Record a measurement of a registered metric.

        Raises:
            KeyError: If the metric is not in the registry.
            ValueError: If an attribute is not one the metric declares.
        """
        recorder = self._recorders[metric.name]
        given = dict(attributes or {})
        undeclared = given.keys() - METRICS[metric.name].attributes
        if undeclared:
            raise ValueError(f"{metric.name} does not declare {sorted(undeclared)}")
        if self._detail != "none":
            given[a.TENANT_ID] = tenant_id
        if self._detail == "workspace":
            given[a.WORKSPACE_ID] = workspace_id
        recorder(value, given)


def workspace_attributes(tenant_id: TenantId, workspace_id: WorkspaceId) -> dict[str, str]:
    """Return the span attributes that attribute work to a workspace."""
    return {a.TENANT_ID: tenant_id, a.WORKSPACE_ID: workspace_id}


def actor_attributes(actor: Actor) -> dict[str, str]:
    """Return the span attributes that attribute work to an actor."""
    found = {a.ACTOR_KIND: actor.kind}
    if isinstance(actor, UserActor):
        found[a.USER_ID] = actor.id
    return found


def chain_attributes(correlation_id: str) -> dict[str, str]:
    """Return the span attributes that put a span in its causal chain's session."""
    return {a.SESSION_ID: correlation_id, a.CONVERSATION_ID: correlation_id}


def current_traceparent() -> str | None:
    """Return the W3C trace context of the current span, if there is one."""
    carrier: dict[str, str] = {}
    _W3C.inject(carrier)
    return carrier.get("traceparent")


def _recorder(meter: Meter, metric: Metric) -> _Recorder:
    name, unit, description = metric.name, metric.unit, metric.description
    if metric.instrument == "histogram":
        return meter.create_histogram(name, unit=unit, description=description).record
    if metric.instrument == "gauge":
        return meter.create_gauge(name, unit=unit, description=description).set
    return meter.create_counter(name, unit=unit, description=description).add
