"""The tracer and instruments a :class:`~reflexr.workspace.Workspaces` records with.

reflexr uses the OpenTelemetry API only. Without an SDK configured every span and measurement
is a no-op, and applications choose the SDK, exporters and backends (ADR-0018).
"""

from collections.abc import Callable, Mapping, Set
from importlib.metadata import version

from opentelemetry import metrics, trace
from opentelemetry.metrics import Meter, MeterProvider
from opentelemetry.trace import SpanContext, Tracer, TracerProvider, get_current_span
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.util.types import AttributeValue

from reflexr.core import Actor, RuleName, TenantId, UserActor, WorkspaceId
from reflexr.telemetry import attributes as a
from reflexr.telemetry.metrics import METRICS, SCOPE, Metric

type Attributes = Mapping[str, AttributeValue]

type _Recorder = Callable[[float, Attributes], None]

_W3C = TraceContextTextMapPropagator()


class Telemetry:
    """reflexr's tracer, and an instrument for every metric in the registry.

    Tenant and workspace are always recorded; a deployment that wants less detail says so to
    the SDK, with ``reflexr.otel.metric_views``.

    Args:
        tracer_provider: Where spans go. Defaults to the global provider.
        meter_provider: Where metrics go. Defaults to the global provider.
        stored_namespaces: The rule namespaces of stored rules, whose names code does not bound.
            Metrics record a rule in one as the namespace, such as ``chat:*``.
    """

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        stored_namespaces: Set[str] = frozenset(),
    ) -> None:
        release = version("reflexr")
        self.tracer: Tracer = trace.get_tracer(SCOPE, release, tracer_provider)
        meter = metrics.get_meter(SCOPE, release, meter_provider)
        self._recorders = {name: _recorder(meter, metric) for name, metric in METRICS.items()}
        self._stored_namespaces = frozenset(stored_namespaces)

    def rule_attribute(self, rule: RuleName) -> str:
        """Return what metrics record as a rule's ``reflexr.rule``.

        That is its name, or for a stored rule its namespace, such as ``chat:*``, so tenants'
        rules share a series per namespace.
        """
        namespace = rule.partition(":")[0]
        return f"{namespace}:*" if namespace in self._stored_namespaces else rule

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

        A rule is recorded as :meth:`rule_attribute` says.

        Raises:
            KeyError: If the metric is not in the registry.
            ValueError: If an attribute is not one the metric declares.
        """
        recorder = self._recorders[metric.name]
        given = {**(attributes or {}), a.TENANT_ID: tenant_id, a.WORKSPACE_ID: workspace_id}
        if a.RULE in given:
            given[a.RULE] = self.rule_attribute(str(given[a.RULE]))
        undeclared = given.keys() - METRICS[metric.name].attributes
        if undeclared:
            raise ValueError(f"{metric.name} does not declare {sorted(undeclared)}")
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


def current_trace_id() -> str | None:
    """Return the current span's trace id as 32 hex digits, if there is a valid span."""
    context = get_current_span().get_span_context()
    return f"{context.trace_id:032x}" if context.is_valid else None


def current_traceparent() -> str | None:
    """Return the W3C trace context of the current span, if there is one."""
    carrier: dict[str, str] = {}
    _W3C.inject(carrier)
    return carrier.get("traceparent")


def parse_traceparent(traceparent: str | None) -> SpanContext | None:
    """Return the span context a W3C ``traceparent`` names, if it is valid."""
    if traceparent is None:
        return None
    context = get_current_span(_W3C.extract({"traceparent": traceparent})).get_span_context()
    return context if context.is_valid else None


def _recorder(meter: Meter, metric: Metric) -> _Recorder:
    name, unit, description = metric.name, metric.unit, metric.description
    if metric.instrument == "histogram":
        return meter.create_histogram(
            name,
            unit=unit,
            description=description,
            explicit_bucket_boundaries_advisory=metric.buckets,
        ).record
    if metric.instrument == "gauge":
        return meter.create_gauge(name, unit=unit, description=description).set
    if metric.instrument == "up_down_counter":
        return meter.create_up_down_counter(name, unit=unit, description=description).add
    return meter.create_counter(name, unit=unit, description=description).add
