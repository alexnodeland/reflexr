"""reflexr's OpenTelemetry instrumentation: spans, the metric registry and attribute names.

reflexr depends on the OpenTelemetry API only and never configures the SDK (ADR-0018). Pass
tracer and meter providers to :class:`~reflexr.workspace.Workspaces`, or configure the global
ones, to see its spans and metrics.
"""

from reflexr.telemetry import attributes
from reflexr.telemetry.metrics import (
    METRICS,
    SCOPE_ATTRIBUTES,
    Instrument,
    Metric,
    MetricsDetail,
)
from reflexr.telemetry.telemetry import (
    SCOPE_NAME,
    Telemetry,
    actor_attributes,
    chain_attributes,
    current_traceparent,
    parse_traceparent,
    workspace_attributes,
)

__all__ = [
    "METRICS",
    "SCOPE_ATTRIBUTES",
    "SCOPE_NAME",
    "Instrument",
    "Metric",
    "MetricsDetail",
    "Telemetry",
    "actor_attributes",
    "attributes",
    "chain_attributes",
    "current_traceparent",
    "parse_traceparent",
    "workspace_attributes",
]
