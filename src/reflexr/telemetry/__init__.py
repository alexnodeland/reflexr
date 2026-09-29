"""reflexr's OpenTelemetry instrumentation: spans, the metric registry and attribute names.

reflexr depends on the OpenTelemetry API only and never configures the SDK (ADR-0018). Pass
tracer and meter providers to :class:`~reflexr.workspace.Workspaces`, or configure the global
ones, to see its spans and metrics. :mod:`reflexr.telemetry.traces` names the spans in reflexr's
traces, and runs polling untraced (ADR-0040).
"""

from reflexr.telemetry import attributes
from reflexr.telemetry.metrics import (
    EXTERNAL_METRICS,
    METRICS,
    SCOPE,
    SCOPED,
    Instrument,
    Metric,
    MetricsDetail,
    kept_attributes,
)
from reflexr.telemetry.telemetry import (
    Telemetry,
    actor_attributes,
    chain_attributes,
    current_trace_id,
    current_traceparent,
    parse_traceparent,
    workspace_attributes,
)
from reflexr.telemetry.traces import TRACE_SCOPES, is_trace_scope, untraced

__all__ = [
    "EXTERNAL_METRICS",
    "METRICS",
    "SCOPE",
    "SCOPED",
    "TRACE_SCOPES",
    "Instrument",
    "Metric",
    "MetricsDetail",
    "Telemetry",
    "actor_attributes",
    "attributes",
    "chain_attributes",
    "current_trace_id",
    "current_traceparent",
    "is_trace_scope",
    "kept_attributes",
    "parse_traceparent",
    "untraced",
    "workspace_attributes",
]
