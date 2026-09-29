"""OpenTelemetry for applications, in one call: the ``[otel]`` extra.

This package is an adapter (ADR-0025). reflexr records through the OpenTelemetry API, its
port; this package wires the SDK behind it, with OTLP exporters, the open instrumentations and
reflexr's metric cardinality policy::

    telemetry = configure_telemetry(service_name="oncall", environment="production")
    app = create_app()
    telemetry.instrument_app(app)
    agent = Agent(..., capabilities=[EventContext(), telemetry.capability()])

Nothing in reflexr requires it, and nothing inside reflexr imports it.
"""

from reflexr.otel.configure import (
    BAGGAGE_KEYS,
    INSTRUMENTED,
    Instrumented,
    TelemetryHandle,
    configure_telemetry,
    installed,
    metric_views,
)

__all__ = [
    "BAGGAGE_KEYS",
    "INSTRUMENTED",
    "Instrumented",
    "TelemetryHandle",
    "configure_telemetry",
    "installed",
    "metric_views",
]
