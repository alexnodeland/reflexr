"""OpenTelemetry for applications, in one call: the ``[otel]`` extra.

This package is an adapter (ADR-0025). reflexr records through the OpenTelemetry API, its
port; this package wires the SDK behind it, with OTLP exporters, the open instrumentations and
reflexr's metric cardinality policy::

    telemetry = configure_telemetry(service_name="oncall", environment="production")
    app = create_app()
    telemetry.instrument_app(app)
    agent = Agent(..., capabilities=[EventContext(), telemetry.capability()])

An application that runs artifactr too passes artifactr's contribution, and configures
telemetry once for both (ADR-0040)::

    telemetry = configure_telemetry(artifactr.otel.telemetry(), service_name="app")

Nothing in reflexr requires it, and nothing inside reflexr imports it.
"""

from reflexr.otel.configure import (
    BAGGAGE_KEYS,
    INSTRUMENTED,
    Contribution,
    Instrumented,
    LangfuseMode,
    TelemetryContribution,
    TelemetryHandle,
    configure_telemetry,
    installed,
    metric_views,
    telemetry,
)

__all__ = [
    "BAGGAGE_KEYS",
    "INSTRUMENTED",
    "Contribution",
    "Instrumented",
    "LangfuseMode",
    "TelemetryContribution",
    "TelemetryHandle",
    "configure_telemetry",
    "installed",
    "metric_views",
    "telemetry",
]
