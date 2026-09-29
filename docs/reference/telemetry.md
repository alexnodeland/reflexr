# reflexr.telemetry

Tracing and metrics through the OpenTelemetry API. See [Observability](../guides/observability.md).

::: reflexr.telemetry
    options:
      members: false
      show_root_heading: false
      show_root_toc_entry: false

## The metric registry

::: reflexr.telemetry.Metric

::: reflexr.telemetry.METRICS

::: reflexr.telemetry.EXTERNAL_METRICS

::: reflexr.telemetry.Instrument

::: reflexr.telemetry.MetricsDetail

::: reflexr.telemetry.kept_attributes

::: reflexr.telemetry.SCOPE

::: reflexr.telemetry.SCOPED

## Traces

Which spans are reflexr's, and polling that makes no traces. See [ADR-0040](../adr/0040-telemetry-that-composes-across-libraries.md).

::: reflexr.telemetry.TRACE_SCOPES

::: reflexr.telemetry.is_trace_scope

::: reflexr.telemetry.untraced

## Attributes

::: reflexr.telemetry.attributes
    options:
      show_root_heading: false
      show_root_toc_entry: false

## Recording

These are what reflexr's components record with. Applications rarely need them, except the attribute helpers to attribute spans of their own.

::: reflexr.telemetry.Telemetry

::: reflexr.telemetry.workspace_attributes

::: reflexr.telemetry.actor_attributes

::: reflexr.telemetry.chain_attributes

::: reflexr.telemetry.current_trace_id

::: reflexr.telemetry.current_traceparent

::: reflexr.telemetry.parse_traceparent
