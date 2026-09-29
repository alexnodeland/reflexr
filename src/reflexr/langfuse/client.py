"""A Langfuse client set up for reflexr's traces."""

from typing import Any

from langfuse import Langfuse
from opentelemetry.sdk.trace import TracerProvider

from reflexr.langfuse.tracing import should_export_span


def langfuse_client(*, tracer_provider: TracerProvider | None = None, **options: Any) -> Langfuse:
    """Return a Langfuse client that exports whole traces from a tracer provider.

    It adds Langfuse's span processor to ``tracer_provider`` (the global one, if omitted) with
    :func:`should_export_span` as its filter. Keys and the base URL come from ``options`` or the
    ``LANGFUSE_*`` environment variables.

    Args:
        tracer_provider: The SDK tracer provider whose spans go to Langfuse.
        **options: Passed to ``Langfuse(...)``, such as ``public_key`` or ``environment``.
    """
    options.setdefault("should_export_span", should_export_span)
    return Langfuse(tracer_provider=tracer_provider, **options)
