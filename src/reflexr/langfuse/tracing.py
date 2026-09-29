"""Traces in Langfuse: span filters that keep whole traces, or none, and a run's attributes."""

import contextlib
from collections.abc import AsyncGenerator, Callable
from typing import Any

from langfuse import propagate_attributes
from langfuse.span_filter import is_default_export_span
from opentelemetry.sdk.trace import ReadableSpan

from reflexr.core import UserActor
from reflexr.telemetry import is_trace_scope
from reflexr.workspace import Reaction

MAX_ATTRIBUTE = 200
"""The longest trace attribute value Langfuse accepts."""


def should_export_span(span: ReadableSpan) -> bool:
    """Return whether Langfuse should export a span: pass it as ``should_export_span``.

    Langfuse's default keeps only LLM spans. This keeps those, and the spans of every scope in
    ``reflexr.telemetry.TRACE_SCOPES`` (reflexr's, pydantic-graph's, the MCP SDK's and the
    FastAPI, SQLAlchemy, asyncpg and httpx instrumentations'), so a run's trace is whole: its
    steps, database queries and HTTP calls around the model calls.
    """
    scope = span.instrumentation_scope.name if span.instrumentation_scope else ""
    return is_trace_scope(scope) or is_default_export_span(span)


def span_filter(*keeps: Callable[[ReadableSpan], bool]) -> Callable[[ReadableSpan], bool]:
    """Return a span filter that keeps Langfuse's LLM spans and every span one of ``keeps`` keeps.

    ``configure_telemetry(langfuse="traces")`` builds its filter this way, from the libraries'
    contributions (ADR-0040), so an application of several libraries keeps each one's spans.
    """

    def keep(span: ReadableSpan) -> bool:
        return is_default_export_span(span) or any(kept(span) for kept in keeps)

    return keep


def no_spans(span: ReadableSpan) -> bool:
    """Keep no spans: the filter for a client that only sets trace attributes and sends scores.

    ``configure_telemetry(langfuse="scores")`` uses it when a Collector sends Langfuse every
    trace already, so no span arrives twice. The client's span processor still sets each
    run's session, user and tags on the spans, which reach Langfuse through the Collector.
    """
    del span
    return False


def run_attributes(reaction: Reaction[Any]) -> dict[str, Any]:
    """Return a run attempt's Langfuse trace attributes, within Langfuse's limits.

    The session is the causal chain, so one incident is one Langfuse session; the user is the
    person whose event made the rule fire, if a person published it; the trace name is the
    rule; the tags name the tenant, the workspace and the rule; the metadata holds reflexr's
    ids. Values are ASCII and at most 200 characters.
    """
    workspace = reaction.workspace
    run = reaction.run
    cause = reaction.events[-1].actor if reaction.events else None
    return {
        "session_id": _limit(run.correlation_id),
        "user_id": _limit(cause.id) if isinstance(cause, UserActor) else None,
        "trace_name": _limit(run.rule),
        "tags": [
            _limit(f"tenant:{workspace.tenant_id}"),
            _limit(f"workspace:{workspace.workspace_id}"),
            _limit(f"rule:{run.rule}"),
        ],
        "metadata": {
            key: _limit(value)
            for key, value in {
                "tenant_id": workspace.tenant_id,
                "workspace_id": workspace.workspace_id,
                "rule": run.rule,
                "scope": run.scope_key,
                "run_id": run.id,
                "attempt": str(run.attempts),
            }.items()
        },
    }


@contextlib.asynccontextmanager
async def langfuse_run(reaction: Reaction[Any]) -> AsyncGenerator[None]:
    """Propagate a run attempt's trace attributes to Langfuse; a ``RunContext`` for the reactor.

    The attributes are set on every span in the attempt, so Langfuse files the trace under its
    session and user, with its tags and metadata.
    """
    with propagate_attributes(**run_attributes(reaction)):
        yield


def _limit(value: str) -> str:
    """Make a value acceptable to Langfuse: ASCII, and at most 200 characters."""
    return value.encode("ascii", "replace").decode("ascii")[:MAX_ATTRIBUTE]
