"""Which spans make up reflexr's traces, and work that makes none (ADR-0040).

:data:`TRACE_SCOPES` names the instrumentation scopes whose spans belong in reflexr's traces;
``reflexr.otel.telemetry()`` offers them to Langfuse's span filter. :func:`untraced` runs
polling without a trace per poll.
"""

import contextlib
import random
from collections.abc import Generator
from typing import Final

from opentelemetry import context, trace
from opentelemetry.trace import NonRecordingSpan, SpanContext

from reflexr.telemetry.metrics import SCOPE

TRACE_SCOPES: Final = frozenset(
    {
        SCOPE,
        "pydantic-graph",
        "mcp-python-sdk",
        "opentelemetry.instrumentation.fastapi",
        "opentelemetry.instrumentation.asgi",
        "opentelemetry.instrumentation.sqlalchemy",
        "opentelemetry.instrumentation.asyncpg",
        "opentelemetry.instrumentation.httpx",
    }
)
"""The instrumentation scopes whose spans make up reflexr's traces, besides the model calls:
reflexr's own, pydantic-graph's, the MCP SDK's, and the FastAPI, ASGI, SQLAlchemy, asyncpg
and httpx instrumentations'."""


def is_trace_scope(scope: str) -> bool:
    """Return whether spans of an instrumentation scope belong in reflexr's traces.

    A sub-scope of one of :data:`TRACE_SCOPES`, such as ``reflexr.workspace``, does too.
    """
    return any(scope == name or scope.startswith(f"{name}.") for name in TRACE_SCOPES)


@contextlib.contextmanager
def untraced() -> Generator[None]:
    """Run a block untraced: every span started in it is a child of a span that is never sampled.

    Under a parent-based sampler, the SDK's default, those spans are not recorded, so nothing
    started in the block is exported: the database queries of a poll, say. Metrics recorded in
    the block are recorded as usual, the instrumentations' own included. reflexr polls
    storage this way, in the reactor, subscriptions and the feedback mirror, so an idle
    application sends no traces; the work a poll finds, such as an evaluation or a run
    attempt, is traced where it happens.

    Don't commit or publish in the block: its trace ids are the unsampled parent's, so an
    envelope or a run would record a trace that does not exist. A sampler that ignores the
    parent, such as ``always_on`` or ``traceidratio``, records the block's spans again, each
    poll in a trace of its own.
    """
    parent = SpanContext(
        trace_id=random.getrandbits(128) | 1,
        span_id=random.getrandbits(64) | 1,
        is_remote=False,
    )
    token = context.attach(trace.set_span_in_context(NonRecordingSpan(parent)))
    try:
        yield
    finally:
        context.detach(token)
