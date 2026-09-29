"""The tools :class:`~reflexr.agent.EventContext` gives an agent."""

import json
from collections.abc import Sequence
from typing import Any

from pydantic import ValidationError
from pydantic_ai import FunctionToolset, ModelRetry, RunContext

from reflexr.agent.render import envelope_text
from reflexr.core import Event, Rejection
from reflexr.workspace import Reaction

_DEFAULT_READ = 20
"""How many envelopes ``read_events`` returns when the model gives neither ``limit`` nor
``last``."""

# Tools annotate their context as RunContext[...] literally: pydantic-ai detects context-taking
# tools by that annotation, and a type alias would hide it.


def event_tools(
    emit: Sequence[type[Event]], *, read_limit: int, max_event_chars: int, max_retries: int
) -> FunctionToolset[Reaction[Any]]:
    """Build the tools for reading the log and, if any types are allowed, emitting events.

    Args:
        emit: The event types the agent may publish; their JSON Schemas are described to the
            model once, so tool definitions never change between requests.
        read_limit: The most envelopes one ``read_events`` call returns.
        max_event_chars: How much of each event ``read_events`` shows.
        max_retries: How many times the model may retry a tool call that was refused, such
            as an event whose fields do not match its type.
    """
    allowed = {t.event_type: t for t in emit}
    toolset = FunctionToolset[Reaction[Any]](max_retries=max_retries)

    @toolset.tool
    async def read_events(
        ctx: RunContext[Reaction[Any]],
        after_seq: int = 0,
        before_seq: int | None = None,
        types: list[str] | None = None,
        limit: int | None = None,
        last: int | None = None,
    ) -> str:
        """Read envelopes from the workspace's log, oldest first.

        Args:
            after_seq: Only envelopes after this position in the log.
            before_seq: Only envelopes before this position in the log.
            types: Only these event types.
            limit: The most envelopes to return, from the start. Without it or ``last``, 20.
            last: The most envelopes to return, from the end: the latest ones. To read further
                back, call again with ``before_seq`` set to the oldest position returned.
        """
        if limit is None and last is None:
            limit = _DEFAULT_READ
        try:
            found = await ctx.deps.workspace.read(
                after_seq=after_seq,
                before_seq=before_seq,
                types=types,
                limit=None if limit is None else min(limit, read_limit),
                last=None if last is None else min(last, read_limit),
            )
        except Rejection as rejection:
            raise ModelRetry(rejection.message) from rejection
        if not found:
            return "No events."
        return "\n".join(envelope_text(e, max_chars=max_event_chars) for e in found)

    if not allowed:
        return toolset
    schemas = "\n".join(
        f"- {name}: {json.dumps(t.model_json_schema(), separators=(',', ':'))}"
        for name, t in allowed.items()
    )

    @toolset.tool(
        description="Publish an event into the workspace's log, as a consequence of this "
        f"firing. The fields must match its type's JSON Schema:\n{schemas}"
    )
    async def emit_event(ctx: RunContext[Reaction[Any]], type: str, fields: dict[str, Any]) -> str:
        """Publish an event.

        Args:
            type: The event type.
            fields: The event's fields.
        """
        event_type = allowed.get(type)
        if event_type is None:
            raise ModelRetry(f"You may not emit {type!r}; allowed: {', '.join(allowed)}.")
        try:
            event = event_type.model_validate(fields)
        except ValidationError as error:
            raise ModelRetry(f"The fields do not match {type}: {error}") from error
        published = await ctx.deps.emit(event)
        envelope = published.envelope
        return f"Published {type} at seq {envelope.seq}."

    return toolset
