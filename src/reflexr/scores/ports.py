"""The ports scores leave through: a sink for scores, and a store for score configs (ADR-0025).

Evaluation backends implement them; ``reflexr.langfuse`` adapts Langfuse. Both are small, so
tests and other backends can implement them in a few lines.
"""

from collections.abc import Collection, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from reflexr.core import TraceId
from reflexr.scores.mapping import ScoreConfig, ScoreDataType


@dataclass(frozen=True)
class Score:
    """One field of one piece of feedback, as a score on a trace or a session.

    Args:
        id: A stable id, derived from the feedback's envelope and the field, so sending a
            score again replaces it rather than adding another.
        name: ``{type}.{field}``.
        value: A number for numeric and boolean scores, text for categorical and text ones.
        data_type: How the value is scored.
        trace_id: The trace the feedback is about, when there is one.
        session_id: The session (causal chain) the feedback is about, when there is no trace.
        timestamp: When the feedback was given.
        metadata: Where the feedback came from: tenant, workspace, type, target and actor.
    """

    id: str
    name: str
    value: float | str
    data_type: ScoreDataType
    trace_id: TraceId | None
    session_id: str | None
    timestamp: datetime
    metadata: Mapping[str, str] = field(default_factory=dict[str, str])


class ScoreSink(Protocol):
    """Where mirrored scores go."""

    async def send(self, score: Score) -> None:
        """Record a score. Sending a score with the same ``id`` again must replace it."""
        ...


class ScoreConfigStore(Protocol):
    """Where score configs live, so a backend knows each score's type and range."""

    async def names(self) -> Collection[str]:
        """Return the names of the configs that exist."""
        ...

    async def create(self, config: ScoreConfig) -> None:
        """Create a config."""
        ...
