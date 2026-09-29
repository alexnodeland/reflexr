"""Feedback as scores: the mapping, the mirror that follows the log, and its ports (ADR-0025).

Evaluation backends see feedback as scores, one per field. This package maps feedback types to
scores and score configs, and mirrors a workspace's feedback to a :class:`ScoreSink`, following
its log. The sink and the :class:`ScoreConfigStore` are ports; ``reflexr.langfuse`` adapts
Langfuse to them::

    mirror = FeedbackMirror(workspace, sink)
    task = asyncio.create_task(mirror.follow())

It matches artifactr's ``artifactr.scores``, with runs, firings and causal chains as targets.
"""

from reflexr.scores.mapping import (
    MAX_TEXT,
    ScoreConfig,
    ScoreDataType,
    score_configs,
    score_values,
)
from reflexr.scores.mirror import FIRING_SEARCH, FeedbackMirror, sync_score_configs
from reflexr.scores.ports import Score, ScoreConfigStore, ScoreSink

__all__ = [
    "FIRING_SEARCH",
    "MAX_TEXT",
    "FeedbackMirror",
    "Score",
    "ScoreConfig",
    "ScoreConfigStore",
    "ScoreDataType",
    "ScoreSink",
    "score_configs",
    "score_values",
    "sync_score_configs",
]
