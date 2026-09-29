"""Feedback as scores: the mirror that follows the log, on evalr's mapping and ports (ADR-0025).

Evaluation backends see feedback as scores, one per field. evalr owns how a field becomes a score,
and the ports scores leave through: a :class:`ScoreSink` for scores and a
:class:`ScoreConfigStore` for score configs. This package names each feedback type's scores as
the type is registered, and mirrors a workspace's feedback to a sink, following its log;
``reflexr.langfuse`` adapts Langfuse to the ports::

    mirror = FeedbackMirror(workspace, sink)
    task = asyncio.create_task(mirror.follow())

It matches artifactr's ``artifactr.scores``, with runs, firings and causal chains as targets. It
needs evalr, which the ``langfuse`` and ``evals`` extras install. ``Score``, ``ScoreConfig``,
``ScoreSink``, ``ScoreConfigStore``, ``ScoreDataType`` (evalr's ``ScoreType``) and ``MAX_TEXT``
are evalr's, re-exported here, where they have always been.
"""

from evalr.core import MAX_TEXT, Score, ScoreConfig, ScoreConfigStore, ScoreSink
from evalr.core import ScoreType as ScoreDataType

from reflexr.scores.mapping import score_configs, score_values
from reflexr.scores.mirror import FIRING_SEARCH, FeedbackMirror, sync_score_configs

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
