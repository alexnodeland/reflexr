"""Feedback as scores: the mirror that follows the log, on evalr's mapping and ports (ADR-0045).

Evaluation backends see feedback as scores, one per field. evalr owns how a field becomes a
score, the ports scores leave through (``evalr.core.ScoreSink`` for scores and
``evalr.core.ScoreConfigStore`` for score configs), and their Langfuse adapters
(``evalr.langfuse``). This package names each feedback type's scores as the type is
registered, and mirrors a workspace's feedback to a sink, following its log::

    mirror = FeedbackMirror(workspace, sink, cursor="langfuse")
    task = asyncio.create_task(mirror.follow())  # after the cursor it saves in the workspace

It matches artifactr's ``artifactr.scores``, with runs, firings and causal chains as targets. It
needs evalr, which the ``langfuse`` and ``evals`` extras install.
"""

from reflexr.scores.mapping import score_configs, score_values
from reflexr.scores.mirror import FIRING_SEARCH, FeedbackMirror, sync_score_configs

__all__ = [
    "FIRING_SEARCH",
    "FeedbackMirror",
    "score_configs",
    "score_values",
    "sync_score_configs",
]
