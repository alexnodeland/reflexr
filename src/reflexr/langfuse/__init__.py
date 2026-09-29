"""Langfuse for reflexr: the ``[langfuse]`` extra (ADR-0018).

An adapter (ADR-0025) that files reflexr's traces and feedback in Langfuse, as artifactr's does:

- :func:`should_export_span` keeps whole traces, not only their LLM spans, and :func:`no_spans`
  none, when a Collector sends Langfuse the traces
- :func:`langfuse_run` is a ``RunContext`` for the reactor that sets each run's session (its
  causal chain), user, trace name (its rule), tags and metadata
- :class:`LangfuseScores` and :class:`LangfuseScoreConfigs` implement evalr's score ports, so
  a ``FeedbackMirror`` records feedback as Langfuse scores

::

    langfuse = langfuse_client(tracer_provider=tracer_provider)
    reactor = Reactor(workspaces, actions=actions, run_context=langfuse_run)
    await sync_score_configs(LangfuseScoreConfigs(langfuse))
    mirror = FeedbackMirror(workspace, LangfuseScores(langfuse), cursor="langfuse")
"""

from reflexr.langfuse.client import langfuse_client
from reflexr.langfuse.scores import LangfuseScoreConfigs, LangfuseScores
from reflexr.langfuse.tracing import (
    MAX_ATTRIBUTE,
    langfuse_run,
    no_spans,
    run_attributes,
    should_export_span,
)

__all__ = [
    "MAX_ATTRIBUTE",
    "LangfuseScoreConfigs",
    "LangfuseScores",
    "langfuse_client",
    "langfuse_run",
    "no_spans",
    "run_attributes",
    "should_export_span",
]
