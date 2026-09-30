"""Langfuse for reflexr: the ``[langfuse]`` extra (ADR-0018).

An adapter (ADR-0025) that files reflexr's traces in Langfuse, as artifactr's does:

- :func:`should_export_span` keeps whole traces, not only their LLM spans, and :func:`no_spans`
  none, when a Collector sends Langfuse the traces
- :func:`langfuse_run` is a ``RunContext`` for the reactor that sets each run's session (its
  causal chain), user, trace name (its rule), tags and metadata

Feedback reaches Langfuse as scores through evalr's adapters, ``evalr.langfuse``'s
``LangfuseScoreSink`` and ``LangfuseScoreConfigStore`` (ADR-0045)::

    langfuse = langfuse_client(tracer_provider=tracer_provider)
    reactor = Reactor(workspaces, actions=actions, run_context=langfuse_run)
    await sync_score_configs(LangfuseScoreConfigStore(langfuse))
    mirror = FeedbackMirror(workspace, LangfuseScoreSink(langfuse), cursor="langfuse")
"""

from reflexr.langfuse.client import langfuse_client
from reflexr.langfuse.tracing import (
    MAX_ATTRIBUTE,
    langfuse_run,
    no_spans,
    run_attributes,
    should_export_span,
)

__all__ = [
    "MAX_ATTRIBUTE",
    "langfuse_client",
    "langfuse_run",
    "no_spans",
    "run_attributes",
    "should_export_span",
]
