# reflexr.langfuse

The `langfuse` extra. See [Langfuse](../guides/observability.md#langfuse) and [Scores](../guides/evaluation.md#scores).

::: reflexr.langfuse
    options:
      members: false
      show_root_heading: false
      show_root_toc_entry: false

## Traces

::: reflexr.langfuse.langfuse_client

::: reflexr.langfuse.should_export_span

::: reflexr.langfuse.no_spans

## Runs

::: reflexr.langfuse.langfuse_run

::: reflexr.langfuse.run_attributes

::: reflexr.langfuse.MAX_ATTRIBUTE

## Scores

Feedback reaches Langfuse through evalr's adapters, [`evalr.langfuse.LangfuseScoreSink`](https://evalr.alexnodeland.com/reference/langfuse/#evalr.langfuse.LangfuseScoreSink) and [`evalr.langfuse.LangfuseScoreConfigStore`](https://evalr.alexnodeland.com/reference/langfuse/#evalr.langfuse.LangfuseScoreConfigStore), which a [`FeedbackMirror`](scores.md#reflexr.scores.FeedbackMirror) and [`sync_score_configs`](scores.md#reflexr.scores.sync_score_configs) take ([ADR-0045](../adr/0045-scores-on-evalr.md)).
