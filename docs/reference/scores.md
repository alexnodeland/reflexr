# reflexr.scores

Feedback as scores: the mirror, on evalr's mapping and ports. See [Feedback and evaluation](../guides/evaluation.md#scores) and [ADR-0025](../adr/0025-ports-and-adapters.md). It needs evalr, which the `langfuse` and `evals` extras install.

::: reflexr.scores
    options:
      members: false
      show_root_heading: false
      show_root_toc_entry: false

## The mirror

::: reflexr.scores.FeedbackMirror

::: reflexr.scores.sync_score_configs

::: reflexr.scores.FIRING_SEARCH

## Feedback types as scores

Each is evalr's function, with the feedback type's registered name as the `{type}` in its scores' names.

::: reflexr.scores.score_configs

::: reflexr.scores.score_values

## evalr's ports and values

`reflexr.scores` re-exports these from `evalr.core`, where they are documented:

| Name | In evalr |
|---|---|
| `Score` | [`evalr.core.Score`](https://evalr.alexnodeland.com/reference/core/#evalr.core.Score) |
| `ScoreSink` | [`evalr.core.ScoreSink`](https://evalr.alexnodeland.com/reference/core/#evalr.core.ScoreSink) |
| `ScoreConfig` | [`evalr.core.ScoreConfig`](https://evalr.alexnodeland.com/reference/core/#evalr.core.ScoreConfig) |
| `ScoreConfigStore` | [`evalr.core.ScoreConfigStore`](https://evalr.alexnodeland.com/reference/core/#evalr.core.ScoreConfigStore) |
| `ScoreDataType` | [`evalr.core.ScoreType`](https://evalr.alexnodeland.com/reference/core/#evalr.core.ScoreType) |
| `MAX_TEXT` | [`evalr.core.MAX_TEXT`](https://evalr.alexnodeland.com/reference/core/#evalr.core.MAX_TEXT) |
