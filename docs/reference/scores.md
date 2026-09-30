# reflexr.scores

Feedback as scores: the mirror, on evalr's mapping and ports. See [Feedback and evaluation](../guides/evaluation.md#scores) and [ADR-0045](../adr/0045-scores-on-evalr.md). It needs evalr, which the `langfuse` and `evals` extras install.

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

`Score`, `ScoreConfig`, `ScoreSink`, `ScoreConfigStore`, `ScoreDataType` and `MAX_TEXT` are evalr's: import them from `evalr.core` ([evalr's reference](https://evalr.alexnodeland.com/reference/core/)).
