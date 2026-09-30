# ADR-0045: Scores on evalr

**Status:** Accepted
**Date:** 2026-09-29
**Deciders:** Alex Nodeland

## Context

Evaluation backends see feedback as scores, one per field. evalr holds the mapping from a field to a score, the ports scores leave through, and the one Langfuse `ScoreSink` and `ScoreConfigStore`, for evaluators and the libraries' mirrors alike; its `Score` checks its own value ([evalr ADR-0012](https://github.com/alexnodeland/evalr/blob/main/docs/adr/0012-langfuse-score-adapters-in-evalr.md)). This record states where scores live, superseding [ADR-0025](0025-ports-and-adapters.md)'s amendment, the last amendment of [ADR-0020](0020-evalr-shared-eval-kit.md), and the part of [ADR-0029](0029-metric-detail-through-sdk-views.md) that keeps the score adapters in `reflexr.langfuse`.

## Decision

- **evalr owns everything about a score but the mirror:**
  - the mapping, `evalr.core.score_configs` and `score_values`
  - the ports, `ScoreSink` and `ScoreConfigStore`
  - the values, `Score`, `ScoreConfig`, `ScoreDataType` and `MAX_TEXT`; a `Score` refuses a value not of its data type, and a span without a trace
  - their Langfuse adapters, `evalr.langfuse.LangfuseScoreSink` and `LangfuseScoreConfigStore`
- **reflexr keeps the mirror,** since only it reads reflexr's log. `FeedbackMirror` knows which trace or session a run's, a firing's or a chain's feedback belongs on, and derives score ids in reflexr's own namespace, so mirroring again replaces scores. `score_configs` and `score_values` pass each feedback type's registered name as the `{type}`, and `sync_score_configs(store, types=None)` creates the missing configs through evalr's.
- **A score has a trace or a session, never both, and never a span.** Feedback judges a trace, through a run or the evaluation of a firing, or a session, through a chain.
- **Applications use evalr's names from evalr.** `reflexr.scores` no longer re-exports them, and `reflexr.langfuse` no longer has score adapters: it files traces and runs, and evalr's adapters file the scores.
- **evalr is needed for scores.** The `[langfuse]` extra depends on `evalr[langfuse]` and `[evals]` on evalr, pinned by git revision until evalr is published ([ADR-0020](0020-evalr-shared-eval-kit.md)). The inner layers never import evalr; `tests/test_layering.py` lets only `reflexr.scores`, which may import only `evalr.core`, and `reflexr.evals` import it.

artifactr makes the same change to `artifactr.scores` and `artifactr.langfuse`.

## Options considered

| Option | Assessment |
|---|---|
| **evalr's adapters, used from evalr (chosen)** | One sink and one store for every project, checked by one run of evalr's contract suites |
| An adapter per project (ADR-0025's amendment) | Three sinks that behave differently for the same scores, maintained apart |
| evalr's adapters, re-exported by the library | One implementation, but two names for it, and a list to keep in step with evalr's |
| The mirror in evalr too | One copy of everything, but evalr would read each library's log |

## Consequences

- Easier: people's and evaluators' scores reach Langfuse through the same code.
- Easier: a score whose value is not of its type fails where it is made, not in a sink.
- Harder: a fix to the adapters reaches reflexr when the evalr pin moves.
- Harder: an application imports scores' types from evalr and the mirror from reflexr.

## Action items

1. [x] Use evalr's Langfuse adapters, pinned at `7a290123`, and delete `reflexr.langfuse`'s.
2. [x] Drop `reflexr.scores`' re-exports of evalr's names.
