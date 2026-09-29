# ADR-0020: evalr, a shared eval kit

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Both artifactr and reflexr need the same evaluation machinery:

- datasets built from typed feedback and synced to Langfuse and Hugging Face
- offline judges trained on those datasets
- experiments that score new agents, prompts and models
- end-to-end measures

Two kinds of evaluator must be honored equally:

- **DSPy judges**, optimized with GEPA (DSPy's reflective, text-feedback optimizer)
- **TypeSafe's Jev**, a "System One" model that returns typed decisions (choices, scores, yes/no) with calibrated probabilities

Writing this twice would drift. Putting it in core would saddle every user with DSPy, Langfuse and Hugging Face.

## Decision

- **evalr** is a separate library in its own repository, with its own RFC and ADRs. It is generic over Pydantic types: an evaluator predicts an instance of a feedback type, and evalr does not import artifactr or reflexr.
- reflexr depends on evalr only through an **`[evals]` extra**, which adds the reflexr-specific parts:
  - datasets from workspace logs
  - experiment tasks that replay a firing's run
  - **evaluators as rules**: an evaluator is an action, so a rule on finished runs judges them online
  - the end-to-end measures for workflows (resolution, dead-letter and retry rates, operator intervention, time to resolution)
- The core library keeps only what evaluation needs from it: typed feedback, the evaluator actor, and trace links. artifactr made the same decision ([artifactr ADR-0029](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0029-evalr-shared-eval-kit.md)).

### Amendment (2026-09-28): the first part of the extra

- **`LogFeedbackSource`** is evalr's `FeedbackSource` over a workspace's log. Each piece of one feedback type becomes an example: the verdict is the feedback, and the input is built by the application from what the feedback is about. For a run or a firing, that is the run with its matched and emitted events; for a chain, the chain's envelopes. Example ids are the feedback's event ids, and run examples carry the trace of the run's latest attempt.
- **`EvaluatorAction`** runs any evalr `Evaluator` as a rule's action. Its verdict is given as feedback by an `EvaluatorActor` with the evaluator's name and version, so people's and evaluators' judgements of one target compare directly. An evaluator that hands off records nothing.
- **Until evalr is published**, the extra resolves it from GitHub at a pinned revision (`[tool.uv.sources]`), and bumps the pin in its own pull requests. Experiment tasks that replay a firing's run, and the end-to-end workflow measures, come next.

### Amendment (2026-09-29): datasets leave evaluators' verdicts out

- **`LogFeedbackSource` skips feedback an `EvaluatorActor` gave,** unless `include_evaluators=True`. It yielded every piece of the type, so a dataset collected from a workspace where an `EvaluatorAction` judges runs held the judge's own verdicts as labels, and training or calibrating the judge on it was circular. Comparing evaluators with each other or with people asks for them explicitly, and `given_by` tells the examples apart.

### Amendment (2026-09-29): the score mirror uses evalr too

reflexr no longer depends on evalr only through `[evals]`. The score mapping and ports moved to evalr ([ADR-0025](0025-ports-and-adapters.md), as amended), so `reflexr.scores` and `reflexr.langfuse` import it, and the `[langfuse]` extra depends on evalr. The core, telemetry and workspace still never import it. The git pin in `[tool.uv.sources]` serves both extras and moves in its own pull requests, as before.

## Options considered

| Option | Duplication | Dependencies for users who don't evaluate | Works without the other library |
|---|---|---|---|
| **A shared kit behind an extra (chosen)** | None | None | Yes |
| Everything in each library | Two copies of the adapters | None | Yes |
| All of it in the combined system | None | None | No: neither library can be evaluated alone |

## Consequences

- Easier: one implementation of judges, Jev evaluators, datasets and experiments, used by both libraries and the combined system.
- Harder: three packages whose conventions must stay aligned; the shared-conventions table in RFC-0002 is the reference.

## Action items

1. [x] Build evalr (evalr RFC-0001), then reflexr's `[evals]` extra (RFC-0002 phase B5): feedback sources and evaluators as rules.
2. [x] The end-to-end workflow measures: `rule_outcomes` (dead-letter, retry and intervention rates per rule) and `time_to_resolution` per chain, from the log.
3. [x] Experiment tasks: `replay_task` replays an example's events against a candidate action in an isolated in-memory workspace, as an evalr `Task`.
