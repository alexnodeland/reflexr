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

1. [ ] Build evalr (evalr RFC-0001), then reflexr's `[evals]` extra (RFC-0002 phase B5).
