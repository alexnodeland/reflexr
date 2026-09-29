# ADR-0019: Typed feedback as events, mirrored to Langfuse

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

People's judgements of workflows (whether a triage was right, whether a rule should have fired, whether an incident was handled well) are the raw material for evaluation, as they are in artifactr ([artifactr ADR-0028](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0028-typed-feedback-as-events.md)). They need to be typed, scoped to what they are about, and usable both in reflexr's log and in Langfuse.

## Decision

- **`Feedback` is a Pydantic base class.** Subclasses register by name and declare the targets they apply to: `run` (a workflow execution), `firing` and `chain` (a causal chain).
- **Feedback is recorded as an event.** `Workspace.give_feedback` validates the type and target and records `feedback_given`, attributed like any publish; every surface exposes it.
- **Evaluators give feedback too.** An evaluator's verdict is an instance of a feedback type, given by a new actor kind, `EvaluatorActor(name, version)`, so human and evaluator judgements share types and can be compared directly.
- **Langfuse gets scores.** The `[langfuse]` extra mirrors `feedback_given` to Langfuse, one score per field (`{type}.{field}`, typed by the field), with score configs generated from the types. Score ids are derived from the event, so mirroring is idempotent.

## Options considered

| Option | Replayable from the log | In Langfuse beside the traces | Typed |
|---|---|---|---|
| **Typed events, mirrored to Langfuse (chosen)** | Yes | Yes | Yes |
| Langfuse scores only | No | Yes | Partly: score configs |
| The log only | Yes | No | Yes |

## Consequences

- Easier: datasets can be rebuilt from any workspace's log, whatever backend was running, and evaluators can run as rules on the same log.
- Easier: agreement between people and evaluators is a query.
- Harder: every feedback type needs thought about its targets and fields, as artifact types do.

## Action items

1. [ ] Implement RFC-0002 phase B2, and the mirror in phase B3.
