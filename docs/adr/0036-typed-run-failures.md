# ADR-0036: Typed run failures

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

RFC-0002 wants a guardrail block to fail a run with a `guardrail_blocked` reason, which the rule's retry policy treats as permanent: retrying the same input would be blocked again. The workspace layer must not know about LiteLLM ([ADR-0025](0025-ports-and-adapters.md)), so the mechanism has to be general. artifactr made the same change for its runs ([artifactr ADR-0042](https://github.com/alexnodeland/artifactr/blob/main/docs/adr/0042-typed-run-failures.md)); reflexr's runs are retried, so it adds permanence.

## Decision

- **`RunFailure(message, reason=, permanent=False)`**, raised by an action, a tool or a capability, fails the attempt with a stable reason code. Other exceptions fail it without one, as before.
- **A permanent failure is dead-lettered at once**, whatever the rule's retry policy allows. `core.fail(..., reason=, permanent=)` decides it.
- **The reason is recorded**: on the run (`Run.reason`, cleared when it succeeds), on `run_retrying` and `run_dead_lettered` (an additive protocol change), and on `reflexr.runs` as `reflexr.run.reason`, bounded because reasons are codes. reflexr's own reasons are `timeout` and `abandoned`.

## Consequences

- Easier: the `[litellm]` extra turns a guardrail block into a permanent `guardrail_blocked` failure without the reactor knowing about gateways.
- Easier: dashboards break failures down by reason.
- Harder: actions must keep reasons to a small set of codes, or the metric's cardinality grows.
