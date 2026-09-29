# ADR-0017: The core's evaluation contract

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland

## Context

Building `reflexr.core` (RFC-0001 phase 1) settled how a host drives evaluation and several details that [ADR-0006](0006-rules-as-typed-serializable-data.md) and [ADR-0007](0007-rule-state-as-pure-reducers.md) left open:

- how the host knows which scope states to load, when time can reach scopes no envelope in the batch touches
- how rules refer to actions that cannot be serialized, without hidden registration
- how field references spell equality under pyright strict, with no suppressions
- what counting, sequences and absence do after they fire
- how a rule avoids reacting to its own activity

## Decision

- **`needs`, then `evaluate`, like artifactr's `needs` and `commit`.** `needs(rule, progress, batch)` returns the scopes whose envelopes pass the filter plus the scopes whose `absence` deadline passes within the batch. The host loads those that exist, and `evaluate` returns the changed states, the new `RuleProgress` and the facts to append. Deadlines live in the progress, so a quiet scope is found without scanning every scope. A deadline scope the host failed to load raises `NotLoaded`.
- **Batching never changes decisions.** Evaluating a log in one batch or many, with state saved as JSON between batches, decides the same firings with the same ids. The conformance runner checks every case both ways, and property tests check random logs and splits.
- **Firing ids are derived** from the rule, its generation, the scope and the `seq`. `reset` starts a new generation, so replaying never reuses an id.
- **Actions are passed to the runtime explicitly.** `run(action)` records only the name. `Rule.check(events=, actions=, predicates=)` confirms at registration that every name, type and field exists, listing all problems at once. Nothing registers itself behind the scenes.
- **Scopes are built with `by(F.service)`**, so `Rule(...)` type-checks with a single `Scope` type.
- **Equality is `.eq()`, or keywords to `where()`.** Ordering operators build filters directly; overriding `==` would need a type suppression.
- **Pattern semantics:** a count's firing consumes the envelopes it counted; a sequence restarts at a new first step while it waits, so the latest start counts; absence fires once per quiet period and rearms with the next matching envelope. Windows are half-open: envelopes exactly `within` apart are not together.
- **No self-reaction.** A rule never sees reflexr's facts about itself (firings, errors, resets, runs), though they still move its clock.
- **Schedules move to phase 2**, with the cron parser they need; core keeps only the `Tick` event.

## Options considered

| Question | Chosen | Alternative | Why |
|---|---|---|---|
| Loading state | `needs` then `evaluate` | Load every scope of a rule for each batch | Work proportional to the batch, not to all scopes |
| Action registration | Explicit, checked at startup | `run(obj)` registers globally | No hidden global state; rules stay pure data |
| Equality | `.eq()` and keywords | `==` with a type suppression | The gates forbid suppressions, and `==` returning non-bool surprises readers |
| Count after firing | Consume | Keep firing while the count holds | One firing per burst; throttles cover the rest |

## Consequences

- Easier: any host (the workspace layer, a test, a port) drives evaluation with two calls and one transaction.
- Easier: a rule with a mistake fails at startup with every problem listed.
- Harder: applications list their actions when building the reactor, next to their rules.

## Action items

1. [x] Implement the contract, the conformance suite (53 cases) and property tests (RFC-0001 phase 1).
