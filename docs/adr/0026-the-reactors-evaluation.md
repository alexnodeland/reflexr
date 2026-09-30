# ADR-0026: The reactor's evaluation: rules on workspaces, the depth of reflexr's facts, and rebuilds

**Status:** Accepted
**Date:** 2026-09-28
**Deciders:** Alex Nodeland
**Amends:** [ADR-0010](0010-loop-and-spend-safety.md)

## Context

Building the reactor's evaluation (RFC-0001 phase 2c) settled four questions the design left open:

1. **Where rules are registered.** Replaying a rule, listing rules and a rule's progress are workspace operations that need the rules' definitions. Executing runs needs the actions.
2. **The depth of reflexr's own facts.** ADR-0010 bounds loops by rejecting a run's events beyond a depth limit. Rules can also watch reflexr's facts (`rule_fired`, `run_succeeded`), and if those facts sat at depth 0, two rules could trigger each other through them forever. No event a run publishes would ever be rejected.
3. **What a replay does.** Replaying from the start could rebuild a rule's state quietly or act on the past again.
4. **Where a rule starts.** "Now" was decided at a rule's first evaluation. Since the reactor only discovers workspaces with a log, a new workspace's first events came before its first evaluation, and `start="now"` rules skipped them.

## Decision

1. **Rules and predicates are registered on `Workspaces`; actions on the reactor.** `Workspaces(storage, events=..., rules=[...], predicates={...})` checks every rule's event types, fields and predicates at construction, and rejects duplicate names. The reactor checks the actions.
2. **Every fact about a firing or run is one step deeper than what the firing matched,** the same depth as the events its run emits: `Firing.causation` and `Run.causation`. `evaluate(max_depth=)` refuses a firing whose facts would exceed the limit. The refusal is recorded as an evaluation error, dead-lettered for the rule, so every cycle through firings is bounded. An error keeps the depth and chain of the envelope it is about. An error while evaluating another rule's `rule_errored` is dead-lettered, but not appended as a new fact, so errors cannot feed on each other.
3. **Replays rebuild by default.** `replay_rule(rule, from_seq=, mode=)` resets the rule. In `"rebuild"` mode, `RuleProgress.silent_through` is set to the head, so the state is recomputed without recording firings or errors. `"refire"` records them again, as new runs with new ids. `RuleReset` records `silent_through`, so the log says what a replay did.
4. **A new workspace meets every rule at its first event.** The first append to a workspace starts every registered rule at seq 0. A rule added later starts at the head if `start="now"`, or at the beginning if `start="beginning"`. A rule whose definition changed starts afresh at the head; replaying is how to reprocess history.

## Options considered

### Depth of facts: one step deeper, with refusal in core (chosen)

| Dimension | Assessment |
|---|---|
| Safety | Every cycle through a firing adds a step, and core refuses past the limit |
| Complexity | Low: two properties and one check, pinned by conformance cases |

### Depth of facts: facts at depth 0

**Cons:** two rules watching each other's `rule_fired` loop forever at constant depth.

### Depth of facts: reject facts beyond the limit on append

**Cons:** facts are reflexr's own record and must never fail to append; the run's outcome would go unrecorded.

### Replay: always refire

**Cons:** replaying after a fix would page everyone again for the past.

## Consequences

- Easier: surfaces list rules and replay them through the same `Workspaces`, and the reactor is only a runtime.
- Easier: loops through reflexr's own facts are bounded, and the refusal is visible as a dead letter.
- Harder: a refused firing does nothing but record an error. Operators see it in dead letters and the `reflexr.rule.errors` metric.

## Action items

1. [x] Core: `Firing.causation`, `Run.causation`, `evaluate(max_depth=)`, `RuleProgress.silent_through`, and `safety.json` and `rebuilds.json` conformance cases.
2. [x] `Workspaces(rules=, predicates=)`, `Workspace.replay_rule`, and `Reactor.evaluate`.
3. [x] Execution with leases, and actions (phase 2c, second part).
