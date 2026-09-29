"""Evaluating rules: pure functions from a rule's state and new envelopes to its decisions.

The host asks which scopes a batch of envelopes needs, loads them, evaluates, and saves the
result with the new cursor in one transaction::

    scopes = needs(rule, progress, batch, predicates=predicates)
    evaluation = evaluate(rule, progress, load(scopes), batch, predicates=predicates)
    save(evaluation)

Nothing here reads a clock: time is the envelopes' ``ts``, and every envelope moves it forward,
including envelopes the rule's filter rejects.
"""

import re
from collections.abc import Callable, Mapping, Sequence
from functools import lru_cache
from types import MappingProxyType
from typing import Any, assert_never

from reflexr.core.conditions import (
    AbsencePattern,
    AllFilter,
    AnyFilter,
    Condition,
    CountPattern,
    EachPattern,
    Filter,
    NotFilter,
    OnFilter,
    Pattern,
    PredicateFilter,
    SequencePattern,
    WhereFilter,
    resolve_field,
)
from reflexr.core.events import Envelope, Event, RuleErrored, RuleFired, RuleReset, about_rule
from reflexr.core.ids import ScopeKey, firing_id
from reflexr.core.rules import Rule, scope_key
from reflexr.core.state import (
    AbsenceState,
    CountState,
    DedupeState,
    EachState,
    Evaluation,
    EvaluationError,
    Fact,
    Firing,
    Match,
    PatternState,
    RuleProgress,
    ScopeState,
    SequenceState,
    ThrottleState,
)

type Predicates = Mapping[str, Callable[[Event], bool]]
"""Registered Python predicates, by name. They must be pure."""

NO_PREDICATES: Predicates = MappingProxyType({})


class NotLoaded(LookupError):
    """The host did not load a scope that :func:`needs` asked for."""


def begin(rule: Rule, *, head_seq: int) -> RuleProgress:
    """Return where a rule starts in a workspace whose log is at ``head_seq``."""
    return RuleProgress(cursor=head_seq if rule.start == "now" else 0, definition=rule.definition())


def reset(
    rule: Rule,
    progress: RuleProgress,
    *,
    from_seq: int,
    replayed: bool = False,
    silent_through: int = 0,
) -> tuple[RuleProgress, RuleReset]:
    """Reset a rule's state to evaluate again from ``from_seq`` onwards.

    The host deletes the rule's scope states along with saving the new progress. Resetting is
    how a changed definition takes effect, and how a rule is replayed.

    Args:
        rule: The rule, as it is now.
        progress: Its current progress.
        from_seq: Evaluate again from the envelope after this ``seq``.
        replayed: Whether an operator asked for the reset, rather than a changed definition.
        silent_through: Rebuild state without recording firings or errors up to this
            ``seq``, typically the head of the log, so a replay recomputes state without
            acting on the past again.
    """
    generation = progress.generation + 1
    event = RuleReset(
        rule=rule.name,
        generation=generation,
        reason="replayed" if replayed else "changed",
        from_seq=from_seq,
        silent_through=silent_through,
    )
    restarted = RuleProgress(
        cursor=from_seq,
        generation=generation,
        definition=rule.definition(),
        silent_through=silent_through,
    )
    return restarted, event


def needs(
    rule: Rule,
    progress: RuleProgress,
    envelopes: Sequence[Envelope],
    *,
    predicates: Predicates = NO_PREDICATES,
) -> frozenset[ScopeKey]:
    """Return the scopes whose state evaluating ``envelopes`` reads.

    These are the scopes of envelopes that pass the rule's filter, and the scopes whose
    ``absence`` deadline passes during the batch. Scopes the host has no state for are new.
    """
    keys: set[ScopeKey] = set()
    for envelope in envelopes:
        if about_rule(envelope.event) == rule.name:
            continue
        try:
            if not matches(rule.when.filter, envelope, predicates):
                continue
        except Exception:  # decided in evaluate(), as an evaluation error
            continue
        scoped = rule.scope.of(envelope.data)
        if scoped is not None:
            keys.add(scoped[0])
    if envelopes:
        last = envelopes[-1].ts
        keys.update(key for key, due in progress.deadlines.items() if due <= last)
    return frozenset(keys)


def evaluate(
    rule: Rule,
    progress: RuleProgress,
    states: Mapping[ScopeKey, ScopeState],
    envelopes: Sequence[Envelope],
    *,
    predicates: Predicates = NO_PREDICATES,
    max_depth: int | None = None,
) -> Evaluation:
    """Fold ``envelopes`` into a rule's state and decide its firings.

    Args:
        rule: The rule.
        progress: Its progress, whose ``definition`` must be the rule's.
        states: The state of the scopes :func:`needs` returned that exist.
        envelopes: New envelopes, in ``seq`` order, after ``progress.cursor``.
        predicates: The registered predicates.
        max_depth: The deepest causal chain a firing may extend. A firing whose facts would
            be deeper is refused and recorded as an evaluation error, so rules that trigger
            each other stop. ``None`` sets no limit.

    Raises:
        ValueError: If the progress belongs to another definition, or the envelopes are out of
            order or already evaluated.
        NotLoaded: If a scope whose deadline passes was not loaded.
    """
    if progress.definition != rule.definition():
        raise ValueError(f"rule {rule.name!r} changed; reset it before evaluating")
    run = _Run(rule, progress, states, predicates, max_depth)
    for envelope in envelopes:
        run.step(envelope)
    return run.result()


def matches(filter: Filter, envelope: Envelope, predicates: Predicates) -> bool:
    """Whether an envelope passes a filter. Predicates may raise."""
    match filter:
        case OnFilter(types=types):
            return envelope.event_type in types
        case WhereFilter():
            return _compare(filter, envelope.data)
        case AllFilter(of=of):
            return all(matches(f, envelope, predicates) for f in of)
        case AnyFilter(of=of):
            return any(matches(f, envelope, predicates) for f in of)
        case NotFilter(filter=inner):
            return not matches(inner, envelope, predicates)
        case PredicateFilter(name=name):
            return predicates[name](envelope.event)
        case _:
            assert_never(filter)


class _Run:
    """One call of :func:`evaluate`: the working state while folding a batch."""

    def __init__(
        self,
        rule: Rule,
        progress: RuleProgress,
        states: Mapping[ScopeKey, ScopeState],
        predicates: Predicates,
        max_depth: int | None,
    ) -> None:
        self.rule = rule
        self.max_depth = max_depth
        self.condition: Condition = rule.when
        self.progress = progress
        self.predicates = predicates
        self.states = dict(states)
        self.changed: dict[ScopeKey, ScopeState] = {}
        self.deadlines = dict(progress.deadlines)
        self.cursor = progress.cursor
        self.firings: list[Firing] = []
        self.errors: list[EvaluationError] = []
        self.facts: list[Fact] = []

    def step(self, envelope: Envelope) -> None:
        if envelope.seq <= self.cursor:
            raise ValueError(f"envelope {envelope.seq} is not after the cursor {self.cursor}")
        self.cursor = envelope.seq
        self._expire(envelope)
        if about_rule(envelope.event) == self.rule.name:
            return  # a rule never reacts to reflexr's facts about itself
        try:
            passed = matches(self.condition.filter, envelope, self.predicates)
        except Exception as error:
            self._error(envelope, f"the filter raised {type(error).__name__}: {error}")
            return
        if not passed:
            return
        scoped = self.rule.scope.of(envelope.data)
        if scoped is None:
            missing = ", ".join(self.rule.scope.fields)
            self._error(envelope, f"the event lacks the scope fields: {missing}")
            return
        key, values = scoped
        state = self.states.get(key) or _initial(self.condition, values)
        match = Match(
            seq=envelope.seq,
            ts=envelope.ts,
            correlation_id=envelope.correlation_id,
            depth=envelope.depth,
        )
        if self.condition.dedupe is not None:
            state, duplicate = _dedupe(self.condition, state, envelope)
            if duplicate:
                self._save(key, state)
                return
        try:
            state, matched = _pattern(
                self.condition.pattern, state, match, envelope, self.predicates
            )
        except Exception as error:
            self._error(envelope, f"a sequence step raised {type(error).__name__}: {error}")
            return
        if isinstance(self.condition.pattern, AbsencePattern):
            self.deadlines[key] = envelope.ts + self.condition.pattern.within
        if matched:
            state = self._fire(key, state, matched, envelope)
        self._save(key, state)

    def _expire(self, envelope: Envelope) -> None:
        due = sorted(
            ((at, key) for key, at in self.deadlines.items() if at <= envelope.ts),
        )
        for _, key in due:
            if key not in self.states:
                raise NotLoaded(f"scope {key} of rule {self.rule.name!r} was not loaded")
            del self.deadlines[key]
            state = self.states[key]
            # Only an absence pattern sets deadlines, and only after it has seen an envelope.
            pattern = state.pattern
            assert isinstance(pattern, AbsenceState)
            assert pattern.last is not None
            state = self._fire(key, state, (pattern.last,), envelope)
            self._save(key, state)

    def _fire(
        self, key: ScopeKey, state: ScopeState, matched: tuple[Match, ...], envelope: Envelope
    ) -> ScopeState:
        depth = max(m.depth for m in matched)
        if self.max_depth is not None and depth + 1 > self.max_depth:
            limit = self.max_depth
            self._error(
                envelope,
                f"the firing would be at causation depth {depth + 1}, beyond the limit of {limit}",
            )
            return state
        throttle = self.condition.throttle
        if throttle is not None:
            recent = tuple(
                t
                for t in (state.throttle or ThrottleState()).fired
                if envelope.ts - t < throttle.per
            )
            if len(recent) >= throttle.at_most:
                return state.model_copy(update={"throttle": ThrottleState(fired=recent)})
            state = state.model_copy(
                update={"throttle": ThrottleState(fired=(*recent, envelope.ts))}
            )
        if self._silent(envelope):
            return state  # a rebuild: the state advances as it did, and nothing is recorded
        fid = firing_id(self.rule.name, self.progress.generation, key, envelope.seq)
        firing = Firing(
            id=fid,
            rule=self.rule.name,
            scope_key=key,
            scope=state.scope,
            seq=envelope.seq,
            at=envelope.ts,
            matched=tuple(m.seq for m in matched),
            depth=depth,
            correlation_id=max(matched, key=lambda m: m.seq).correlation_id,
        )
        self.firings.append(firing)
        fired = RuleFired(
            rule=self.rule.name,
            scope=state.scope,
            scope_key=key,
            firing_id=fid,
            matched=firing.matched,
        )
        self.facts.append(
            Fact(event=fired, correlation_id=firing.correlation_id, causation=firing.causation)
        )
        return state

    def _silent(self, envelope: Envelope) -> bool:
        return envelope.seq <= self.progress.silent_through

    def _error(self, envelope: Envelope, message: str) -> None:
        if self._silent(envelope):
            return
        self.errors.append(EvaluationError(rule=self.rule.name, seq=envelope.seq, error=message))
        if isinstance(envelope.event, RuleErrored):
            return  # dead-lettered, but not a new fact: errors cannot feed on each other
        errored = RuleErrored(rule=self.rule.name, seq=envelope.seq, error=message)
        self.facts.append(
            Fact(
                event=errored,
                correlation_id=envelope.correlation_id,
                causation=envelope.causation,
            )
        )

    def _save(self, key: ScopeKey, state: ScopeState) -> None:
        self.states[key] = state
        self.changed[key] = state

    def result(self) -> Evaluation:
        return Evaluation(
            progress=self.progress.model_copy(
                update={"cursor": self.cursor, "deadlines": self.deadlines}
            ),
            states=self.changed,
            firings=tuple(self.firings),
            errors=tuple(self.errors),
            facts=tuple(self.facts),
        )


def _initial(condition: Condition, scope: dict[str, Any]) -> ScopeState:
    return ScopeState(
        scope=scope,
        dedupe=DedupeState() if condition.dedupe else None,
        pattern=_initial_pattern(condition.pattern),
        throttle=ThrottleState() if condition.throttle else None,
    )


def _initial_pattern(pattern: Pattern) -> PatternState:
    match pattern:
        case EachPattern():
            return EachState()
        case CountPattern():
            return CountState()
        case SequencePattern():
            return SequenceState()
        case AbsencePattern():
            return AbsenceState()
        case _:
            assert_never(pattern)


def _dedupe(condition: Condition, state: ScopeState, envelope: Envelope) -> tuple[ScopeState, bool]:
    dedupe = condition.dedupe
    assert dedupe is not None
    now = envelope.ts
    seen = {
        k: t for k, t in (state.dedupe or DedupeState()).seen.items() if now - t < dedupe.within
    }
    key = scope_key([resolve_field(envelope.data, path)[1] for path in dedupe.key])
    duplicate = key in seen
    if not duplicate:
        seen[key] = now
    return state.model_copy(update={"dedupe": DedupeState(seen=seen)}), duplicate


def _pattern(
    pattern: Pattern,
    state: ScopeState,
    match: Match,
    envelope: Envelope,
    predicates: Predicates,
) -> tuple[ScopeState, tuple[Match, ...]]:
    """Advance a pattern by one envelope; return the new state and the matches that fire."""
    match pattern:
        case EachPattern():
            return state, (match,)
        case CountPattern(at_least=at_least, within=within):
            current = state.pattern if isinstance(state.pattern, CountState) else CountState()
            counted = tuple(m for m in (*current.matched, match) if match.ts - m.ts < within)
            if len(counted) >= at_least:
                return _with_pattern(state, CountState()), counted
            return _with_pattern(state, CountState(matched=counted)), ()
        case SequencePattern(steps=steps, within=within):
            current = state.pattern if isinstance(state.pattern, SequenceState) else SequenceState()
            done = current.matched
            if done and match.ts - done[0].ts >= within:
                done = ()
            if matches(steps[len(done)], envelope, predicates):
                done = (*done, match)
            elif done and matches(steps[0], envelope, predicates):
                done = (match,)
            if len(done) == len(steps):
                return _with_pattern(state, SequenceState()), done
            return _with_pattern(state, SequenceState(matched=done)), ()
        case AbsencePattern():
            return _with_pattern(state, AbsenceState(last=match)), ()
        case _:
            assert_never(pattern)


def _with_pattern(state: ScopeState, pattern: PatternState) -> ScopeState:
    return state.model_copy(update={"pattern": pattern})


def _compare(where: WhereFilter, data: Mapping[str, Any]) -> bool:
    found, value = resolve_field(data, where.field)
    if where.op == "exists":
        return found is where.value
    if not found:
        return False
    expected = where.value
    match where.op:
        case "eq":
            return value == expected
        case "ne":
            return value != expected
        case "in":
            return isinstance(expected, list) and value in expected
        case "contains":
            if isinstance(value, list):
                return expected in value
            return isinstance(value, str) and isinstance(expected, str) and expected in value
        case "matches":
            return (
                isinstance(value, str)
                and isinstance(expected, str)
                and _regex(expected).search(value) is not None
            )
        case _:  # lt, le, gt and ge
            return _ordered(where.op, value, expected)


def _ordered(op: str, value: Any, expected: Any) -> bool:
    numbers = _is_number(value) and _is_number(expected)
    strings = isinstance(value, str) and isinstance(expected, str)
    if not (numbers or strings):
        return False
    match op:
        case "lt":
            return value < expected
        case "le":
            return value <= expected
        case "gt":
            return value > expected
        case _:
            return value >= expected


def _is_number(value: Any) -> bool:
    return isinstance(value, int | float) and not isinstance(value, bool)


@lru_cache(maxsize=256)
def _regex(pattern: str) -> re.Pattern[str]:
    return re.compile(pattern)
