"""Rules: a condition, a scope, the action to run, and how to run it.

A rule is data, so it can be listed, stored, diffed, validated and written by an agent::

    error_spike = Rule(
        name="error-spike",
        when=on(ServiceError).where(F.severity >= 7).count(at_least=3, within=minute),
        scope=by(F.service),
        then=run("triage"),
    )

Actions are code, so a rule refers to its action by name. The runtime is given the actions, and
:meth:`Rule.check` confirms at startup that every name, type, field and predicate a rule uses
exists.
"""

import hashlib
import json
from collections.abc import Collection, Mapping
from datetime import datetime, timedelta
from typing import Any, Literal, Protocol

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, JsonValue

from reflexr.core.conditions import (
    AllFilter,
    AnyFilter,
    Condition,
    FieldRef,
    Filter,
    NotFilter,
    OnFilter,
    PredicateFilter,
    SequencePattern,
    WhereFilter,
    has_field,
    resolve_field,
)
from reflexr.core.errors import InvalidRule
from reflexr.core.events import SYSTEM_EVENTS, Event
from reflexr.core.ids import RuleName, ScopeKey

_RULE_NAME = r"^[a-z0-9][a-z0-9._-]*$"


class Scope(BaseModel):
    """The event fields that partition a rule's state and ordering. None: the whole workspace."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    fields: tuple[str, ...] = ()

    def of(self, data: Mapping[str, Any]) -> tuple[ScopeKey, dict[str, JsonValue]] | None:
        """Return the scope key and values of an event's data, or None if a field is missing."""
        values: dict[str, JsonValue] = {}
        for path in self.fields:
            found, value = resolve_field(data, path)
            if not found:
                return None
            values[path] = value
        return scope_key(list(values.values())), values


def by(*fields: FieldRef | str) -> Scope:
    """Scope a rule by event fields: ``by(F.service)``, ``by(F.service, F.region)``."""
    return Scope(fields=tuple(str(f) for f in fields))


def scope_key(values: list[JsonValue]) -> ScopeKey:
    """Return the canonical key of scope values: their JSON, with sorted object keys."""
    return json.dumps(values, sort_keys=True, separators=(",", ":"))


class Named(Protocol):
    """Anything with a name, such as an action."""

    @property
    def name(self) -> str:
        """The name a rule refers to it by."""
        ...


class ActionRef(BaseModel):
    """The action a rule runs, by name."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    action: str = Field(min_length=1)


def run(action: str | Named) -> ActionRef:
    """Refer to an action by its name, or by the action itself: ``run(triage)``."""
    return ActionRef(action=action if isinstance(action, str) else action.name)


class RetryPolicy(BaseModel):
    """How a failed run is retried: exponential backoff, up to ``max_attempts`` attempts."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    max_attempts: int = Field(default=5, ge=1)
    backoff: timedelta = Field(default=timedelta(seconds=1), gt=timedelta(0))
    """The delay after the first failure."""

    multiplier: float = Field(default=2.0, ge=1.0)
    max_backoff: timedelta = Field(default=timedelta(minutes=5), gt=timedelta(0))

    def delay(self, attempts: int) -> timedelta:
        """Return the delay after the ``attempts``-th failed attempt."""
        seconds = self.backoff.total_seconds() * self.multiplier ** max(attempts - 1, 0)
        return min(timedelta(seconds=seconds), self.max_backoff)

    def next_attempt(self, attempts: int, now: AwareDatetime) -> datetime | None:
        """Return when to try again after ``attempts`` failed attempts, or None to give up."""
        if attempts >= self.max_attempts:
            return None
        return now + self.delay(attempts)


class Rule(BaseModel):
    """When ``when`` holds for a scope, run ``then``."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    name: RuleName = Field(pattern=_RULE_NAME, max_length=100)
    description: str = ""
    when: Condition
    scope: Scope = Scope()
    then: ActionRef
    retry: RetryPolicy = RetryPolicy()
    ordering: Literal["scope", "none"] = "scope"
    """``scope``: runs of one scope execute in firing order. ``none``: in parallel."""

    on_dead_letter: Literal["continue", "block"] = "continue"
    """Whether later runs of a scope continue past a dead-lettered run, or wait for an operator."""

    start: Literal["now", "beginning"] = "now"
    """Where a rule starts in a workspace whose log already has envelopes."""

    timeout: timedelta | None = Field(default=None, gt=timedelta(0))
    """How long one attempt of the action may take before it is cancelled and counts as failed."""

    enabled: bool = True

    def definition(self) -> str:
        """Return a hash of what the rule decides (its condition and scope).

        When it changes, the rule's state no longer applies and is reset. Changes to the action,
        retries or ordering do not reset it.
        """
        decided = {"when": self.when.model_dump(mode="json"), "scope": self.scope.fields}
        canonical = json.dumps(decided, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()[:16]

    def check(
        self,
        *,
        events: Mapping[str, type[Event]],
        actions: Collection[str],
        predicates: Collection[str] = (),
    ) -> None:
        """Confirm that everything the rule refers to exists.

        Args:
            events: The event types the rule may watch, by name. reflexr's own events are
                always available.
            actions: The names of the registered actions.
            predicates: The names of the registered predicates.

        Raises:
            InvalidRule: Listing every problem found.
        """
        known = {**{t.event_type: t for t in SYSTEM_EVENTS}, **events}
        problems: list[str] = []
        _check_filter(self.when.filter, known, predicates, problems)
        if isinstance(self.when.pattern, SequencePattern):
            for step in self.when.pattern.steps:
                _check_filter(step, known, predicates, problems)
        admitted = _admitted(self.when.filter, known)
        fields = [*self.scope.fields, *(self.when.dedupe.key if self.when.dedupe else ())]
        problems.extend(_missing_fields(fields, admitted))
        if self.then.action not in actions:
            problems.append(f"no action {self.then.action!r}")
        if problems:
            raise InvalidRule(self.name, problems)


def _check_filter(
    filter: Filter,
    known: Mapping[str, type[Event]],
    predicates: Collection[str],
    problems: list[str],
) -> None:
    problems.extend(f"no event type {n!r}" for n in _names(filter, OnFilter) if n not in known)
    problems.extend(
        f"no predicate {n!r}" for n in _names(filter, PredicateFilter) if n not in predicates
    )
    problems.extend(_missing_fields(_names(filter, WhereFilter), _admitted(filter, known)))


def _names(filter: Filter, kind: type[OnFilter | WhereFilter | PredicateFilter]) -> list[str]:
    match filter:
        case OnFilter(types=types) if kind is OnFilter:
            return list(types)
        case WhereFilter(field=path) if kind is WhereFilter:
            return [path]
        case PredicateFilter(name=name) if kind is PredicateFilter:
            return [name]
        case AllFilter(of=of) | AnyFilter(of=of):
            return [name for f in of for name in _names(f, kind)]
        case NotFilter(filter=inner):
            return _names(inner, kind)
        case _:
            return []


def _admitted(filter: Filter, known: Mapping[str, type[Event]]) -> list[type[Event]]:
    return [known[name] for name in dict.fromkeys(_names(filter, OnFilter)) if name in known]


def _missing_fields(paths: list[str], admitted: list[type[Event]]) -> list[str]:
    return [
        f"no field {path!r} on {event_type.event_type}"
        for path in dict.fromkeys(paths)
        for event_type in admitted
        if not has_field(event_type, path)
    ]
