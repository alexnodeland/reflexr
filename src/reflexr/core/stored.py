"""Stored rules: the configuration that allows them, and the limits every one is held to.

A stored rule is an ordinary :class:`~reflexr.core.Rule`, installed in one workspace at runtime
rather than registered in code (RFC-0003). What it may do is fixed: the application's
:class:`StoredRules` names the actions and rule namespaces it may use, and constants bound
everything else, the same for every tenant. :func:`check_stored` lists what a rule breaks, so a
draft can be checked before anyone proposes it.
"""

import json
import math
from collections.abc import Awaitable, Callable, Mapping, Set
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from pydantic import BaseModel, ConfigDict, JsonValue
from pydantic_core import to_jsonable_python

from reflexr.core.actors import Actor
from reflexr.core.conditions import (
    EachPattern,
    Filter,
    PredicateFilter,
    SequencePattern,
    WhereFilter,
    walk,
)
from reflexr.core.errors import InvalidRule
from reflexr.core.ids import RuleName, TenantId, WorkspaceId
from reflexr.core.rules import Rule, load_params

MAX_STORED_FIRINGS_PER_HOUR = 60
"""The most firings a stored rule's throttle may allow in an hour."""

MAX_STORED_ATTEMPTS = 5
"""The most attempts a stored rule's retry policy may make of a run."""

MAX_STORED_TIMEOUT = timedelta(minutes=5)
"""The longest timeout a stored rule may give its action."""

MAX_STORED_WINDOW = timedelta(days=1)
"""The longest window a stored rule may use: a pattern's, a dedupe's, or a throttle's period."""

MAX_STORED_DESCRIPTION = 2000
"""The most characters a stored rule's description may have."""

MAX_STORED_BYTES = 16 * 1024
"""The most bytes a stored rule's JSON may have."""

_FIXED: dict[str, JsonValue] = {
    "ordering": "none",
    "on_dead_letter": "continue",
    "start": "now",
    "enabled": True,
}
"""The policies every stored rule has: it starts at the head, runs unordered, and never waits
on a dead letter. Archiving is the only way to stop it."""

_HOUR = timedelta(hours=1)


class RuleChange(BaseModel):
    """A change to a stored rule, which the configuration's ``allow`` hook allows or refuses."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["install", "update", "archive"]
    """Which command makes the change: ``install_rule``, ``update_rule`` or ``archive_rule``."""

    rule: RuleName
    """The stored rule's name."""

    spec: Rule | None = None
    """The rule as installed or updated. None when archiving."""


@dataclass(frozen=True)
class StoredRules:
    """What stored rules may do: who may change them, the actions they run, their namespaces.

    Everything else is fixed by core's limits, the same for every tenant.

    Args:
        allow: Decides whether an actor may make a change to a workspace's stored rules. Shaped
            like :data:`~reflexr.workspace.Authorize`, with the change, and asked after it.
        actions: The only actions stored rules may run, by name, each mapped to the params
            model it declares, or to None if it declares none.
        namespaces: The rule namespaces stored rules may be named in, such as ``{"chat"}``.

    Raises:
        ValueError: If ``namespaces`` includes ``reflexr``, which is reserved.
    """

    allow: Callable[[TenantId, WorkspaceId, Actor, RuleChange], Awaitable[bool]]
    actions: Mapping[str, type[BaseModel] | None]
    namespaces: Set[str]

    def __post_init__(self) -> None:
        if "reflexr" in self.namespaces:
            raise ValueError("the reflexr namespace is reserved")


def check_stored(rule: Rule, config: StoredRules) -> list[str]:
    """Return every problem with a rule as a stored rule, under ``config`` and core's limits.

    It is pure, beside :meth:`Rule.check`, so a draft can be checked with both before it is
    proposed. The problems follow the rule's fields, each once, and an empty list means the
    rule may be stored.
    """
    namespace = rule.name.partition(":")[0]
    problems: list[str] = []
    if namespace not in config.namespaces:
        problems.append(f"the namespace {namespace!r} is not one stored rules may use")
    if len(rule.description) > MAX_STORED_DESCRIPTION:
        problems.append(f"description must be at most {MAX_STORED_DESCRIPTION} characters")
    problems.extend(_condition_problems(rule))
    if rule.scope.fields:
        problems.append("scope.fields must be empty")
    problems.extend(_action_problems(rule, config))
    if rule.retry.max_attempts > MAX_STORED_ATTEMPTS:
        problems.append(f"retry.max_attempts must be at most {MAX_STORED_ATTEMPTS}")
    if rule.timeout is None:
        problems.append("timeout is required")
    elif rule.timeout > MAX_STORED_TIMEOUT:
        problems.append(f"timeout must be at most {to_jsonable_python(MAX_STORED_TIMEOUT)}")
    held = rule.model_dump(mode="json", include=set(_FIXED))
    problems.extend(
        f"{name} must be {json.dumps(value)}"
        for name, value in _FIXED.items()
        if held[name] != value
    )
    if len(rule.model_dump_json().encode()) > MAX_STORED_BYTES:
        problems.append(f"the rule's JSON must be at most {MAX_STORED_BYTES} bytes")
    return list(dict.fromkeys(problems))


def _condition_problems(rule: Rule) -> list[str]:
    """What a stored rule's condition may not have, stage by stage."""
    condition = rule.when
    problems = _filter_problems(condition.filter)
    if condition.dedupe is not None:
        problems.extend(_window_problems("when.dedupe.within", condition.dedupe.within))
    pattern = condition.pattern
    if isinstance(pattern, SequencePattern):
        for step in pattern.steps:
            problems.extend(_filter_problems(step))
    if not isinstance(pattern, EachPattern):
        problems.extend(_window_problems("when.pattern.within", pattern.within))
    throttle = condition.throttle
    if throttle is None:
        problems.append("when.throttle is required")
    else:
        problems.extend(_window_problems("when.throttle.per", throttle.per))
        # at_most firings in any period of per, so that many in each period an hour spans.
        if throttle.at_most * math.ceil(_HOUR / throttle.per) > MAX_STORED_FIRINGS_PER_HOUR:
            limit = MAX_STORED_FIRINGS_PER_HOUR
            problems.append(f"when.throttle must allow at most {limit} firings in any hour")
    return problems


def _filter_problems(filter: Filter) -> list[str]:
    """The predicates and ``matches`` a filter may not have."""
    filters = list(walk(filter))
    problems = [
        f"the predicate {f.name!r} is not allowed"
        for f in filters
        if isinstance(f, PredicateFilter)
    ]
    if any(isinstance(f, WhereFilter) and f.op == "matches" for f in filters):
        problems.append("the 'matches' operator is not allowed")
    return problems


def _window_problems(path: str, window: timedelta) -> list[str]:
    if window > MAX_STORED_WINDOW:
        return [f"{path} must be at most {to_jsonable_python(MAX_STORED_WINDOW)}"]
    return []


def _action_problems(rule: Rule, config: StoredRules) -> list[str]:
    """Whether a stored rule's action is allowed, and its params validate as its model."""
    action = rule.then.action
    if action not in config.actions:
        return [f"the action {action!r} is not one stored rules may run"]
    try:
        load_params(rule, config.actions[action])
    except InvalidRule as invalid:
        return invalid.problems
    return []
