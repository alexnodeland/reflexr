"""Conditions: what a rule watches for, as typed, serializable data.

A condition is a pipeline of stages, evaluated per scope:

- **filter** (stateless): which envelopes the rule considers
- **dedupe** (optional): drop an envelope whose key was seen within a window
- **pattern** (stateful): when the considered envelopes make the rule fire
- **throttle** (optional): at most so many firings per period

The fluent builder produces the models::

    on(ServiceError).where(F.severity >= 7).count(at_least=3, within=timedelta(minutes=1))

Field references (``F.severity``, ``F.labels.env``) name fields of the event, and are checked
against the event types given to :func:`on` when the condition is built.
"""

import re
from collections.abc import Mapping
from datetime import timedelta
from typing import Annotated, Any, Literal, Self, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from reflexr.core.events import Event, event_types

Op = Literal["eq", "ne", "lt", "le", "gt", "ge", "in", "contains", "matches", "exists"]
"""How a :class:`WhereFilter` compares a field with its value."""

_FIELD_PATH = r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$"

type Filter = Annotated[
    OnFilter | WhereFilter | AllFilter | AnyFilter | NotFilter | PredicateFilter,
    Field(discriminator="kind"),
]
"""Which envelopes a rule considers."""

type Pattern = Annotated[
    EachPattern | CountPattern | SequencePattern | AbsencePattern,
    Field(discriminator="kind"),
]
"""When the envelopes a rule considers make it fire."""


class _Stage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# ─── filters ─────────────────────────────────────────────────────────────────


class OnFilter(_Stage):
    """Envelopes whose event is one of ``types``."""

    kind: Literal["on"] = "on"
    types: tuple[str, ...] = Field(min_length=1)


class WhereFilter(_Stage):
    """Envelopes whose event field ``field`` compares with ``value`` by ``op``.

    A missing field matches nothing, except ``exists`` with ``value=False``.
    """

    kind: Literal["where"] = "where"
    field: str = Field(pattern=_FIELD_PATH)
    """A dotted path into the event's fields, such as ``labels.env``."""

    op: Op
    value: JsonValue = None

    @model_validator(mode="after")
    def _check_value(self) -> Self:
        if self.op == "in" and not isinstance(self.value, list):
            raise ValueError("'in' compares with a list of values")
        if self.op == "matches":
            if not isinstance(self.value, str):
                raise ValueError("'matches' compares with a regular expression")
            try:
                re.compile(self.value)
            except re.error as error:
                raise ValueError(f"invalid regular expression: {error}") from error
        if self.op == "exists" and not isinstance(self.value, bool):
            raise ValueError("'exists' compares with true or false")
        return self


class AllFilter(_Stage):
    """Envelopes that match every filter in ``of``."""

    kind: Literal["all"] = "all"
    of: tuple[Filter, ...] = Field(min_length=1)


class AnyFilter(_Stage):
    """Envelopes that match at least one filter in ``of``."""

    kind: Literal["any"] = "any"
    of: tuple[Filter, ...] = Field(min_length=1)


class NotFilter(_Stage):
    """Envelopes that do not match ``filter``."""

    kind: Literal["not"] = "not"
    filter: Filter


class PredicateFilter(_Stage):
    """Envelopes whose event the registered Python predicate ``name`` accepts.

    An escape hatch for what the other filters cannot express. Predicates must be pure.
    """

    kind: Literal["predicate"] = "predicate"
    name: str = Field(min_length=1)


# ─── stages around the pattern ───────────────────────────────────────────────


class Dedupe(_Stage):
    """Drop an envelope whose ``key`` fields were already seen within ``within``."""

    key: tuple[str, ...] = Field(min_length=1)
    within: timedelta = Field(gt=timedelta(0))


class Throttle(_Stage):
    """Allow at most ``at_most`` firings per scope in any period of ``per``."""

    at_most: int = Field(ge=1)
    per: timedelta = Field(gt=timedelta(0))


# ─── patterns ────────────────────────────────────────────────────────────────


class EachPattern(_Stage):
    """Fire for every envelope that passes the filter."""

    kind: Literal["each"] = "each"


class CountPattern(_Stage):
    """Fire when ``at_least`` envelopes pass within ``within``. A firing consumes them."""

    kind: Literal["count"] = "count"
    at_least: int = Field(ge=1)
    within: timedelta = Field(gt=timedelta(0))


class SequencePattern(_Stage):
    """Fire when envelopes match ``steps`` in order, all within ``within`` of the first.

    An envelope that matches the first step while a sequence is in progress (but not the step
    it is waiting for) starts the sequence again from it, so the latest start counts.
    """

    kind: Literal["sequence"] = "sequence"
    steps: tuple[Filter, ...] = Field(min_length=2)
    within: timedelta = Field(gt=timedelta(0))


class AbsencePattern(_Stage):
    """Fire once when nothing has passed the filter for ``within`` since the last envelope did.

    Only scopes that have been seen can go quiet. Time passes with every envelope in the log,
    whether or not it passes the filter.
    """

    kind: Literal["absence"] = "absence"
    within: timedelta = Field(gt=timedelta(0))


# ─── field references ────────────────────────────────────────────────────────


class FieldRef:
    """A reference to an event field, built from :data:`F`: ``F.severity``, ``F.labels.env``.

    Comparisons build :class:`WhereFilter` s. Equality is spelled :meth:`eq`, because ``==`` has
    to return a bool; ``where(service="auth")`` is the shorthand.
    """

    __slots__ = ("_path",)

    def __init__(self, path: tuple[str, ...]) -> None:
        self._path = path

    def __getattr__(self, name: str) -> "FieldRef":
        if name.startswith("_"):
            raise AttributeError(name)
        return FieldRef((*self._path, name))

    def __str__(self) -> str:
        return ".".join(self._path)

    def __repr__(self) -> str:
        return f"F.{self}"

    def __lt__(self, value: JsonValue) -> WhereFilter:
        return self._where("lt", value)

    def __le__(self, value: JsonValue) -> WhereFilter:
        return self._where("le", value)

    def __gt__(self, value: JsonValue) -> WhereFilter:
        return self._where("gt", value)

    def __ge__(self, value: JsonValue) -> WhereFilter:
        return self._where("ge", value)

    def eq(self, value: JsonValue) -> WhereFilter:
        """The field equals ``value``."""
        return self._where("eq", value)

    def ne(self, value: JsonValue) -> WhereFilter:
        """The field exists and does not equal ``value``."""
        return self._where("ne", value)

    def is_in(self, values: list[JsonValue]) -> WhereFilter:
        """The field equals one of ``values``."""
        return self._where("in", values)

    def contains(self, value: JsonValue) -> WhereFilter:
        """The field is a list containing ``value``, or a string containing it."""
        return self._where("contains", value)

    def matches(self, pattern: str) -> WhereFilter:
        """The field is a string that the regular expression ``pattern`` searches."""
        return self._where("matches", pattern)

    def exists(self, exists: bool = True) -> WhereFilter:
        """The field is present (or, with ``False``, absent)."""
        return self._where("exists", exists)

    def _where(self, op: Op, value: JsonValue) -> WhereFilter:
        return WhereFilter(field=str(self), op=op, value=value)


class _FieldRoot:
    __slots__ = ()

    def __getattr__(self, name: str) -> FieldRef:
        if name.startswith("_"):
            raise AttributeError(name)
        return FieldRef((name,))


F = _FieldRoot()
"""The root of field references: ``F.severity >= 7``."""


def field(path: str) -> FieldRef:
    """Return a reference to the field at a dotted ``path``, for names ``F`` cannot spell.

    Examples are fields named like :class:`FieldRef` methods (``field("contains")``).
    """
    return FieldRef(tuple(path.split(".")))


# ─── the condition and its builder ───────────────────────────────────────────


class Condition(_Stage):
    """A rule's ``when``: a filter, and optionally dedupe, a pattern and a throttle."""

    filter: Filter
    dedupe: Dedupe | None = None
    pattern: Pattern = EachPattern()
    throttle: Throttle | None = None

    def where(self, *filters: Filter, **equals: JsonValue) -> "Condition":
        """Narrow the filter: every filter given, and every field equal to its keyword."""
        added = [*filters, *(WhereFilter(field=k, op="eq", value=v) for k, v in equals.items())]
        for where in _where_filters(added):
            self._check_field(where.field)
        current = self.filter.of if isinstance(self.filter, AllFilter) else (self.filter,)
        return self._with(filter=AllFilter(of=(*current, *added)))

    def distinct(self, *key: FieldRef | str, within: timedelta) -> "Condition":
        """Drop an envelope whose ``key`` fields were already seen within ``within``."""
        paths = tuple(str(k) for k in key)
        for path in paths:
            self._check_field(path)
        return self._with(dedupe=Dedupe(key=paths, within=within))

    def count(self, *, at_least: int, within: timedelta) -> "Condition":
        """Fire when ``at_least`` envelopes pass within ``within``."""
        return self._with_pattern(CountPattern(at_least=at_least, within=within))

    def absent(self, *, within: timedelta) -> "Condition":
        """Fire when nothing has passed for ``within`` since the last envelope that did."""
        return self._with_pattern(AbsencePattern(within=within))

    def at_most(self, times: int, *, per: timedelta) -> "Condition":
        """Allow at most ``times`` firings per scope in any period of ``per``."""
        return self._with(throttle=Throttle(at_most=times, per=per))

    def _with_pattern(self, pattern: Pattern) -> "Condition":
        if not isinstance(self.pattern, EachPattern):
            raise ValueError(f"the condition already has a {self.pattern.kind} pattern")
        return self._with(pattern=pattern)

    def _with(self, **update: Any) -> "Condition":
        return self.model_copy(update=update)

    def _check_field(self, path: str) -> None:
        missing = [t.event_type for t in admitted_types(self.filter) if not has_field(t, path)]
        if missing:
            raise ValueError(f"no field {path!r} on {', '.join(missing)}")


def on(*types: type[Event] | str) -> Condition:
    """Start a condition on envelopes of the given event types, by class or by name."""
    if not types:
        raise ValueError("on() needs at least one event type")
    names = tuple(t if isinstance(t, str) else t.event_type for t in types)
    return Condition(filter=OnFilter(types=names))


def sequence(*steps: Condition, within: timedelta) -> Condition:
    """Fire when envelopes match ``steps`` in order within ``within``.

    Each step is a filter-only condition, such as ``on(Deploy)`` or
    ``on(ServiceError).where(F.severity >= 7)``.
    """
    for step in steps:
        if step.dedupe or step.throttle or not isinstance(step.pattern, EachPattern):
            raise ValueError("a sequence step is a filter only: on(...) and where(...)")
    filters = tuple(step.filter for step in steps)
    return Condition(
        filter=AnyFilter(of=filters), pattern=SequencePattern(steps=filters, within=within)
    )


def resolve_field(data: Mapping[str, Any], path: str) -> tuple[bool, Any]:
    """Return whether the dotted ``path`` exists in event data, and its value."""
    value: Any = data
    for part in path.split("."):
        if not isinstance(value, Mapping) or part not in value:
            return False, None
        value = cast("Mapping[str, Any]", value)[part]
    return True, value


def admitted_types(filter: Filter) -> list[type[Event]]:
    """Return the registered event classes a filter names with ``on``, in order, once each.

    Names that are not registered are left out: their fields cannot be checked.
    """
    registry = event_types()
    found: list[type[Event]] = []
    for name in _on_names(filter):
        event_type = registry.get(name)
        if event_type is not None and event_type not in found:
            found.append(event_type)
    return found


def has_field(model: type[BaseModel], path: str) -> bool:
    """Whether the dotted ``path`` names a field of ``model``.

    Paths through fields that are not Pydantic models (such as dicts) cannot be checked, and
    are accepted.
    """
    head, _, rest = path.partition(".")
    info = model.model_fields.get(head)
    if info is None:
        return False
    if not rest:
        return True
    nested = _model_of(info.annotation)
    return nested is None or has_field(nested, rest)


def _model_of(annotation: Any) -> type[BaseModel] | None:
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return annotation
    if get_origin(annotation) is not None:
        models = [m for arg in get_args(annotation) if (m := _model_of(arg)) is not None]
        if len(models) == 1:
            return models[0]
    return None


def _on_names(filter: Filter) -> list[str]:
    match filter:
        case OnFilter(types=types):
            return list(types)
        case AllFilter(of=of) | AnyFilter(of=of):
            return [name for f in of for name in _on_names(f)]
        case _:
            return []


def _where_filters(filters: list[Filter]) -> list[WhereFilter]:
    found: list[WhereFilter] = []
    for f in filters:
        match f:
            case WhereFilter():
                found.append(f)
            case AllFilter(of=of) | AnyFilter(of=of):
                found.extend(_where_filters(list(of)))
            case NotFilter(filter=inner):
                found.extend(_where_filters([inner]))
            case _:
                pass
    return found
