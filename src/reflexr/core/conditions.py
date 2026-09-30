"""Conditions: what a rule watches for, as typed, serializable data.

A condition is a pipeline of stages, evaluated per scope:

- **filter** (stateless): which envelopes the rule considers
- **dedupe** (optional): drop an envelope whose key was seen within a window
- **pattern** (stateful): when the considered envelopes make the rule fire
- **throttle** (optional): at most so many firings per period

The fluent builder produces the models::

    on(ServiceError).where(F.severity >= 7).count(at_least=3, within=timedelta(minutes=1))

Field references (``F.severity``, ``F.labels.env``) name fields of the event, and are checked
when the condition is built against the event types that can reach them: those given to the
:func:`on` in their own conjunction, or else to the condition's.
"""

import re
from collections.abc import Iterator, Mapping
from datetime import timedelta
from typing import Annotated, Any, Literal, Self, cast, get_args, get_origin

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator

from reflexr.core.events import DEFAULT_REGISTRY, Event, EventName

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

type Admitted = tuple[str, ...] | None
"""The event types, by name, that can reach a point in a filter. None: any type can."""


class _Stage(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


# ─── filters ─────────────────────────────────────────────────────────────────


class OnFilter(_Stage):
    """Envelopes whose event is one of ``types``."""

    kind: Literal["on"] = "on"
    types: tuple[EventName, ...] = Field(min_length=1)


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
        current = self.filter.of if isinstance(self.filter, AllFilter) else (self.filter,)
        narrowed = AllFilter(of=(*current, *added))
        for path, types in where_fields(narrowed):
            _check_field(path, types)
        return self._with(filter=narrowed)

    def distinct(self, *key: FieldRef | str, within: timedelta) -> "Condition":
        """Drop an envelope whose ``key`` fields were already seen within ``within``."""
        paths = tuple(str(k) for k in key)
        for path in paths:
            _check_field(path, admitted(self.filter))
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


def _check_field(path: str, types: Admitted) -> None:
    missing = [
        name
        for name in types or ()
        if (event_type := DEFAULT_REGISTRY.get(name)) is not None
        and not has_field(event_type, path)
    ]
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


def walk(filter: Filter) -> Iterator[Filter]:
    """Yield a filter and every filter inside it, depth first."""
    yield filter
    if isinstance(filter, AllFilter | AnyFilter):
        for inner in filter.of:
            yield from walk(inner)
    elif isinstance(filter, NotFilter):
        yield from walk(filter.filter)


def admitted(filter: Filter, within: Admitted = None) -> Admitted:
    """Return the event types an envelope that passes ``filter`` can have, by name, in order.

    ``within`` is what the filter's context already admits. ``on`` narrows it to its types;
    ``all`` narrows it by each of its filters, ``any`` admits what any of its filters admits,
    and ``not`` removes the types its filter accepts whatever their fields. ``where`` and
    predicates decide by fields, so they leave it as it is.
    """
    match filter:
        case OnFilter(types=types):
            names = tuple(dict.fromkeys(types))
            return names if within is None else tuple(n for n in within if n in names)
        case AllFilter(of=of):
            # A ``not`` removes types from what the rest admit, so it applies last.
            for inner in sorted(of, key=lambda f: isinstance(f, NotFilter)):
                within = admitted(inner, within)
            return within
        case AnyFilter(of=of):
            found: list[str] = []
            for inner in of:
                names = admitted(inner, within)
                if names is None:
                    return None
                found.extend(names)
            return tuple(dict.fromkeys(found))
        case NotFilter(filter=inner):
            excluded = _accepted(inner)
            return None if within is None else tuple(n for n in within if n not in excluded)
        case _:
            return within


def where_fields(filter: Filter, within: Admitted = None) -> list[tuple[str, Admitted]]:
    """Return the field each ``where`` in a filter compares, with the types it is compared on.

    A ``where`` is compared on the types its own conjunction admits: inside ``all``, what its
    ``on`` siblings admit, and with none, what the enclosing context admits. So
    ``on(Deploy) | on(ServiceError).where(F.severity >= 7)`` compares ``severity`` on service
    errors only.
    """
    match filter:
        case WhereFilter(field=path):
            return [(path, within)]
        case AllFilter(of=of):
            narrowed = admitted(filter, within)
            return [found for inner in of for found in where_fields(inner, narrowed)]
        case AnyFilter(of=of):
            return [found for inner in of for found in where_fields(inner, within)]
        case NotFilter(filter=inner):
            return where_fields(inner, within)
        case _:
            return []


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


def _accepted(filter: Filter) -> frozenset[str]:
    """Return the event types a filter accepts whatever their fields."""
    match filter:
        case OnFilter(types=types):
            return frozenset(types)
        case AnyFilter(of=of):
            return frozenset(name for inner in of for name in _accepted(inner))
        case AllFilter(of=of):
            first, *rest = (_accepted(inner) for inner in of)
            return first.intersection(*rest)
        case _:
            return frozenset()
