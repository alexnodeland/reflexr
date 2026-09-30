"""Events, their namespaces and registries, reflexr's own events, and envelopes.

An event type is a Pydantic model that subclasses :class:`Event`, through a base that declares
its owner's namespace. Defining the class registers it under the namespace and a local name,
derived from the class name or given as ``name=``::

    class OncallEvent(Event, abstract=True, event_namespace="oncall"): ...


    class AlertFired(OncallEvent, name="alert.fired"):  # oncall:alert.fired
        service: str
        severity: int

On the wire, and in storage, an event is its fields plus ``"type"``: the qualified name. Stored
events are wrapped in an :class:`Envelope` with their position in the workspace's log.
"""

import re
from collections.abc import Iterator, Mapping
from functools import cached_property
from typing import Annotated, Any, ClassVar, Literal, cast

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    PlainSerializer,
    PlainValidator,
    SerializationInfo,
    ValidationError,
    WithJsonSchema,
)

from reflexr.core.actors import Actor
from reflexr.core.errors import ValidationFailed
from reflexr.core.feedback import FeedbackTarget
from reflexr.core.ids import EventId, FiringId, RuleName, RunId, ScopeKey, WorkspaceId

_NAMESPACE = r"[a-z][a-z0-9_]*"
_LOCAL = r"[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*"
_EVENT_NAME = rf"^{_NAMESPACE}:{_LOCAL}$"
"""A qualified event type name: a namespace, a ``:`` and a local name, such as
``oncall:alert.fired``."""

_REFLEXR = "reflexr"

_types: dict[str, type["Event"]] = {}
"""Every event type in the process, by name. There is one table, so a name parses one way."""

_namespaces: dict[str, type["Event"]] = {}
"""Every namespace in the process, with the base that declared it."""


class EventRegistry(Mapping[str, type["Event"]]):
    """A set of namespaces, read as the event types in them, by name.

    A registry scopes which types a ``Workspaces`` accepts, so applications in one process stay
    apart. It is a view of the process's one table of types: a base's ``registry=`` puts its
    namespace in a registry, and :meth:`add` includes another's. reflexr's own namespace is in
    every registry.
    """

    def __init__(self) -> None:
        self._included = {_REFLEXR}

    def add(self, *bases: type["Event"]) -> None:
        """Include the namespace each base declared, and so every type in it.

        Raises:
            TypeError: If a base declares no namespace.
        """
        for base in bases:
            namespace = base.__dict__.get("event_namespace")
            if not isinstance(namespace, str):
                raise TypeError(
                    f"{base.__qualname__} declares no namespace; "
                    "pass the base that declares one with event_namespace=..."
                )
            self._included.add(namespace)

    def __getitem__(self, name: str) -> type["Event"]:
        event_type = _types[name]
        if event_type.event_namespace not in self._included:
            raise KeyError(name)
        return event_type

    def __iter__(self) -> Iterator[str]:
        return (name for name, t in _types.items() if t.event_namespace in self._included)

    def __len__(self) -> int:
        return sum(1 for _ in self)


DEFAULT_REGISTRY = EventRegistry()
"""The registry of every namespace whose base names no other one."""


def _snake_case(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()


def _qualified(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _declare(base: "type[Event]", namespace: str, registry: EventRegistry) -> None:
    if re.fullmatch(_NAMESPACE, namespace) is None:
        raise TypeError(
            f"{namespace!r} is not a namespace: lowercase letters, digits and underscores, "
            "starting with a letter"
        )
    existing = _namespaces.get(namespace)
    if existing is not None and _qualified(existing) != _qualified(base):
        raise TypeError(f"namespace {namespace!r} is already declared by {_qualified(existing)}")
    _namespaces[namespace] = base
    base.event_namespace = namespace
    registry.add(base)


def _register(event_type: "type[Event]", name: str | None) -> None:
    namespace = event_type.event_namespace
    if namespace is None:
        raise TypeError(
            f"{event_type.__name__} has no namespace; subclass a base that declares one, such as "
            'class OncallEvent(Event, abstract=True, event_namespace="oncall")'
        )
    local = name or _snake_case(event_type.__name__)
    if re.fullmatch(_LOCAL, local) is None:
        raise TypeError(
            f"{event_type.__name__}'s name {local!r} is not a local name: lowercase words joined "
            "by dots, without the namespace, which comes from its base"
        )
    event_type.event_type = f"{namespace}:{local}"
    existing = _types.get(event_type.event_type)
    if existing is not None and _qualified(existing) != _qualified(event_type):
        raise TypeError(
            f"event type name {event_type.event_type!r} is already registered by "
            f"{_qualified(existing)}; pass name=... to choose another"
        )
    _types[event_type.event_type] = event_type


class Event(BaseModel):
    """Base class for event types.

    Subclass it through an abstract base that declares a namespace with ``event_namespace=``,
    and give it ordinary Pydantic fields. Events are facts, so they are immutable, and unknown
    fields are rejected so that a producer's typo fails loudly.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_type: ClassVar[str]
    """The qualified name: the namespace, a ``:``, and the local name, derived from the class
    name unless given as ``name=``."""

    event_namespace: ClassVar[str | None] = None
    """The namespace, from the base that declared it."""

    def __init_subclass__(
        cls,
        *,
        name: str | None = None,
        abstract: bool = False,
        event_namespace: str | None = None,
        registry: EventRegistry | None = None,
        **kwargs: Any,
    ) -> None:
        # The class arguments are consumed in __pydantic_init_subclass__, which runs once the
        # fields exist; they must not reach object.__init_subclass__.
        super().__init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(
        cls,
        *,
        name: str | None = None,
        abstract: bool = False,
        event_namespace: str | None = None,
        registry: EventRegistry | None = None,
        **kwargs: Any,
    ) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if event_namespace is not None:
            if not abstract:
                raise TypeError(f"{cls.__name__} declares a namespace, so it must be abstract=True")
            _declare(cls, event_namespace, DEFAULT_REGISTRY if registry is None else registry)
        elif registry is not None:
            raise TypeError(f"{cls.__name__} passes registry= without declaring a namespace")
        if not abstract:
            _register(cls, name)


class UnknownEvent(Event, abstract=True):
    """An event whose type is not registered in this process, kept as-is so it round-trips.

    Stored envelopes can outlive the code that defined their types, and a newer producer can use
    types this version does not know. Their data is kept in :attr:`model_extra`.
    """

    model_config = ConfigDict(frozen=True, extra="allow")

    unknown_type: str
    """The type name the event was stored with."""


def check_event_name(name: str) -> str:
    """Return ``name`` if it is a qualified event type name.

    Raises:
        ValueError: If it isn't. A name with no namespace is told the qualified names of the
            types with that local name, in whatever registry.
    """
    if re.fullmatch(_EVENT_NAME, name) is not None:
        return name
    if ":" not in name:
        found = sorted(t for t in _types if t.partition(":")[2] == name)
        if len(found) == 1:
            raise ValueError(f"event type {name!r} has no namespace; did you mean {found[0]!r}?")
        if found:
            raise ValueError(
                f"event type {name!r} has no namespace; did you mean one of "
                f"{', '.join(repr(t) for t in found)}?"
            )
        raise ValueError(
            f"event type {name!r} has no namespace; a name is a namespace, a ':' and a local "
            "name, such as 'oncall:alert.fired'"
        )
    raise ValueError(
        f"{name!r} is not an event type name: a namespace, a ':' and a local name, in "
        "lowercase, such as 'oncall:alert.fired'"
    )


EventName = Annotated[
    str,
    AfterValidator(check_event_name),
    WithJsonSchema({"type": "string", "pattern": _EVENT_NAME}),
]
"""A qualified event type name, checked with a hint when it has no namespace."""


def type_of(event: Event) -> str:
    """Return the type name of an event, including one kept as an :class:`UnknownEvent`."""
    if isinstance(event, UnknownEvent):
        return event.unknown_type
    return event.event_type


def load_event(data: Mapping[str, Any]) -> Event:
    """Validate event data, including its ``"type"``, as the registered type it names.

    Data of an unregistered type becomes an :class:`UnknownEvent`.

    Raises:
        ValidationFailed: If there is no ``"type"``, or the data does not match its type.
    """
    name, fields = _split(data)
    if name is None:
        raise ValidationFailed("an event needs a string 'type'", [])
    try:
        return _load(name, fields)
    except ValidationError as error:
        raise ValidationFailed(f"invalid {name} event", _errors(error)) from error


def _split(data: Mapping[str, Any]) -> tuple[str | None, dict[str, Any]]:
    fields = dict(data)
    name = fields.pop("type", None)
    return (name if isinstance(name, str) else None), fields


def _load(name: str, fields: dict[str, Any]) -> Event:
    event_type = _types.get(name)
    if event_type is None:
        fields.pop("unknown_type", None)
        return UnknownEvent(unknown_type=name, **fields)
    return event_type.model_validate(fields)


def dump_event(event: Event, *, mode: Literal["json", "python"] = "json") -> dict[str, Any]:
    """Return an event's data with its ``"type"`` first, as stored and sent on the wire."""
    if isinstance(event, UnknownEvent):
        return {"type": event.unknown_type, **(event.model_extra or {})}
    return {"type": event.event_type, **event.model_dump(mode=mode)}


def _validate_event(value: Any) -> Event:
    if isinstance(value, Event):
        return value
    if not isinstance(value, Mapping):
        raise ValueError("an event must be an object with a 'type'")
    name, fields = _split(cast("Mapping[str, Any]", value))
    if name is None:
        raise ValueError("an event needs a string 'type'")
    try:
        return _load(name, fields)
    except ValidationError as error:
        # Inside a model, a validator must raise ValueError for Pydantic to report it as a
        # validation error of the enclosing model, such as a request body.
        problems = "; ".join(
            f"{'.'.join(str(p) for p in e['loc'])}: {e['msg']}"
            for e in error.errors(include_url=False)
        )
        raise ValueError(f"invalid {name} event: {problems}") from None


def _serialize_event(event: Event, info: SerializationInfo) -> dict[str, Any]:
    return dump_event(event, mode="json" if info.mode_is_json() else "python")


AnyEvent = Annotated[
    Event,
    PlainValidator(_validate_event),
    PlainSerializer(_serialize_event),
    WithJsonSchema(
        {
            "type": "object",
            "properties": {"type": {"type": "string"}},
            "required": ["type"],
            "additionalProperties": True,
        }
    ),
]
"""Any event, validated through the registry by its ``"type"``."""


# ─── reflexr's own events ────────────────────────────────────────────────────


class _ReflexrEvent(Event, abstract=True, event_namespace=_REFLEXR):
    """reflexr's own events, in the ``reflexr`` namespace, which every registry includes."""


class RuleFired(_ReflexrEvent, name="rule_fired"):
    """A rule's condition held for one scope; a run of its action was created."""

    rule: RuleName
    scope: dict[str, JsonValue]
    scope_key: ScopeKey
    firing_id: FiringId
    matched: tuple[int, ...]
    """The ``seq`` of every envelope that made the condition hold."""


class RuleErrored(_ReflexrEvent, name="rule_errored"):
    """A rule could not evaluate one envelope; it is dead-lettered for that rule alone."""

    rule: RuleName
    seq: int
    error: str


class RuleReset(_ReflexrEvent, name="rule_reset"):
    """A rule's state was reset, because its definition changed or it was replayed."""

    rule: RuleName
    generation: int
    reason: Literal["changed", "replayed"]
    from_seq: int
    silent_through: int = 0
    """Envelopes up to this ``seq`` rebuild the rule's state without recording firings."""


class RuleInstalled(_ReflexrEvent, name="rule_installed"):
    """A stored rule was installed or updated: its new version, whole, and where it came from.

    The facts are a stored rule's history: the version that fired a run is the latest one
    before its firing.
    """

    rule: RuleName
    version: int
    spec: dict[str, JsonValue]
    """The rule, as JSON."""

    provenance: dict[str, JsonValue]
    """Where the change came from, as its installer said."""


class RuleArchived(_ReflexrEvent, name="rule_archived"):
    """A stored rule was archived, and its unfinished runs cancelled."""

    rule: RuleName
    version: int
    reason: str | None = None
    cancelled: int
    """How many unfinished runs archiving cancelled."""


class RunStarted(_ReflexrEvent, name="run_started"):
    """An attempt of a run began."""

    run_id: RunId
    rule: RuleName
    scope: dict[str, JsonValue] = {}
    """The values of the rule's scope fields, as on ``reflexr:rule_fired``."""

    scope_key: ScopeKey
    attempt: int


class RunProgressed(_ReflexrEvent, name="run_progressed"):
    """A graph run completed a step and saved a checkpoint."""

    run_id: RunId
    rule: RuleName
    step: str


class RunRetrying(_ReflexrEvent, name="run_retrying"):
    """An attempt failed, and the run will be tried again."""

    run_id: RunId
    rule: RuleName
    attempt: int
    error: str
    next_attempt_at: AwareDatetime
    reason: str | None = None
    """A stable code for why the attempt failed, when the action gave one."""


class RunSucceeded(_ReflexrEvent, name="run_succeeded"):
    """A run finished."""

    run_id: RunId
    rule: RuleName
    output: JsonValue = None


class RunDeadLettered(_ReflexrEvent, name="run_dead_lettered"):
    """A run exhausted its retries, or failed in a way retrying cannot fix."""

    run_id: RunId
    rule: RuleName
    attempts: int
    error: str
    reason: str | None = None
    """A stable code for why the last attempt failed, when the action gave one."""


class RunCancelled(_ReflexrEvent, name="run_cancelled"):
    """A run was cancelled."""

    run_id: RunId
    rule: RuleName
    reason: str | None = None


class RunRequeued(_ReflexrEvent, name="run_requeued"):
    """Someone made a run runnable again. The envelope's actor records who."""

    run_id: RunId
    rule: RuleName


class RunSkipped(_ReflexrEvent, name="run_skipped"):
    """A run was skipped without completing, unblocking its scope."""

    run_id: RunId
    rule: RuleName
    reason: str | None = None


class FeedbackGiven(_ReflexrEvent, name="feedback_given"):
    """A person or an evaluator judged a run, a firing or a causal chain."""

    feedback_type: str
    target: FeedbackTarget
    value: dict[str, JsonValue]
    """The feedback's fields, as validated against its registered type."""


class Tick(_ReflexrEvent, name="tick"):
    """A schedule's tick. Ticks also move rules' clocks forward in quiet workspaces."""

    schedule: str
    at: AwareDatetime


SystemEvent = (
    RuleFired
    | RuleErrored
    | RuleReset
    | RuleInstalled
    | RuleArchived
    | RunStarted
    | RunProgressed
    | RunRetrying
    | RunSucceeded
    | RunDeadLettered
    | RunCancelled
    | RunRequeued
    | RunSkipped
)
"""reflexr's facts about rules and runs. Each names the rule it is about."""

SYSTEM_EVENTS: tuple[type[Event], ...] = (
    RuleFired,
    RuleErrored,
    RuleReset,
    RuleInstalled,
    RuleArchived,
    RunStarted,
    RunProgressed,
    RunRetrying,
    RunSucceeded,
    RunDeadLettered,
    RunCancelled,
    RunRequeued,
    RunSkipped,
    FeedbackGiven,
    Tick,
)
"""Every event type reflexr itself appends."""


# ─── envelopes ───────────────────────────────────────────────────────────────


class Causation(BaseModel):
    """What caused an event that a run emitted, and how deep its causal chain is."""

    model_config = ConfigDict(frozen=True)

    firing_id: FiringId
    run_id: RunId
    depth: int = Field(ge=1)
    """How many runs lie between this event and an event nothing caused."""


class Envelope(BaseModel):
    """A stored event with its position and attribution. Its shape is also the wire shape."""

    model_config = ConfigDict(frozen=True)

    seq: int = Field(ge=1)
    """The event's position in its workspace's log, gap-free from 1."""

    id: EventId
    ts: AwareDatetime
    """When the event was appended. Rules measure time with it; it never decreases."""

    workspace_id: WorkspaceId
    actor: Actor
    causation: Causation | None = None
    correlation_id: str
    """The id of the first event in this event's causal chain."""

    traceparent: str | None = None
    """The W3C trace context of the span that published the event, so the runs it causes can
    link back to it."""

    event: AnyEvent

    @property
    def depth(self) -> int:
        """How many runs lie between this event and an event nothing caused."""
        return self.causation.depth if self.causation else 0

    @property
    def event_type(self) -> str:
        """The type name of the event."""
        return type_of(self.event)

    @cached_property
    def data(self) -> dict[str, Any]:
        """The event's fields as JSON-compatible values, which conditions read."""
        return dump_event(self.event)


def about_rule(event: Event) -> RuleName | None:
    """Return the rule a reflexr event is about, or None for any other event."""
    match event:
        case (
            RuleFired()
            | RuleErrored()
            | RuleReset()
            | RuleInstalled()
            | RuleArchived()
            | RunStarted()
            | RunProgressed()
            | RunRetrying()
            | RunSucceeded()
            | RunDeadLettered()
            | RunCancelled()
            | RunRequeued()
            | RunSkipped()
        ):
            return event.rule
        case _:
            return None


def _errors(error: ValidationError) -> list[JsonValue]:
    return [
        {"loc": [str(part) for part in item["loc"]], "msg": item["msg"], "type": item["type"]}
        for item in error.errors(include_url=False)
    ]
