"""Events, their registry, reflexr's own events, and envelopes.

An event type is a Pydantic model that subclasses :class:`Event`. Defining the class registers it
under a name derived from the class name, or the ``name=`` given explicitly::

    class ServiceError(Event, name="service.error"):
        service: str
        severity: int

On the wire, and in storage, an event is its fields plus ``"type"``: the registered name. Stored
events are wrapped in an :class:`Envelope` with their position in the workspace's log.
"""

import re
from collections.abc import Mapping
from functools import cached_property
from types import MappingProxyType
from typing import Annotated, Any, ClassVar, Literal, cast

from pydantic import (
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
from reflexr.core.errors import NotFound, ValidationFailed
from reflexr.core.feedback import FeedbackTarget
from reflexr.core.ids import EventId, FiringId, RuleName, RunId, ScopeKey, WorkspaceId

_registry: dict[str, type["Event"]] = {}


def _snake_case(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()


def _register(event_type: "type[Event]") -> None:
    existing = _registry.get(event_type.event_type)
    if existing is not None and _qualified(existing) != _qualified(event_type):
        raise TypeError(
            f"event type name {event_type.event_type!r} is already registered by "
            f"{_qualified(existing)}; pass name=... to choose another"
        )
    _registry[event_type.event_type] = event_type


def _qualified(event_type: "type[Event]") -> str:
    return f"{event_type.__module__}.{event_type.__qualname__}"


class Event(BaseModel):
    """Base class for event types.

    Subclass it with ordinary Pydantic fields. Events are facts, so they are immutable, and
    unknown fields are rejected so that a producer's typo fails loudly.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_type: ClassVar[str]
    """The registered type name, derived from the class name unless given as ``name=``."""

    def __init_subclass__(
        cls, *, name: str | None = None, abstract: bool = False, **kwargs: Any
    ) -> None:
        # The class arguments are consumed in __pydantic_init_subclass__, which runs once the
        # fields exist; they must not reach object.__init_subclass__.
        super().__init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(
        cls, *, name: str | None = None, abstract: bool = False, **kwargs: Any
    ) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if not abstract:
            cls.event_type = name or _snake_case(cls.__name__)
            _register(cls)


class UnknownEvent(Event, abstract=True):
    """An event whose type is not registered in this process, kept as-is so it round-trips.

    Stored envelopes can outlive the code that defined their types, and a newer producer can use
    types this version does not know. Their data is kept in :attr:`model_extra`.
    """

    model_config = ConfigDict(frozen=True, extra="allow")

    unknown_type: str
    """The type name the event was stored with."""


def event_types() -> Mapping[str, type[Event]]:
    """Return a read-only view of every registered event type, by name."""
    return MappingProxyType(_registry)


def get_event_type(name: str) -> type[Event]:
    """Return the event type registered under ``name``.

    Raises:
        NotFound: If no type is registered under that name.
    """
    try:
        return _registry[name]
    except KeyError:
        raise NotFound("event type", name) from None


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
    event_type = _registry.get(name)
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


class RuleFired(Event, name="rule_fired"):
    """A rule's condition held for one scope; a run of its action was created."""

    rule: RuleName
    scope: dict[str, JsonValue]
    scope_key: ScopeKey
    firing_id: FiringId
    matched: tuple[int, ...]
    """The ``seq`` of every envelope that made the condition hold."""


class RuleErrored(Event, name="rule_errored"):
    """A rule could not evaluate one envelope; it is dead-lettered for that rule alone."""

    rule: RuleName
    seq: int
    error: str


class RuleReset(Event, name="rule_reset"):
    """A rule's state was reset, because its definition changed or it was replayed."""

    rule: RuleName
    generation: int
    reason: Literal["changed", "replayed"]
    from_seq: int
    silent_through: int = 0
    """Envelopes up to this ``seq`` rebuild the rule's state without recording firings."""


class RunStarted(Event, name="run_started"):
    """An attempt of a run began."""

    run_id: RunId
    rule: RuleName
    scope: dict[str, JsonValue] = {}
    """The values of the rule's scope fields, as on ``rule_fired``."""

    scope_key: ScopeKey
    attempt: int


class RunProgressed(Event, name="run_progressed"):
    """A graph run completed a step and saved a checkpoint."""

    run_id: RunId
    rule: RuleName
    step: str


class RunRetrying(Event, name="run_retrying"):
    """An attempt failed, and the run will be tried again."""

    run_id: RunId
    rule: RuleName
    attempt: int
    error: str
    next_attempt_at: AwareDatetime
    reason: str | None = None
    """A stable code for why the attempt failed, when the action gave one."""


class RunSucceeded(Event, name="run_succeeded"):
    """A run finished."""

    run_id: RunId
    rule: RuleName
    output: JsonValue = None


class RunDeadLettered(Event, name="run_dead_lettered"):
    """A run exhausted its retries, or failed in a way retrying cannot fix."""

    run_id: RunId
    rule: RuleName
    attempts: int
    error: str
    reason: str | None = None
    """A stable code for why the last attempt failed, when the action gave one."""


class RunCancelled(Event, name="run_cancelled"):
    """A run was cancelled."""

    run_id: RunId
    rule: RuleName
    reason: str | None = None


class RunRequeued(Event, name="run_requeued"):
    """Someone made a run runnable again. The envelope's actor records who."""

    run_id: RunId
    rule: RuleName


class RunSkipped(Event, name="run_skipped"):
    """A run was skipped without completing, unblocking its scope."""

    run_id: RunId
    rule: RuleName
    reason: str | None = None


class FeedbackGiven(Event, name="feedback_given"):
    """A person or an evaluator judged a run, a firing or a causal chain."""

    feedback_type: str
    target: FeedbackTarget
    value: dict[str, JsonValue]
    """The feedback's fields, as validated against its registered type."""


class Tick(Event, name="tick"):
    """A schedule's tick. Ticks also move rules' clocks forward in quiet workspaces."""

    schedule: str
    at: AwareDatetime


SystemEvent = (
    RuleFired
    | RuleErrored
    | RuleReset
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
