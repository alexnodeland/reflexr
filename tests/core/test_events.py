"""Event types, their namespaces and registries, envelopes and actors."""

import re
from datetime import UTC, datetime
from typing import get_args

import pytest
from pydantic import TypeAdapter, ValidationError

from reflexr.core import (
    DEFAULT_REGISTRY,
    SYSTEM_EVENTS,
    AgentActor,
    Causation,
    Envelope,
    EvaluatorActor,
    Event,
    EventName,
    EventRegistry,
    ExternalAgentActor,
    RuleFired,
    SourceActor,
    SystemActor,
    SystemEvent,
    Tick,
    UnknownEvent,
    UserActor,
    ValidationFailed,
    about_rule,
    check_event_name,
    dump_event,
    load_event,
    type_of,
)
from tests.event_types import AppEvent, Flag, ServiceError

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_types_register_under_their_namespace() -> None:
    assert (Flag.event_namespace, Flag.event_type) == ("app", "app:flag")
    assert ServiceError.event_type == "app:service.error"
    assert RuleFired.event_type == "reflexr:rule_fired"
    assert DEFAULT_REGISTRY["app:flag"] is Flag
    assert DEFAULT_REGISTRY.get("app:nope") is None


def test_a_name_cannot_be_registered_twice() -> None:
    with pytest.raises(TypeError, match="already registered"):

        class Other(AppEvent, name="flag"):
            pass


def test_abstract_types_are_not_registered() -> None:
    class Base(AppEvent, abstract=True):
        pass

    assert "app:base" not in DEFAULT_REGISTRY


def test_a_type_needs_a_namespace_and_a_local_name() -> None:
    with pytest.raises(TypeError, match=r"Loose has no namespace; subclass a base"):

        class Loose(Event):
            pass

    with pytest.raises(TypeError, match=r"name 'app:named' is not a local name"):

        class Named(AppEvent, name="app:named"):
            pass


def test_a_namespace_is_declared_once_on_an_abstract_base() -> None:
    with pytest.raises(
        TypeError, match=r"'app' is already declared by tests\.event_types\.AppEvent"
    ):

        class Again(Event, abstract=True, event_namespace="app"):
            pass

    with pytest.raises(TypeError, match="must be abstract=True"):

        class Concrete(Event, event_namespace="concrete"):
            pass

    with pytest.raises(TypeError, match="'Bad' is not a namespace"):

        class Capital(Event, abstract=True, event_namespace="Bad"):
            pass

    with pytest.raises(TypeError, match="registry= without declaring a namespace"):

        class Stray(AppEvent, abstract=True, registry=EventRegistry()):
            pass


def test_the_same_base_may_be_imported_again() -> None:
    defined: list[type[Event]] = []
    for _ in range(2):  # the same module and name, as when a module is imported again

        class Reloaded(Event, abstract=True, event_namespace="reloaded"):
            pass

        class Thing(Reloaded):
            pass

        defined.append(Thing)
    assert DEFAULT_REGISTRY["reloaded:thing"] is defined[-1]


def test_a_registry_is_a_view_of_its_namespaces() -> None:
    private = EventRegistry()

    class PrivateEvent(Event, abstract=True, event_namespace="private", registry=private):
        pass

    class Secret(PrivateEvent):
        pass

    facts = {t.event_type for t in SYSTEM_EVENTS}
    assert private["private:secret"] is Secret
    assert "private:secret" not in DEFAULT_REGISTRY
    assert "reflexr:tick" in private
    assert "app:flag" not in private
    assert set(private) == {"private:secret", *facts}
    assert len(private) == len(facts) + 1
    private.add(AppEvent)
    assert private["app:flag"] is Flag
    with pytest.raises(TypeError, match="Flag declares no namespace"):
        private.add(Flag)


def test_names_are_qualified_with_a_hint() -> None:
    class Left(Event, abstract=True, event_namespace="left"):
        pass

    class Right(Event, abstract=True, event_namespace="right"):
        pass

    class LeftTwin(Left, name="twin"):
        pass

    class RightTwin(Right, name="twin"):
        pass

    assert check_event_name("app:flag") == "app:flag"
    hints = {
        "service.error": "has no namespace; did you mean 'app:service.error'?",
        "twin": "did you mean one of 'left:twin', 'right:twin'?",
        "nothing.like.it": "has no namespace; a name is a namespace, a ':' and a local name",
        "App:Flag": "'App:Flag' is not an event type name",
        "a:b:c": "'a:b:c' is not an event type name",
    }
    for name, hint in hints.items():
        with pytest.raises(ValueError, match=re.escape(hint)):
            check_event_name(name)
    names = TypeAdapter(tuple[EventName, ...])
    assert names.validate_python(["app:flag"]) == ("app:flag",)
    with pytest.raises(ValidationError, match=re.escape("did you mean 'reflexr:tick'?")):
        names.validate_python(["tick"])
    assert names.json_schema()["items"]["pattern"].startswith("^[a-z]")


def test_events_are_immutable_and_strict() -> None:
    error = ServiceError(service="auth")
    attribute = "service"
    with pytest.raises(ValidationError):
        setattr(error, attribute, "billing")
    with pytest.raises(ValidationError):
        ServiceError.model_validate({"service": "auth", "sevrity": 1})


def test_loading_and_dumping_carry_the_type() -> None:
    loaded = load_event({"type": "app:service.error", "service": "auth", "severity": 8})
    assert loaded == ServiceError(service="auth", severity=8)
    assert dump_event(loaded)["type"] == "app:service.error"
    assert dump_event(loaded, mode="python")["labels"] == {"env": "prod", "team": None}


def test_loading_rejects_bad_data() -> None:
    with pytest.raises(ValidationFailed, match="string 'type'"):
        load_event({"service": "auth"})
    with pytest.raises(ValidationFailed, match=r"invalid app:service\.error") as failed:
        load_event({"type": "app:service.error"})
    assert failed.value.payload()["errors"]


def test_unknown_types_round_trip() -> None:
    unknown = load_event({"type": "app:legacy.alert", "level": 3, "unknown_type": "ignored"})
    assert isinstance(unknown, UnknownEvent)
    assert type_of(unknown) == "app:legacy.alert"
    assert dump_event(unknown) == {"type": "app:legacy.alert", "level": 3}


def test_envelopes_validate_events_through_the_registry() -> None:
    raw = {
        "seq": 1,
        "id": "evt_1",
        "ts": "2026-01-01T00:00:00Z",
        "workspace_id": "w1",
        "actor": {"kind": "source", "name": "monitor"},
        "correlation_id": "evt_1",
        "event": {"type": "app:service.error", "service": "auth"},
    }
    envelope = Envelope.model_validate(raw)
    assert (envelope.event_type, envelope.depth) == ("app:service.error", 0)
    assert envelope.data["service"] == "auth"
    assert Envelope.model_validate_json(envelope.model_dump_json()) == envelope
    assert envelope.model_dump()["event"]["type"] == "app:service.error"
    with pytest.raises(ValidationError, match="an object with a 'type'"):
        Envelope.model_validate({**raw, "event": 3})


def test_causation_gives_an_envelope_its_depth() -> None:
    envelope = Envelope(
        seq=1,
        id="e",
        ts=NOW,
        workspace_id="w",
        actor=SystemActor(),
        causation=Causation(firing_id="f", run_id="f", depth=3),
        correlation_id="e",
        event=Tick(schedule="clock", at=NOW),
    )
    assert envelope.depth == 3


def test_about_rule_names_the_rule_of_reflexr_facts_only() -> None:
    fired = RuleFired(rule="app:r", scope={}, scope_key="[]", firing_id="f", matched=(1,))
    assert about_rule(fired) == "app:r"
    assert about_rule(Tick(schedule="clock", at=NOW)) is None
    assert about_rule(ServiceError(service="auth")) is None


@pytest.mark.parametrize("event_type", get_args(SystemEvent))
def test_every_fact_about_a_rule_names_it(event_type: type[Event]) -> None:
    # Without this, a new fact about runs could trigger the rule whose run it describes.
    assert about_rule(event_type.model_construct(rule="app:r")) == "app:r"


@pytest.mark.parametrize(
    ("actor", "name"),
    [
        (UserActor(id="u1"), "u1"),
        (UserActor(id="u1", name="Alice"), "Alice"),
        (AgentActor(rule="app:r", run_id="fir_1", name="triage"), "triage"),
        (ExternalAgentActor(client_id="c1"), "c1"),
        (ExternalAgentActor(client_id="c1", name="Claude"), "Claude"),
        (SystemActor(), "reflexr"),
        (SourceActor(name="monitor"), "monitor"),
    ],
)
def test_actors_have_display_names(
    actor: UserActor | AgentActor | ExternalAgentActor | SystemActor | SourceActor, name: str
) -> None:
    assert actor.display_name == name


def test_participants_are_stable_across_one_actors_actions() -> None:
    assert [
        actor.participant
        for actor in (
            UserActor(id="ada"),
            AgentActor(rule="app:triage", run_id="fir_1", name="triage"),
            AgentActor(rule="app:triage", run_id="fir_2", name="triage"),
            ExternalAgentActor(client_id="claude-code"),
            SystemActor(name="reactor"),
            SourceActor(name="monitor"),
            EvaluatorActor(name="judge", version="3"),
        )
    ] == [
        "user:ada",
        "agent:app:triage",
        "agent:app:triage",
        "external_agent:claude-code",
        "system:reactor",
        "source:monitor",
        "evaluator:judge@3",
    ]
