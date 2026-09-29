"""Event types, their registry, envelopes and actors."""

from datetime import UTC, datetime
from typing import get_args

import pytest
from pydantic import ValidationError

import tests.event_types  # noqa: F401  (registers the test event types)
from reflexr.core import (
    AgentActor,
    Causation,
    Envelope,
    EvaluatorActor,
    Event,
    ExternalAgentActor,
    NotFound,
    RuleFired,
    SourceActor,
    SystemActor,
    SystemEvent,
    Tick,
    UnknownEvent,
    UserActor,
    ValidationFailed,
    about_rule,
    dump_event,
    event_types,
    get_event_type,
    load_event,
    type_of,
)
from tests.event_types import Flag, ServiceError

NOW = datetime(2026, 1, 1, tzinfo=UTC)


def test_types_register_by_class_name_or_explicit_name() -> None:
    assert Flag.event_type == "flag"
    assert ServiceError.event_type == "service.error"
    assert get_event_type("flag") is Flag
    assert event_types()["service.error"] is ServiceError
    with pytest.raises(NotFound):
        get_event_type("nope")


def test_a_name_cannot_be_registered_twice() -> None:
    with pytest.raises(TypeError, match="already registered"):

        class Other(Event, name="flag"):
            pass


def test_abstract_types_are_not_registered() -> None:
    class Base(Event, abstract=True):
        pass

    assert "base" not in event_types()


def test_events_are_immutable_and_strict() -> None:
    error = ServiceError(service="auth")
    attribute = "service"
    with pytest.raises(ValidationError):
        setattr(error, attribute, "billing")
    with pytest.raises(ValidationError):
        ServiceError.model_validate({"service": "auth", "sevrity": 1})


def test_loading_and_dumping_carry_the_type() -> None:
    loaded = load_event({"type": "service.error", "service": "auth", "severity": 8})
    assert loaded == ServiceError(service="auth", severity=8)
    assert dump_event(loaded)["type"] == "service.error"
    assert dump_event(loaded, mode="python")["labels"] == {"env": "prod", "team": None}


def test_loading_rejects_bad_data() -> None:
    with pytest.raises(ValidationFailed, match="string 'type'"):
        load_event({"service": "auth"})
    with pytest.raises(ValidationFailed, match=r"invalid service\.error") as failed:
        load_event({"type": "service.error"})
    assert failed.value.payload()["errors"]


def test_unknown_types_round_trip() -> None:
    unknown = load_event({"type": "legacy.alert", "level": 3, "unknown_type": "ignored"})
    assert isinstance(unknown, UnknownEvent)
    assert type_of(unknown) == "legacy.alert"
    assert dump_event(unknown) == {"type": "legacy.alert", "level": 3}


def test_envelopes_validate_events_through_the_registry() -> None:
    raw = {
        "seq": 1,
        "id": "evt_1",
        "ts": "2026-01-01T00:00:00Z",
        "workspace_id": "w1",
        "actor": {"kind": "source", "name": "monitor"},
        "correlation_id": "evt_1",
        "event": {"type": "service.error", "service": "auth"},
    }
    envelope = Envelope.model_validate(raw)
    assert (envelope.event_type, envelope.depth) == ("service.error", 0)
    assert envelope.data["service"] == "auth"
    assert Envelope.model_validate_json(envelope.model_dump_json()) == envelope
    assert envelope.model_dump()["event"]["type"] == "service.error"
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
    fired = RuleFired(rule="r", scope={}, scope_key="[]", firing_id="f", matched=(1,))
    assert about_rule(fired) == "r"
    assert about_rule(Tick(schedule="clock", at=NOW)) is None
    assert about_rule(ServiceError(service="auth")) is None


@pytest.mark.parametrize("event_type", get_args(SystemEvent))
def test_every_fact_about_a_rule_names_it(event_type: type[Event]) -> None:
    # Without this, a new fact about runs could trigger the rule whose run it describes.
    assert about_rule(event_type.model_construct(rule="r")) == "r"


@pytest.mark.parametrize(
    ("actor", "name"),
    [
        (UserActor(id="u1"), "u1"),
        (UserActor(id="u1", name="Alice"), "Alice"),
        (AgentActor(rule="r", run_id="fir_1", name="triage"), "triage"),
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
            AgentActor(rule="triage", run_id="fir_1", name="triage"),
            AgentActor(rule="triage", run_id="fir_2", name="triage"),
            ExternalAgentActor(client_id="claude-code"),
            SystemActor(name="reactor"),
            SourceActor(name="monitor"),
            EvaluatorActor(name="judge", version="3"),
        )
    ] == [
        "user:ada",
        "agent:triage",
        "agent:triage",
        "external_agent:claude-code",
        "system:reactor",
        "source:monitor",
        "evaluator:judge@3",
    ]
