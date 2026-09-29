"""Typed feedback, its targets, and evaluator actors."""

from datetime import UTC, datetime
from typing import Any, Literal

import pytest

from reflexr.core import (
    ChainTarget,
    Envelope,
    EvaluatorActor,
    Feedback,
    FeedbackGiven,
    FiringTarget,
    NotFound,
    RunTarget,
    ValidationFailed,
    feedback_types,
    load_feedback,
)


class Triage(Feedback, name="triage", targets={"run"}):
    correct: bool
    severity: Literal["low", "high", "critical"]
    reason: str | None = None


class Resolution(Feedback, targets={"chain", "run"}):
    resolved: bool


def test_feedback_types_register_with_their_targets() -> None:
    assert (Triage.feedback_type, Triage.targets) == ("triage", frozenset({"run"}))
    assert Resolution.feedback_type == "resolution"
    assert feedback_types()["triage"] is Triage


def test_targets_are_required_and_known() -> None:
    with pytest.raises(TypeError, match="must declare targets"):

        class NoTargets(Feedback):
            ok: bool

    unknown: Any = {"thread"}
    with pytest.raises(TypeError, match="must declare targets"):

        class WrongTargets(Feedback, targets=unknown):
            ok: bool


def test_names_are_unique_and_abstract_types_are_not_registered() -> None:
    with pytest.raises(TypeError, match="already registered"):

        class Other(Feedback, name="triage", targets={"run"}):
            ok: bool

    class Base(Feedback, abstract=True):
        pass

    assert "base" not in feedback_types()


def test_feedback_is_validated_against_its_type_and_target() -> None:
    feedback = load_feedback(
        "triage", RunTarget(run_id="fir_1"), {"correct": True, "severity": "high"}
    )
    assert feedback == Triage(correct=True, severity="high")
    with pytest.raises(NotFound):
        load_feedback("nope", RunTarget(run_id="fir_1"), {})
    with pytest.raises(ValidationFailed, match="given on run, not on a firing"):
        load_feedback("triage", FiringTarget(firing_id="fir_1"), {"correct": True})
    with pytest.raises(ValidationFailed, match="invalid triage feedback") as invalid:
        load_feedback("triage", RunTarget(run_id="fir_1"), {"correct": True, "severity": "meh"})
    assert invalid.value.payload()["errors"]


def test_feedback_is_an_event_with_a_typed_target() -> None:
    given = FeedbackGiven(
        feedback_type="resolution",
        target=ChainTarget(correlation_id="evt_1"),
        value={"resolved": True},
    )
    envelope = Envelope(
        seq=1,
        id="evt_2",
        ts=datetime(2026, 1, 1, tzinfo=UTC),
        workspace_id="w1",
        actor=EvaluatorActor(name="resolution-judge", version="3f9c"),
        correlation_id="evt_1",
        traceparent="00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        event=given,
    )
    restored = Envelope.model_validate_json(envelope.model_dump_json())
    assert restored == envelope
    assert isinstance(restored.event, FeedbackGiven)
    assert restored.event.target == ChainTarget(correlation_id="evt_1")
    assert envelope.actor.display_name == "resolution-judge@3f9c"
