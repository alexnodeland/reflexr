"""Feedback types as score configs, and feedback values as scores."""

from typing import Annotated

from pydantic import Field

from reflexr.core import Feedback
from reflexr.scores import MAX_TEXT, ScoreConfig, score_configs, score_values
from tests.scores.kinds import Accuracy, Helpfulness


def test_each_field_is_scored_by_its_type() -> None:
    assert score_configs(Helpfulness) == (
        ScoreConfig(
            name="helpfulness.rating",
            feedback_type="helpfulness",
            field="rating",
            data_type="NUMERIC",
            minimum=1,
            maximum=5,
        ),
        ScoreConfig(
            name="helpfulness.reason", feedback_type="helpfulness", field="reason", data_type="TEXT"
        ),
    )
    correct, verdict, tone, confidence = score_configs(Accuracy)
    assert correct.data_type == "BOOLEAN"
    assert (verdict.data_type, verdict.categories) == ("CATEGORICAL", ("right", "wrong"))
    assert (tone.data_type, tone.categories) == ("CATEGORICAL", ("formal", "casual"))
    assert (confidence.minimum, confidence.maximum) == (0, 1)


def test_optional_constrained_and_unscorable_fields() -> None:
    class Review(Feedback, name="review_for_scores", targets={"chain"}):
        effort: Annotated[int, Field(gt=0, lt=10, description="How hard it was")] | None = None
        tags: tuple[str, ...] = ()
        either: int | str = 0
        unbounded: float = 0.0

    [effort, unbounded] = score_configs(Review)
    assert (effort.name, effort.minimum, effort.maximum) == ("review_for_scores.effort", 0, 10)
    assert effort.description == "How hard it was"
    assert (unbounded.minimum, unbounded.maximum) == (None, None)


def test_values_become_scores() -> None:
    assert [(c.field, v) for c, v in score_values(Helpfulness, {"rating": 4, "reason": None})] == [
        ("rating", 4.0)
    ]
    assert score_values(Helpfulness, {"rating": 4, "reason": ""})[1:] == []
    long = score_values(Helpfulness, {"rating": 1, "reason": "x" * 900})[1][1]
    assert long == "x" * MAX_TEXT
    value = {"correct": False, "verdict": "wrong", "tone": "casual", "confidence": 0.5}
    assert [v for _, v in score_values(Accuracy, value)] == [0.0, "wrong", "casual", 0.5]
    assert score_values(Accuracy, value | {"correct": True})[0][1] == 1.0
