"""Feedback types for the score tests. Importing this module registers them."""

from enum import StrEnum
from typing import Annotated, Literal

from pydantic import Field

from reflexr import Feedback


class Helpfulness(Feedback, name="helpfulness", targets={"run", "chain"}):
    """A rating of how helpful a run was."""

    rating: Annotated[int, Field(ge=1, le=5)]
    reason: str | None = None


class Verdict(StrEnum):
    RIGHT = "right"
    WRONG = "wrong"


class Accuracy(Feedback, name="accuracy", targets={"firing"}):
    """Whether a rule was right to fire."""

    correct: bool
    verdict: Verdict = Verdict.RIGHT
    tone: Literal["formal", "casual"] = "formal"
    confidence: Annotated[float, Field(ge=0, le=1)] = 1.0
