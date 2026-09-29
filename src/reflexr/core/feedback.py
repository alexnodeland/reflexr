"""Typed feedback: people's and evaluators' judgements of runs, firings and causal chains.

A feedback type is a Pydantic model that subclasses :class:`Feedback`, registered by name like an
event type, and declares what it can be given on::

    class Triage(Feedback, name="triage", targets={"run"}):
        correct: bool
        severity: Literal["low", "high", "critical"]
        reason: str | None = None

Feedback is recorded as a ``feedback_given`` event, so it is attributed, replayable, and rules
can watch it. An evaluator's verdict is feedback too, given by an :class:`EvaluatorActor`.
"""

import re
from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue, ValidationError

from reflexr.core.errors import NotFound, ValidationFailed
from reflexr.core.ids import FiringId, RunId

type TargetKind = Literal["run", "firing", "chain"]
"""What feedback can be about: a run (a workflow execution), a firing, or a causal chain."""

TARGET_KINDS: frozenset[TargetKind] = frozenset({"run", "firing", "chain"})


class RunTarget(BaseModel):
    """Feedback on one run: a workflow execution."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["run"] = "run"
    run_id: RunId


class FiringTarget(BaseModel):
    """Feedback on one firing: whether the rule should have fired."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["firing"] = "firing"
    firing_id: FiringId


class ChainTarget(BaseModel):
    """Feedback on a causal chain: everything one triggering event led to."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    kind: Literal["chain"] = "chain"
    correlation_id: str


type FeedbackTarget = Annotated[RunTarget | FiringTarget | ChainTarget, Field(discriminator="kind")]
"""What a piece of feedback is about."""

_registry: dict[str, type["Feedback"]] = {}


class Feedback(BaseModel):
    """Base class for feedback types.

    Field types decide how each field is scored: bounded numbers are numeric, ``bool`` is a
    yes/no, ``Literal`` and ``Enum`` are categories, and ``str`` is free text.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    feedback_type: ClassVar[str]
    """The registered type name, derived from the class name unless given as ``name=``."""

    targets: ClassVar[frozenset[TargetKind]]
    """What this type of feedback can be given on."""

    def __init_subclass__(
        cls,
        *,
        name: str | None = None,
        targets: Iterable[TargetKind] | None = None,
        abstract: bool = False,
        **kwargs: Any,
    ) -> None:
        # Consumed in __pydantic_init_subclass__, once the fields exist.
        super().__init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(
        cls,
        *,
        name: str | None = None,
        targets: Iterable[TargetKind] | None = None,
        abstract: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__pydantic_init_subclass__(**kwargs)
        if abstract:
            return
        chosen = frozenset(targets or ())
        if not chosen or not chosen <= TARGET_KINDS:
            raise TypeError(
                f"{cls.__name__} must declare targets from {sorted(TARGET_KINDS)}, "
                f"such as targets={{'run'}}"
            )
        cls.targets = chosen
        cls.feedback_type = name or _snake_case(cls.__name__)
        _register(cls)


def _snake_case(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])", "_", name).lower()


def _qualified(feedback_type: "type[Feedback]") -> str:
    return f"{feedback_type.__module__}.{feedback_type.__qualname__}"


def _register(feedback_type: "type[Feedback]") -> None:
    existing = _registry.get(feedback_type.feedback_type)
    if existing is not None and _qualified(existing) != _qualified(feedback_type):
        raise TypeError(
            f"feedback type name {feedback_type.feedback_type!r} is already registered by "
            f"{_qualified(existing)}; pass name=... to choose another"
        )
    _registry[feedback_type.feedback_type] = feedback_type


def feedback_types() -> Mapping[str, type[Feedback]]:
    """Return a read-only view of every registered feedback type, by name."""
    return MappingProxyType(_registry)


def load_feedback(
    feedback_type: str, target: RunTarget | FiringTarget | ChainTarget, value: Mapping[str, Any]
) -> Feedback:
    """Validate feedback of a registered type for a target.

    Raises:
        NotFound: If no feedback type is registered under that name.
        ValidationFailed: If the type cannot be given on that kind of target, or the value does
            not validate.
    """
    try:
        model = _registry[feedback_type]
    except KeyError:
        raise NotFound("feedback type", feedback_type) from None
    if target.kind not in model.targets:
        allowed = ", ".join(sorted(model.targets))
        raise ValidationFailed(
            f"{feedback_type} feedback is given on {allowed}, not on a {target.kind}", []
        )
    try:
        return model.model_validate(dict(value))
    except ValidationError as error:
        errors: list[JsonValue] = [
            {"loc": [str(p) for p in e["loc"]], "msg": e["msg"], "type": e["type"]}
            for e in error.errors(include_url=False)
        ]
        raise ValidationFailed(f"invalid {feedback_type} feedback", errors) from error
