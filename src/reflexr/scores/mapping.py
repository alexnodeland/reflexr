"""How feedback becomes scores: one score per field, typed by the field (ADR-0019).

Each field of a feedback type is a score named ``{type}.{field}``. Its field type decides the
score's data type:

- ``bool``: ``BOOLEAN`` (1 or 0)
- ``int`` and ``float``: ``NUMERIC``, bounded by the field's ``ge``, ``gt``, ``le`` and ``lt``
- ``Literal`` and ``Enum``: ``CATEGORICAL``, with their values as the categories
- ``str``: ``TEXT``

Optional fields are scored when they have a value. Fields of any other type are not scored.
"""

import types
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from enum import Enum
from typing import Annotated, Any, Literal, Union, assert_never, get_args, get_origin

from pydantic import JsonValue
from pydantic.fields import FieldInfo

from reflexr.core import Feedback

ScoreDataType = Literal["NUMERIC", "BOOLEAN", "CATEGORICAL", "TEXT"]
"""A score's data type."""

MAX_TEXT = 500
"""The longest text score; longer text is cut."""


@dataclass(frozen=True)
class ScoreConfig:
    """How one field of a feedback type is scored.

    Args:
        name: The score's name, ``{type}.{field}``.
        feedback_type: The feedback type's registered name.
        field: The field's name.
        data_type: How the field is scored.
        description: The field's description, if it has one.
        minimum: The lowest numeric value, if the field is bounded below.
        maximum: The highest numeric value, if the field is bounded above.
        categories: A categorical field's values.
    """

    name: str
    feedback_type: str
    field: str
    data_type: ScoreDataType
    description: str | None = None
    minimum: float | None = None
    maximum: float | None = None
    categories: tuple[str, ...] = ()


def score_configs(feedback_type: type[Feedback]) -> tuple[ScoreConfig, ...]:
    """Return how each scorable field of a feedback type is scored, in field order."""
    configs: list[ScoreConfig] = []
    for field_name, field in feedback_type.model_fields.items():
        config = _config(feedback_type.feedback_type, field_name, field)
        if config is not None:
            configs.append(config)
    return tuple(configs)


def score_values(
    feedback_type: type[Feedback], value: Mapping[str, JsonValue]
) -> list[tuple[ScoreConfig, float | str]]:
    """Return the scores in a validated feedback value, skipping fields without a value."""
    scores: list[tuple[ScoreConfig, float | str]] = []
    for config in score_configs(feedback_type):
        raw = value.get(config.field)
        if raw is None or raw == "":
            continue
        match config.data_type:
            case "BOOLEAN":
                scores.append((config, 1.0 if raw else 0.0))
            case "NUMERIC":
                assert isinstance(raw, int | float), "the value was validated as a number"
                scores.append((config, float(raw)))
            case "CATEGORICAL" | "TEXT":
                scores.append((config, str(raw)[:MAX_TEXT]))
            case _:
                assert_never(config.data_type)
    return scores


def _config(feedback_type: str, field_name: str, field: FieldInfo) -> ScoreConfig | None:
    annotation, inner = _unwrap(field.annotation)
    # A Field() inside an Optional keeps its own constraints and description.
    nested = [info for info in inner if isinstance(info, FieldInfo)]
    metadata = [*field.metadata, *inner, *(m for info in nested for m in info.metadata)]
    descriptions = [field.description, *(info.description for info in nested)]
    common: dict[str, Any] = {
        "name": f"{feedback_type}.{field_name}",
        "feedback_type": feedback_type,
        "field": field_name,
        "description": next((d for d in descriptions if d), None),
    }
    if annotation is bool:
        return ScoreConfig(data_type="BOOLEAN", **common)
    if annotation is int or annotation is float:
        minimum, maximum = _bounds(metadata)
        return ScoreConfig(data_type="NUMERIC", minimum=minimum, maximum=maximum, **common)
    if get_origin(annotation) is Literal:
        categories = tuple(str(arg) for arg in get_args(annotation))
        return ScoreConfig(data_type="CATEGORICAL", categories=categories, **common)
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        categories = tuple(str(member.value) for member in annotation)
        return ScoreConfig(data_type="CATEGORICAL", categories=categories, **common)
    if annotation is str:
        return ScoreConfig(data_type="TEXT", **common)
    return None


def _unwrap(annotation: Any) -> tuple[Any, list[Any]]:
    """Strip ``Optional`` and ``Annotated``, keeping ``Annotated``'s metadata."""
    metadata: list[Any] = []
    while True:
        if get_origin(annotation) is Annotated:
            annotation, *extra = get_args(annotation)
            metadata.extend(extra)
        elif get_origin(annotation) in (Union, types.UnionType):
            members = [arg for arg in get_args(annotation) if arg is not type(None)]
            if len(members) != 1:
                return annotation, metadata
            annotation = members[0]
        else:
            return annotation, metadata


def _bounds(metadata: Iterable[Any]) -> tuple[float | None, float | None]:
    minimum: float | None = None
    maximum: float | None = None
    for constraint in metadata:
        for name in ("ge", "gt"):
            if (bound := getattr(constraint, name, None)) is not None:
                minimum = float(bound)
        for name in ("le", "lt"):
            if (bound := getattr(constraint, name, None)) is not None:
                maximum = float(bound)
    return minimum, maximum
