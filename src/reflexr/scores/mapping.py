"""Feedback types through evalr's mapping, their scores named as the types are registered.

evalr owns how a field becomes a score (ADR-0025, as amended): one score per field, named
``{type}.{field}`` and typed by the field. Here the ``{type}`` is a feedback type's registered
name, which differs from evalr's default, the class name in snake case, for a type registered
with ``name=``.
"""

from collections.abc import Mapping

import evalr.core
from evalr.core import ScoreConfig
from pydantic import JsonValue

from reflexr.core import Feedback


def score_configs(feedback_type: type[Feedback]) -> tuple[ScoreConfig, ...]:
    """Return how each scorable field of a feedback type is scored, in field order."""
    return evalr.core.score_configs(feedback_type, type_name=feedback_type.feedback_type)


def score_values(
    feedback_type: type[Feedback], value: Mapping[str, JsonValue]
) -> list[tuple[ScoreConfig, bool | float | str]]:
    """Return the scores in a validated feedback value, skipping fields without a value.

    A ``BOOLEAN`` score's value is a bool, a ``NUMERIC`` one's a float, and a ``CATEGORICAL`` or
    ``TEXT`` one's a string of at most ``MAX_TEXT`` characters.
    """
    return evalr.core.score_values(feedback_type, value, type_name=feedback_type.feedback_type)
