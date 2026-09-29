"""Langfuse behind evalr's score ports: scores and score configs (ADR-0025)."""

import asyncio
import re
from collections.abc import Sequence
from typing import Any

from evalr.core import Score, ScoreConfig
from langfuse import Langfuse
from langfuse.api import ConfigCategory, ScoreConfigDataType

_CONFIG_NAME = re.compile(r"^[A-Za-z0-9_ .()-]{1,35}$")
"""The score config names Langfuse accepts."""

_PAGE = 100


class LangfuseScores:
    """A ``ScoreSink`` that records scores in Langfuse.

    Scores are queued by the Langfuse client and sent in the background; a score whose id
    Langfuse already has replaces it. A yes or no is sent as 1 or 0, and the score's metadata
    (where the feedback came from) as Langfuse's.

    Args:
        client: The Langfuse client.
    """

    def __init__(self, client: Langfuse) -> None:
        self._client = client

    async def record(self, scores: Sequence[Score], /) -> None:
        """Queue scores for Langfuse.

        Raises:
            ValueError: If a score's value is not of its data type; none of the scores is queued.
        """
        for arguments in [_arguments(score) for score in scores]:
            self._client.create_score(**arguments)


def _arguments(score: Score) -> dict[str, Any]:
    """A score as the Langfuse client's ``create_score`` takes it."""
    arguments: dict[str, Any] = {
        "name": score.name,
        "score_id": score.id,
        "trace_id": score.trace_id,
        "session_id": score.session_id,
        "timestamp": score.timestamp,
        "metadata": score.metadata,
    }
    match score.data_type, score.value:
        case ("NUMERIC" | "BOOLEAN") as data_type, int() | float() as number:
            return {**arguments, "value": float(number), "data_type": data_type}
        case ("CATEGORICAL" | "TEXT") as data_type, str() as text:
            return {**arguments, "value": text, "data_type": data_type}
        case _:
            raise ValueError(f"a {score.data_type} score cannot be {score.value!r}")


class LangfuseScoreConfigs:
    """A ``ScoreConfigStore`` over Langfuse's score configs.

    Langfuse's API is synchronous, so its calls run in a thread.

    Args:
        client: The Langfuse client.
    """

    def __init__(self, client: Langfuse) -> None:
        self._client = client

    async def names(self) -> set[str]:
        """Return the names of Langfuse's score configs, archived or not."""
        return await asyncio.to_thread(self._names)

    def _names(self) -> set[str]:
        names: set[str] = set()
        page = 1
        while True:
            configs = self._client.api.score_configs.get(page=page, limit=_PAGE)
            names.update(config.name for config in configs.data)
            if page >= (configs.meta.total_pages or 1):
                return names
            page += 1

    async def create(self, config: ScoreConfig) -> None:
        """Create a score config in Langfuse.

        Raises:
            ValueError: If the name is not one Langfuse accepts: at most 35 letters, digits,
                spaces, and ``_.()-``. Shorten the feedback type's name or the field's.
        """
        if not _CONFIG_NAME.match(config.name):
            raise ValueError(
                f"Langfuse does not accept the score config name {config.name!r}: shorten the "
                "feedback type's name (name=...) or the field's, to 35 characters at most"
            )
        options: dict[str, Any] = {}
        if config.categories:
            options["categories"] = [
                ConfigCategory(label=label, value=index)
                for index, label in enumerate(config.categories)
            ]
        if config.minimum is not None:
            options["min_value"] = config.minimum
        if config.maximum is not None:
            options["max_value"] = config.maximum
        if config.description:
            options["description"] = config.description
        await asyncio.to_thread(
            self._client.api.score_configs.create,
            name=config.name,
            data_type=ScoreConfigDataType(config.data_type),
            **options,
        )
