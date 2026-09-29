"""Fakes of the score ports, for tests of the mirror and as a reference for adapters."""

from collections.abc import Collection

from reflexr.scores import Score, ScoreConfig


class FakeSink:
    """A score sink that keeps scores by id, so sending one again replaces it."""

    def __init__(self) -> None:
        self.scores: dict[str, Score] = {}

    async def send(self, score: Score) -> None:
        self.scores[score.id] = score


class FakeConfigStore:
    """A score config store in memory."""

    def __init__(self, *names: str) -> None:
        self.configs: dict[str, ScoreConfig | None] = dict.fromkeys(names)

    async def names(self) -> Collection[str]:
        return set(self.configs)

    async def create(self, config: ScoreConfig) -> None:
        assert config.name not in self.configs, f"{config.name} exists already"
        self.configs[config.name] = config
