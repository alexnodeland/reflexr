"""The score ports' in-memory adapters come from evalr; this seeds its config store."""

from evalr.core import ScoreConfig
from evalr.memory import InMemoryScoreConfigStore


def seeded(*names: str) -> InMemoryScoreConfigStore:
    """An in-memory score config store that already has configs of these names."""
    store = InMemoryScoreConfigStore()
    for name in names:
        type_name, field = name.split(".")
        store.configs[name] = ScoreConfig(name, type_name, field, "NUMERIC")
    return store
