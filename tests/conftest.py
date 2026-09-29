import pytest

from tests.databases import POSTGRES_URL


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Leave out the PostgreSQL tests unless a database is configured; the summary counts them."""
    if POSTGRES_URL:
        return
    postgres = [item for item in items if item.get_closest_marker("postgres")]
    config.hook.pytest_deselected(items=postgres)
    items[:] = [item for item in items if item not in postgres]
