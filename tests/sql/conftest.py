"""Fixtures for the SQL storage tests, parametrized over SQLite and PostgreSQL."""

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from reflexr.sql import create_schema
from tests.databases import SQL_BACKENDS, Database, empty_database


@pytest.fixture(params=SQL_BACKENDS)
async def database(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Database]:
    """An empty database, with no tables."""
    async with empty_database(request.param, tmp_path) as database:
        yield database


@pytest.fixture
def engine(database: Database) -> AsyncEngine:
    """An engine on the empty database."""
    return database.engine()


@pytest.fixture
async def schema(database: Database) -> Database:
    """A database with reflexr's tables."""
    await create_schema(database.engine())
    return database
