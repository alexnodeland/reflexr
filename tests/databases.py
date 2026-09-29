"""Empty databases for the SQL tests: a SQLite file, and PostgreSQL when it is configured.

The PostgreSQL tests run when ``REFLEXR_TEST_POSTGRES_URL`` is set (``make pg-up`` starts a
database, and ``make test-pg`` sets it); otherwise they are deselected. Each test gets its own
schema, dropped afterwards.
"""

import os
import uuid
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from reflexr.sql import create_sqlite_engine

POSTGRES_URL = os.environ.get("REFLEXR_TEST_POSTGRES_URL")

SQL_BACKENDS = ["sqlite", pytest.param("postgres", marks=pytest.mark.postgres)]


@dataclass
class Database:
    """An empty database, and the engines a test opens on it."""

    connect: Callable[[], AsyncEngine]
    engines: list[AsyncEngine] = field(default_factory=list[AsyncEngine])

    def engine(self) -> AsyncEngine:
        """Open another engine on the database, as another process would."""
        engine = self.connect()
        self.engines.append(engine)
        return engine

    async def dispose(self) -> None:
        for engine in self.engines:
            await engine.dispose()


@asynccontextmanager
async def empty_database(backend: str, directory: Path) -> AsyncIterator[Database]:
    """Yield an empty database, and dispose of its engines (and it) afterwards."""
    if backend == "sqlite":
        url = f"sqlite+aiosqlite:///{directory / 'reflexr.db'}"
        database = Database(lambda: create_sqlite_engine(url))
        try:
            yield database
        finally:
            await database.dispose()
        return
    assert POSTGRES_URL, "PostgreSQL tests are deselected without a database"
    url = POSTGRES_URL
    schema = f"test_{uuid.uuid4().hex}"
    admin = create_async_engine(url)
    async with admin.begin() as connection:
        await connection.exec_driver_sql(f'CREATE SCHEMA "{schema}"')
    settings = {"server_settings": {"search_path": schema}}
    database = Database(lambda: create_async_engine(url, connect_args=settings))
    try:
        yield database
    finally:
        await database.dispose()
        async with admin.begin() as connection:
            await connection.exec_driver_sql(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.dispose()
