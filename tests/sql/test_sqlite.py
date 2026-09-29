"""SQLite engines: one connection per engine, so a process's transactions take turns in order."""

import asyncio
from pathlib import Path

from sqlalchemy.pool import NullPool

from reflexr.sql import SqlStorage, create_schema, create_sqlite_engine
from reflexr.workspace import WorkspaceRef
from tests.workspace.helpers import entry

ACME = WorkspaceRef("acme", "prod")


async def test_transactions_in_one_process_take_turns_in_the_order_they_begin(
    tmp_path: Path,
) -> None:
    engine = create_sqlite_engine(f"sqlite+aiosqlite:///{tmp_path / 'reflexr.db'}")
    try:
        await create_schema(engine)
        storage = SqlStorage(engine)

        async def append(event_id: str) -> None:
            async with storage.transaction(ACME) as transaction:
                await transaction.append([entry(event_id)])

        ids = [f"e{i}" for i in range(20)]
        await asyncio.gather(*(append(event_id) for event_id in ids))
        assert [e.id for e in await storage.read(ACME)] == ids
    finally:
        await engine.dispose()


async def test_an_engine_can_pool_connections_its_own_way(tmp_path: Path) -> None:
    engine = create_sqlite_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'reflexr.db'}", poolclass=NullPool
    )
    try:
        assert isinstance(engine.pool, NullPool)
        await create_schema(engine)
        storage = SqlStorage(engine)
        async with storage.transaction(ACME) as transaction:
            await transaction.append([entry("e1")])
        assert await storage.head_seq(ACME) == 1
    finally:
        await engine.dispose()
