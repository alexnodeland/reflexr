"""The packaged migrations build exactly the schema the models describe."""

from typing import Any

from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import JSON, Connection, column, func, insert, inspect, select, table
from sqlalchemy.ext.asyncio import AsyncEngine

from reflexr.core import Envelope, Event, SourceActor, UnknownEvent
from reflexr.sql import SqlStorage, create_schema, migrate
from reflexr.sql.schema import _alembic_config
from reflexr.sql.tables import VERSION_TABLE, EventRow, metadata
from reflexr.workspace import WorkspaceRef
from tests.clock import START
from tests.event_types import Deploy, ServiceError


def _differences(connection: Connection) -> list[Any]:
    context = MigrationContext.configure(connection, opts={"version_table": VERSION_TABLE})
    return compare_metadata(context, metadata)


def _tables(connection: Connection) -> set[str]:
    return set(inspect(connection).get_table_names())


async def test_migrations_build_the_models_schema(engine: AsyncEngine) -> None:
    await migrate(engine)
    async with engine.connect() as connection:
        assert await connection.run_sync(_differences) == []
        assert await connection.run_sync(_tables) == {*metadata.tables, VERSION_TABLE}


async def test_migrating_an_up_to_date_database_changes_nothing(engine: AsyncEngine) -> None:
    await migrate(engine)
    await migrate(engine)
    async with engine.connect() as connection:
        assert await connection.run_sync(_differences) == []


async def test_migrations_downgrade_to_an_empty_database(engine: AsyncEngine) -> None:
    await migrate(engine)
    async with engine.begin() as connection:
        await connection.run_sync(lambda c: command.downgrade(_alembic_config(c), "base"))
        assert await connection.run_sync(_tables) == {VERSION_TABLE}


async def test_create_schema_builds_the_same_schema(engine: AsyncEngine) -> None:
    await create_schema(engine)
    async with engine.connect() as connection:
        assert await connection.run_sync(_tables) == set(metadata.tables)
        assert await connection.run_sync(_differences) == []


def _stored(seq: int, event: Event) -> dict[str, Any]:
    """An events row as reflexr 0002 wrote it: the envelope's JSON, with no type column."""
    envelope = Envelope(
        seq=seq,
        id=f"e{seq}",
        ts=START,
        workspace_id="prod",
        actor=SourceActor(name="monitor"),
        correlation_id=f"e{seq}",
        event=event,
    )
    return {
        "tenant_id": "acme",
        "workspace_id": "prod",
        "seq": seq,
        "event_id": envelope.id,
        "envelope": envelope.model_dump(mode="json"),
    }


async def test_upgrading_fills_the_event_type_column_from_the_stored_envelopes(
    engine: AsyncEngine,
) -> None:
    events = table(
        "reflexr_events",
        column("tenant_id"),
        column("workspace_id"),
        column("seq"),
        column("event_id"),
        column("envelope", JSON()),
    )
    stored = [
        _stored(1, ServiceError(service="auth")),
        _stored(2, Deploy(service="auth")),
        _stored(3, UnknownEvent(unknown_type="pager.sent")),
    ]
    async with engine.begin() as connection:
        await connection.run_sync(lambda c: command.upgrade(_alembic_config(c), "0002"))
        await connection.execute(insert(events), stored)
    await migrate(engine)
    async with engine.connect() as connection:
        query = select(EventRow.seq, EventRow.event_type).order_by(EventRow.seq)
        assert (await connection.execute(query)).all() == [
            (1, "service.error"),
            (2, "deploy.finished"),
            (3, "pager.sent"),
        ]
    storage = SqlStorage(engine)
    [deploy] = await storage.read(WorkspaceRef("acme", "prod"), types={"deploy.finished"})
    assert deploy.seq == 2
    async with engine.begin() as connection:
        await connection.run_sync(lambda c: command.downgrade(_alembic_config(c), "0002"))
        assert await connection.scalar(select(func.count()).select_from(events)) == 3, (
            "the rows outlive the column"
        )
