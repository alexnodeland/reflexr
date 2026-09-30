"""The packaged migrations build exactly the schema the models describe."""

from typing import Any

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy import JSON, Connection, column, func, insert, inspect, select, table
from sqlalchemy.ext.asyncio import AsyncEngine

from reflexr.core import Envelope, Event, RunSkipped, SourceActor, Tick, UnknownEvent
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
        _stored(3, UnknownEvent(unknown_type="app:pager.sent")),
    ]
    async with engine.begin() as connection:
        await connection.run_sync(lambda c: command.upgrade(_alembic_config(c), "0002"))
        await connection.execute(insert(events), stored)
    await migrate(engine)
    async with engine.connect() as connection:
        query = select(EventRow.seq, EventRow.event_type).order_by(EventRow.seq)
        assert (await connection.execute(query)).all() == [
            (1, "app:service.error"),
            (2, "app:deploy.finished"),
            (3, "app:pager.sent"),
        ]
    storage = SqlStorage(engine)
    [deploy] = await storage.read(WorkspaceRef("acme", "prod"), types={"app:deploy.finished"})
    assert deploy.seq == 2
    async with engine.begin() as connection:
        await connection.run_sync(lambda c: command.downgrade(_alembic_config(c), "0002"))
        assert await connection.scalar(select(func.count()).select_from(events)) == 3, (
            "the rows outlive the column"
        )


def _qualify_facts(connection: Connection, step: str) -> None:
    """Run migration 0005's ``upgrade`` or ``downgrade`` by itself, a row per batch."""
    script = ScriptDirectory.from_config(_alembic_config(connection)).get_revision("0005")
    assert script is not None
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(script.module, "_BATCH", 1)
        with Operations.context(MigrationContext.configure(connection)):
            getattr(script.module, step)()


async def test_upgrading_qualifies_reflexrs_own_event_types(engine: AsyncEngine) -> None:
    events = table(
        "reflexr_events",
        column("tenant_id"),
        column("workspace_id"),
        column("seq"),
        column("event_id"),
        column("event_type"),
        column("envelope", JSON()),
    )

    def stored_as_0004(seq: int, event: Event, name: str, workspace: str = "prod") -> Any:
        row = _stored(seq, event)
        row["envelope"]["event"]["type"] = name
        return {**row, "workspace_id": workspace, "event_type": name}

    tick = Tick(schedule="clock", at=START)
    stored = [
        stored_as_0004(1, tick, "tick"),
        stored_as_0004(2, ServiceError(service="auth"), "app:service.error"),
        stored_as_0004(3, RunSkipped(run_id="fir_1", rule="app:triage"), "run_skipped"),
        stored_as_0004(1, tick, "tick", workspace="dev"),
    ]
    async with engine.begin() as connection:
        await connection.run_sync(lambda c: command.upgrade(_alembic_config(c), "0004"))
        await connection.execute(insert(events), stored)
        await connection.run_sync(_qualify_facts, "upgrade")
    storage = SqlStorage(engine)
    envelopes = await storage.read(WorkspaceRef("acme", "prod"))
    assert [e.event_type for e in envelopes] == [
        "reflexr:tick",
        "app:service.error",
        "reflexr:run_skipped",
    ]
    assert envelopes[0].event == tick
    [skipped] = await storage.read(WorkspaceRef("acme", "prod"), types={"reflexr:run_skipped"})
    assert skipped.seq == 3
    query = select(events.c.workspace_id, events.c.event_type, events.c.envelope).order_by(
        events.c.workspace_id, events.c.seq
    )
    async with engine.begin() as connection:
        rows = (await connection.execute(query)).all()
        assert [(w, t, e["event"]["type"]) for w, t, e in rows] == [
            ("dev", "reflexr:tick", "reflexr:tick"),
            ("prod", "reflexr:tick", "reflexr:tick"),
            ("prod", "app:service.error", "app:service.error"),
            ("prod", "reflexr:run_skipped", "reflexr:run_skipped"),
        ]
        await connection.run_sync(_qualify_facts, "downgrade")
        rows = (await connection.execute(query)).all()
    assert [(w, t, e["event"]["type"]) for w, t, e in rows] == [
        ("dev", "tick", "tick"),
        ("prod", "tick", "tick"),
        ("prod", "app:service.error", "app:service.error"),
        ("prod", "run_skipped", "run_skipped"),
    ]
