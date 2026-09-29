"""reflexr's tables, as SQLAlchemy declarative models.

Every table's primary key starts with the tenant and the workspace, so every row belongs to one
tenant's workspace and every query is scoped by it (ADR-0016). Envelopes, runs, rule progress,
scope states and dead letters are stored as the JSON of their Pydantic models, next to the
columns that reads filter and order by. The type is ``JSON`` everywhere, not PostgreSQL's
``JSONB``, which would reorder the keys of dicts in events and states. Table names start with
``reflexr_`` so they can share a database with the application's own tables.
"""

from datetime import UTC, datetime
from typing import Any, override

from sqlalchemy import (
    JSON,
    BigInteger,
    DateTime,
    Dialect,
    Index,
    MetaData,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

VERSION_TABLE = "reflexr_alembic_version"
"""Where Alembic records reflexr's schema version, apart from the application's own."""


class Base(DeclarativeBase):
    """The declarative base of reflexr's tables."""

    metadata = MetaData(naming_convention={"pk": "pk_%(table_name)s"})


metadata = Base.metadata
"""The schema the migrations build, and that :func:`reflexr.sql.create_schema` creates."""


class UtcDateTime(TypeDecorator[datetime]):
    """A point in time, stored and read back in UTC.

    SQLite keeps timestamps as text without their zone and compares them as text, so every
    value is converted to UTC before it is stored. PostgreSQL returns ``timestamptz`` values in
    UTC already, and SQLite returns them without a zone, so reads attach UTC on both.
    """

    impl = DateTime(timezone=True)
    cache_ok = True

    @override
    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        return None if value is None else value.astimezone(UTC)

    @override
    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        return None if value is None else value.replace(tzinfo=UTC)


class ScopedRow(Base):
    """A row that belongs to one tenant's workspace."""

    __abstract__ = True

    # Negative sort orders put these columns first, so they lead every primary key and a
    # workspace's rows are one range of its index.
    tenant_id: Mapped[str] = mapped_column(primary_key=True, sort_order=-3)
    workspace_id: Mapped[str] = mapped_column(primary_key=True, sort_order=-2)


class WorkspaceRow(ScopedRow):
    """A workspace: the row every transaction locks, and its counters."""

    __tablename__ = "reflexr_workspaces"

    head_seq: Mapped[int] = mapped_column(BigInteger)
    """The ``seq`` of the log's latest envelope, or 0."""

    head_ts: Mapped[datetime | None] = mapped_column(UtcDateTime)
    """The ``ts`` of the log's latest envelope, which the next one's cannot precede."""

    last_position: Mapped[int] = mapped_column(BigInteger)
    """The position given to the most recently created row, or 0."""


class EventRow(ScopedRow):
    """One envelope in a workspace's log."""

    __tablename__ = "reflexr_events"
    __table_args__ = (
        UniqueConstraint("tenant_id", "workspace_id", "event_id", name="uq_reflexr_events_id"),
    )

    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[str]
    envelope: Mapped[dict[str, Any]] = mapped_column(JSON)


class RunRow(ScopedRow):
    """A run, with the columns its reads filter and order by."""

    __tablename__ = "reflexr_runs"
    __table_args__ = (
        Index("ix_reflexr_runs_position", "tenant_id", "workspace_id", "position"),
        Index(
            "ix_reflexr_runs_scope", "tenant_id", "workspace_id", "rule", "scope_key", "fired_seq"
        ),
        # Due runs are found across workspaces.
        Index("ix_reflexr_runs_due", "status", "next_attempt_at"),
    )

    id: Mapped[str] = mapped_column(primary_key=True)
    position: Mapped[int] = mapped_column(BigInteger)
    """Creation order within the workspace, from :attr:`WorkspaceRow.last_position`."""

    rule: Mapped[str]
    scope_key: Mapped[str]
    status: Mapped[str]
    fired_seq: Mapped[int] = mapped_column(BigInteger)
    next_attempt_at: Mapped[datetime] = mapped_column(UtcDateTime)
    body: Mapped[dict[str, Any]] = mapped_column(JSON)


class ProgressRow(ScopedRow):
    """A rule's progress in a workspace: its cursor, generation and deadlines."""

    __tablename__ = "reflexr_rule_progress"

    rule: Mapped[str] = mapped_column(primary_key=True)
    position: Mapped[int] = mapped_column(BigInteger)
    """Creation order within the workspace."""

    body: Mapped[dict[str, Any]] = mapped_column(JSON)


class StateRow(ScopedRow):
    """What a rule remembers about one scope."""

    __tablename__ = "reflexr_scope_states"

    rule: Mapped[str] = mapped_column(primary_key=True)
    scope_key: Mapped[str] = mapped_column(primary_key=True)
    body: Mapped[dict[str, Any]] = mapped_column(JSON)


class DeadLetterRow(ScopedRow):
    """An envelope a rule could not evaluate."""

    __tablename__ = "reflexr_dead_letters"
    __table_args__ = (
        Index("ix_reflexr_dead_letters_rule", "tenant_id", "workspace_id", "rule", "position"),
    )

    position: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    rule: Mapped[str]
    body: Mapped[dict[str, Any]] = mapped_column(JSON)


class ScheduleRow(ScopedRow):
    """The time of a schedule's last tick in a workspace."""

    __tablename__ = "reflexr_schedules"

    name: Mapped[str] = mapped_column(primary_key=True)
    position: Mapped[int] = mapped_column(BigInteger)
    """Creation order within the workspace."""

    at: Mapped[datetime] = mapped_column(UtcDateTime)


class LeaseRow(ScopedRow):
    """An exclusive, expiring claim on a key, such as a workspace's evaluation or a run."""

    __tablename__ = "reflexr_leases"

    key: Mapped[str] = mapped_column(primary_key=True)
    holder: Mapped[str]
    expires_at: Mapped[datetime] = mapped_column(UtcDateTime)
