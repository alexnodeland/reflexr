"""Qualify reflexr's own event types in the stored log: ``tick`` becomes ``reflexr:tick``.

Every event type is qualified by its namespace (ADR-0039). This renames reflexr's thirteen, in
the ``event_type`` column and in each envelope's ``event.type``; an application's own types are
its to rename, in its own migration. Rows are rewritten in Python, a batch at a time in the order
of the primary key, so the migration is the same on every database: PostgreSQL's only setter for
a JSON path is ``jsonb_set``, and JSONB reorders keys, which the stored envelopes avoid.

Revision ID: 0005
Revises: 0004
Create Date: 2026-09-29
"""

from collections.abc import Sequence
from typing import Any

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | Sequence[str] | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_FACTS = (
    "rule_fired",
    "rule_errored",
    "rule_reset",
    "run_started",
    "run_progressed",
    "run_retrying",
    "run_succeeded",
    "run_dead_lettered",
    "run_cancelled",
    "run_requeued",
    "run_skipped",
    "feedback_given",
    "tick",
)
"""reflexr's event types when this migration was written, as they were stored before it."""

_BATCH = 500

_events = sa.table(
    "reflexr_events",
    sa.column("tenant_id", sa.String()),
    sa.column("workspace_id", sa.String()),
    sa.column("seq", sa.BigInteger()),
    sa.column("event_type", sa.String()),
    sa.column("envelope", sa.JSON()),
)


def upgrade() -> None:
    """Upgrade the schema."""
    _rename({name: f"reflexr:{name}" for name in _FACTS})


def downgrade() -> None:
    """Downgrade the schema."""
    _rename({f"reflexr:{name}": name for name in _FACTS})


def _rename(names: dict[str, str]) -> None:
    connection = op.get_bind()
    key = (_events.c.tenant_id, _events.c.workspace_id, _events.c.seq)
    found = sa.select(
        _events.c.tenant_id,
        _events.c.workspace_id,
        _events.c.seq,
        _events.c.event_type,
        _events.c.envelope,
    ).where(_events.c.event_type.in_(names))
    after: sa.ColumnElement[bool] = sa.true()
    rewrite = (
        _events.update()
        .where(
            _events.c.tenant_id == sa.bindparam("row_tenant"),
            _events.c.workspace_id == sa.bindparam("row_workspace"),
            _events.c.seq == sa.bindparam("row_seq"),
        )
        .values(
            event_type=sa.bindparam("new_type"),
            envelope=sa.bindparam("new_envelope", type_=sa.JSON()),
        )
    )
    while rows := connection.execute(found.where(after).order_by(*key).limit(_BATCH)).all():
        changes: list[dict[str, Any]] = []
        for tenant_id, workspace_id, seq, event_type, envelope in rows:
            renamed = names[event_type]
            changes.append(
                {
                    "row_tenant": tenant_id,
                    "row_workspace": workspace_id,
                    "row_seq": seq,
                    "new_type": renamed,
                    "new_envelope": {**envelope, "event": {**envelope["event"], "type": renamed}},
                }
            )
        connection.execute(rewrite, changes)
        after = sa.tuple_(*key) > sa.tuple_(*rows[-1][:3])
