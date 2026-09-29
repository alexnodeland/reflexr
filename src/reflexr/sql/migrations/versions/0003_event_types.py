"""Keep each event's type in a column, so reads of some types filter in the database.

The column is filled from the stored envelopes, whose ``event.type`` is the type's name.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0003"
down_revision: str | Sequence[str] | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade the schema."""
    op.add_column("reflexr_events", sa.Column("event_type", sa.String(), nullable=True))
    events = sa.table(
        "reflexr_events", sa.column("envelope", sa.JSON()), sa.column("event_type", sa.String())
    )
    stored = events.c.envelope[("event", "type")].as_string()
    op.execute(events.update().values(event_type=stored))
    # SQLite cannot add NOT NULL to a column, so batch mode copies the table there.
    with op.batch_alter_table("reflexr_events") as batch:
        batch.alter_column("event_type", existing_type=sa.String(), nullable=False)
    op.create_index(
        "ix_reflexr_events_type",
        "reflexr_events",
        ["tenant_id", "workspace_id", "event_type", "seq"],
    )


def downgrade() -> None:
    """Downgrade the schema."""
    op.drop_index("ix_reflexr_events_type", table_name="reflexr_events")
    with op.batch_alter_table("reflexr_events") as batch:
        batch.drop_column("event_type")
