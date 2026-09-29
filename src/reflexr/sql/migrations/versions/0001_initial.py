"""The initial schema.

Revision ID: 0001
Revises:
Create Date: 2026-09-28
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0001"
down_revision: str | Sequence[str] | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLES = (
    "leases",
    "schedules",
    "dead_letters",
    "scope_states",
    "rule_progress",
    "runs",
    "events",
    "workspaces",
)
"""Every table, in the order the downgrade drops them."""


def _scope() -> list[sa.Column[str]]:
    return [
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
    ]


def upgrade() -> None:
    """Upgrade the schema."""
    op.create_table(
        "reflexr_workspaces",
        *_scope(),
        sa.Column("head_seq", sa.BigInteger(), nullable=False),
        sa.Column("head_ts", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_position", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "workspace_id", name=op.f("pk_reflexr_workspaces")),
    )
    op.create_table(
        "reflexr_events",
        *_scope(),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.Column("event_id", sa.String(), nullable=False),
        sa.Column("envelope", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "workspace_id", "seq", name=op.f("pk_reflexr_events")),
        sa.UniqueConstraint("tenant_id", "workspace_id", "event_id", name="uq_reflexr_events_id"),
    )
    op.create_table(
        "reflexr_runs",
        *_scope(),
        sa.Column("id", sa.String(), nullable=False),
        sa.Column("position", sa.BigInteger(), nullable=False),
        sa.Column("rule", sa.String(), nullable=False),
        sa.Column("scope_key", sa.String(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("fired_seq", sa.BigInteger(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("body", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "workspace_id", "id", name=op.f("pk_reflexr_runs")),
    )
    op.create_index(
        "ix_reflexr_runs_position", "reflexr_runs", ["tenant_id", "workspace_id", "position"]
    )
    op.create_index(
        "ix_reflexr_runs_scope",
        "reflexr_runs",
        ["tenant_id", "workspace_id", "rule", "scope_key", "fired_seq"],
    )
    op.create_index("ix_reflexr_runs_due", "reflexr_runs", ["status", "next_attempt_at"])
    op.create_table(
        "reflexr_rule_progress",
        *_scope(),
        sa.Column("rule", sa.String(), nullable=False),
        sa.Column("position", sa.BigInteger(), nullable=False),
        sa.Column("body", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "workspace_id", "rule", name=op.f("pk_reflexr_rule_progress")
        ),
    )
    op.create_table(
        "reflexr_scope_states",
        *_scope(),
        sa.Column("rule", sa.String(), nullable=False),
        sa.Column("scope_key", sa.String(), nullable=False),
        sa.Column("body", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "workspace_id",
            "rule",
            "scope_key",
            name=op.f("pk_reflexr_scope_states"),
        ),
    )
    op.create_table(
        "reflexr_dead_letters",
        *_scope(),
        sa.Column("position", sa.BigInteger(), nullable=False),
        sa.Column("rule", sa.String(), nullable=False),
        sa.Column("body", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "workspace_id", "position", name=op.f("pk_reflexr_dead_letters")
        ),
    )
    op.create_index(
        "ix_reflexr_dead_letters_rule",
        "reflexr_dead_letters",
        ["tenant_id", "workspace_id", "rule", "position"],
    )
    op.create_table(
        "reflexr_schedules",
        *_scope(),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("position", sa.BigInteger(), nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "workspace_id", "name", name=op.f("pk_reflexr_schedules")
        ),
    )
    op.create_table(
        "reflexr_leases",
        *_scope(),
        sa.Column("key", sa.String(), nullable=False),
        sa.Column("holder", sa.String(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "workspace_id", "key", name=op.f("pk_reflexr_leases")),
    )


def downgrade() -> None:
    """Downgrade the schema."""
    for table in _TABLES:
        op.drop_table(f"reflexr_{table}")
