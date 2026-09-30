"""Keep each workspace's stored rules, installed at runtime (RFC-0003).

One row per rule: its current version, whether it is active or archived, and the JSON of the rule
and its provenance. The log's facts hold every version, so downgrading drops the stored rules and
keeps their facts.

Revision ID: 0006
Revises: 0005
Create Date: 2026-09-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | Sequence[str] | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade the schema."""
    op.create_table(
        "reflexr_rules",
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column(
            "name",
            sa.String().with_variant(sa.String(collation="C"), "postgresql"),
            nullable=False,
        ),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("rule", sa.JSON(), nullable=False),
        sa.Column("provenance", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "workspace_id", "name", name=op.f("pk_reflexr_rules")),
    )


def downgrade() -> None:
    """Downgrade the schema."""
    op.drop_table("reflexr_rules")
