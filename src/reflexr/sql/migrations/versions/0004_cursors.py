"""Keep the cursors of the log's consumers, such as feedback mirrors (ADR-0040).

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-29
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade the schema."""
    op.create_table(
        "reflexr_cursors",
        sa.Column("tenant_id", sa.String(), nullable=False),
        sa.Column("workspace_id", sa.String(), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("seq", sa.BigInteger(), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id", "workspace_id", "name", name=op.f("pk_reflexr_cursors")
        ),
    )


def downgrade() -> None:
    """Downgrade the schema."""
    op.drop_table("reflexr_cursors")
