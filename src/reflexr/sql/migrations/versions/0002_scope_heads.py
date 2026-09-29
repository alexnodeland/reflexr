"""Index runs by scope and status, so due runs are the first of each scope.

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-29
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade the schema."""
    op.create_index(
        "ix_reflexr_runs_scope_head",
        "reflexr_runs",
        ["tenant_id", "workspace_id", "rule", "scope_key", "status", "fired_seq", "position"],
    )


def downgrade() -> None:
    """Downgrade the schema."""
    op.drop_index("ix_reflexr_runs_scope_head", table_name="reflexr_runs")
