"""add filtered_signals

Revision ID: c1a2b3d4e5f6
Revises: 0bc68d81f3da
Create Date: 2026-07-22 17:30:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "c1a2b3d4e5f6"
down_revision: Union[str, None] = "0bc68d81f3da"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "filtered_signals",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("exchange", sa.String(length=32), nullable=False),
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_filtered_signals_timestamp"),
        "filtered_signals",
        ["timestamp"],
        unique=False,
    )
    op.create_index(
        op.f("ix_filtered_signals_symbol"),
        "filtered_signals",
        ["symbol"],
        unique=False,
    )
    op.create_index(
        op.f("ix_filtered_signals_stage"),
        "filtered_signals",
        ["stage"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(op.f("ix_filtered_signals_stage"), table_name="filtered_signals")
    op.drop_index(op.f("ix_filtered_signals_symbol"), table_name="filtered_signals")
    op.drop_index(op.f("ix_filtered_signals_timestamp"), table_name="filtered_signals")
    op.drop_table("filtered_signals")
