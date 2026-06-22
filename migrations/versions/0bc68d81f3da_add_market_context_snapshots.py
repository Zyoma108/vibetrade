"""add market_context_snapshots

Revision ID: 0bc68d81f3da
Revises: 476939c77bf1
Create Date: 2026-06-16 23:15:00.000000

"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0bc68d81f3da"
down_revision: Union[str, None] = "476939c77bf1"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "market_context_snapshots",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("timestamp", sa.DateTime(), nullable=False),
        sa.Column("regime", sa.String(length=16), nullable=False),
        sa.Column("regime_start", sa.DateTime(), nullable=False),
        sa.Column("trend", sa.String(length=16), nullable=False),
        sa.Column("trend_start", sa.DateTime(), nullable=False),
        sa.Column("supertrend_color", sa.String(length=8), nullable=False),
        sa.Column("btc_change_1h", sa.Float(), nullable=False),
        sa.Column("btc_change_4h", sa.Float(), nullable=False),
        sa.Column("others_value", sa.Float(), nullable=False),
        sa.Column("others_change_1h", sa.Float(), nullable=False),
        sa.Column("others_change_4h", sa.Float(), nullable=False),
        sa.Column("ready", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_market_context_snapshots_timestamp"),
        "market_context_snapshots",
        ["timestamp"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_market_context_snapshots_timestamp"),
        table_name="market_context_snapshots",
    )
    op.drop_table("market_context_snapshots")
