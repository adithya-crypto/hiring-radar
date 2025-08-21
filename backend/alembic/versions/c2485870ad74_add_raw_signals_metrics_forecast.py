"""add_raw_signals_metrics_forecast

Revision ID: c2485870ad74
Revises: 46bdf821611c
Create Date: 2025-08-18 23:59:59
"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "c2485870ad74"
down_revision = "46bdf821611c"
branch_labels = None
depends_on = None

def _table_exists(conn, name: str) -> bool:
    insp = sa.inspect(conn)
    return name in insp.get_table_names()

def _column_exists(conn, table: str, column: str) -> bool:
    insp = sa.inspect(conn)
    return column in [c["name"] for c in insp.get_columns(table)]

def upgrade():
    conn = op.get_bind()

    # 1) companies.linkedin_url (new optional column)
    if not _column_exists(conn, "companies", "linkedin_url"):
        op.add_column("companies", sa.Column("linkedin_url", sa.String(), nullable=True))

    # 2) sources (NEW)
    if not _table_exists(conn, "sources"):
        op.create_table(
            "sources",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("company_id", sa.Integer, sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
            sa.Column("kind", sa.String, nullable=False),          # greenhouse | lever | ashby | smartrecruiters
            sa.Column("endpoint_url", sa.String, nullable=False),
            sa.Column("auth_kind", sa.String, nullable=True),
            sa.Column("last_ok_at", sa.DateTime(timezone=True), nullable=True),
        )

    # 3) job_raw (NEW)
    if not _table_exists(conn, "job_raw"):
        op.create_table(
            "job_raw",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("company_id", sa.Integer, sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
            sa.Column("source_id", sa.Integer, sa.ForeignKey("sources.id", ondelete="SET NULL"), nullable=True),
            sa.Column("fetched_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
            sa.Column("payload_json", sa.JSON, nullable=False),
        )

    # 4) signals – already created in 46bdf821611c (skip)

    # 5) job_metrics – already created in 46bdf821611c (skip)

    # 6) forecast (NEW)
    if not _table_exists(conn, "forecast"):
        op.create_table(
            "forecast",
            sa.Column("id", sa.Integer, primary_key=True),
            sa.Column("company_id", sa.Integer, sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
            sa.Column("role_family", sa.String, nullable=False),
            sa.Column("computed_at", sa.DateTime(timezone=True), server_default=sa.text("now()")),
            sa.Column("prob_next_8w", sa.Float, nullable=False),
            sa.Column("likely_month", sa.String, nullable=False),
            sa.Column("method", sa.String, nullable=True),
            sa.Column("ci_low", sa.Float, nullable=True),
            sa.Column("ci_high", sa.Float, nullable=True),
            sa.Column("features_json", sa.JSON, nullable=True),
        )

def downgrade():
    conn = op.get_bind()

    if _table_exists(conn, "forecast"):
        op.drop_table("forecast")

    if _table_exists(conn, "job_raw"):
        op.drop_table("job_raw")

    if _table_exists(conn, "sources"):
        op.drop_table("sources")

    if _column_exists(conn, "companies", "linkedin_url"):
        op.drop_column("companies", "linkedin_url")
