"""update bids_result table

Revision ID: ca6c52333938
Revises: ad96d8573c01
Create Date: 2022-07-11 11:59:20.714018

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = 'ca6c52333938'
down_revision = 'ad96d8573c01'
branch_labels = None
depends_on = None


def upgrade():
    op.create_unique_constraint(
        "dataset_geid", "bids_results", ["dataset_geid"], schema='dataset',)


def downgrade():
    op.drop_table('bids_results', schema='dataset')
