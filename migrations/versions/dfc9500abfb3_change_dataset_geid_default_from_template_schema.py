"""change dataset_geid default from template_schema.

Revision ID: dfc9500abfb3
Revises: ca6c52333938
Create Date: 2022-07-13 16:13:22.537504
"""
from alembic import op

# revision identifiers, used by Alembic.
revision = 'dfc9500abfb3'
down_revision = 'ca6c52333938'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('schema_template', 'dataset_geid', nullable=True, schema='dataset')


def downgrade():
    op.execute("UPDATE dataset.schema_template SET dataset_geid = '' where schema_template.dataset_geid is null")
    op.alter_column('schema_template', 'dataset_geid', nullable=False, schema='dataset')
