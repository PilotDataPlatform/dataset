# Copyright (C) 2022 Indoc Research
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Added dataset table.

Revision ID: ad96d8573c01
Revises: 61f23e197e20
Create Date: 2022-05-10 10:49:41.878977
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'ad96d8573c01'
down_revision = '61f23e197e20'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'datasets',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('source', sa.VARCHAR(length=256), nullable=False),
        sa.Column('authors', postgresql.ARRAY(sa.VARCHAR(length=256)), nullable=False),
        sa.Column('code', sa.VARCHAR(length=32), nullable=False),
        sa.Column('type', sa.VARCHAR(length=256), nullable=False),
        sa.Column('modality', postgresql.ARRAY(sa.VARCHAR(length=256)), nullable=True),
        sa.Column('collection_method', postgresql.ARRAY(sa.VARCHAR(length=256)), nullable=True),
        sa.Column('license', sa.VARCHAR(length=256), nullable=True),
        sa.Column('tags', postgresql.ARRAY(sa.VARCHAR(length=256)), nullable=True),
        sa.Column('description', sa.VARCHAR(length=5000), nullable=False),
        sa.Column('size', sa.INTEGER(), nullable=True),
        sa.Column('total_files', sa.INTEGER(), nullable=True),
        sa.Column('title', sa.VARCHAR(length=256), nullable=False),
        sa.Column('creator', sa.VARCHAR(length=256), nullable=False),
        sa.Column('project_id', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('updated_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        schema='dataset',
    )
    op.create_index(op.f('ix_dataset_datasets_code'), 'datasets', ['code'], unique=True, schema='dataset')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f('ix_dataset_datasets_code'), table_name='datasets', schema='dataset')
    op.drop_table('datasets', schema='dataset')
    # ### end Alembic commands ###
