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
"""legacy tables.

Revision ID: 61f23e197e20
Revises:
Create Date: 2022-05-06 11:54:58.190003
"""
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '61f23e197e20'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        'bids_results',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dataset_geid', sa.String(), nullable=True),
        sa.Column('created_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('updated_time', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('validate_output', sa.JSON(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='dataset',
    )
    op.create_table(
        'dataset_schema_template',
        sa.Column('geid', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('dataset_geid', sa.String(), nullable=True),
        sa.Column('standard', sa.String(), nullable=True),
        sa.Column('system_defined', sa.Boolean(), nullable=True),
        sa.Column('is_draft', sa.Boolean(), nullable=True),
        sa.Column('content', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('create_timestamp', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('update_timestamp', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('creator', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('geid'),
        schema='dataset',
    )
    op.create_table(
        'dataset_version',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('dataset_code', sa.String(), nullable=True),
        sa.Column('dataset_geid', sa.String(), nullable=True),
        sa.Column('version', sa.String(), nullable=True),
        sa.Column('created_by', sa.String(), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('location', sa.String(), nullable=True),
        sa.Column('notes', sa.String(), nullable=True),
        sa.PrimaryKeyConstraint('id'),
        schema='dataset',
    )
    op.create_table(
        'dataset_schema',
        sa.Column('geid', sa.String(), nullable=False),
        sa.Column('name', sa.String(), nullable=True),
        sa.Column('dataset_geid', sa.String(), nullable=True),
        sa.Column('tpl_geid', sa.String(), nullable=True),
        sa.Column('standard', sa.String(), nullable=True),
        sa.Column('system_defined', sa.Boolean(), nullable=True),
        sa.Column('is_draft', sa.Boolean(), nullable=True),
        sa.Column('content', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column('create_timestamp', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('update_timestamp', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('creator', sa.String(), nullable=True),
        sa.ForeignKeyConstraint(
            ['tpl_geid'],
            ['dataset.dataset_schema_template.geid'],
        ),
        sa.PrimaryKeyConstraint('geid'),
        schema='dataset',
    )
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('dataset_schema', schema='dataset')
    op.drop_table('dataset_version', schema='dataset')
    op.drop_table('dataset_schema_template', schema='dataset')
    op.drop_table('bids_results', schema='dataset')
    # ### end Alembic commands ###