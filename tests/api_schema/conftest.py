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

from uuid import uuid4

import pytest

from app.config import ConfigClass
from app.models.schema import DatasetSchema
from app.models.schema import DatasetSchemaTemplate


@pytest.fixture(autouse=True)
def test_db(db_session):
    yield


@pytest.fixture
def schema_template(db_session, dataset):
    new_template = DatasetSchemaTemplate(
        geid=str(uuid4()),
        dataset_geid=dataset.id,
        name='test_schema_template',
        standard='default',
        system_defined=True,
        is_draft=True,
        content={},
        creator='admin',
    )
    db_session.add(new_template)
    db_session.commit()
    yield new_template.to_dict()
    db_session.delete(new_template)
    db_session.commit()


@pytest.fixture
def schema(schema_template, db_session, dataset):
    schema = DatasetSchema(
        geid=str(uuid4()),
        name='unittestdataset',
        dataset_geid=dataset.id,
        tpl_geid=schema_template['geid'],
        standard='default',
        system_defined=True,
        is_draft=False,
        content={},
        creator='admin',
    )
    db_session.add(schema)
    db_session.commit()
    yield schema.to_dict()
    db_session.delete(schema)
    db_session.commit()


@pytest.fixture
def essential_schema(schema_template, db_session, dataset):
    schema = DatasetSchema(
        geid=str(uuid4()),
        name=ConfigClass.ESSENTIALS_NAME,
        dataset_geid=dataset.id,
        tpl_geid=schema_template['geid'],
        standard='default',
        system_defined=True,
        is_draft=False,
        content={},
        creator='admin',
    )
    db_session.add(schema)
    db_session.commit()
    yield schema.to_dict()
    db_session.delete(schema)
    db_session.commit()
