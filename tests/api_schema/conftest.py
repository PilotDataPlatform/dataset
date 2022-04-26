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


@pytest.fixture(autouse=True)
def test_db(db_session):
    yield


@pytest.fixture
def schema_template(db_session):
    from app.models.schema_sql import DatasetSchemaTemplate

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    schema_template_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'

    new_template = DatasetSchemaTemplate(
        geid=schema_template_geid,
        dataset_geid=dataset_geid,
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
def schema(schema_template, db_session):
    from app.models.schema_sql import DatasetSchema

    db_session = db_session

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    schema_template_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'
    schema = DatasetSchema(
        geid='ef4eb37d-6d81-46a7-a9d9-db71bf44edc7',
        name='unittestdataset',
        dataset_geid=dataset_geid,
        tpl_geid=schema_template_geid,
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
def essential_schema(schema_template, db_session):
    from app.config import ConfigClass
    from app.models.schema_sql import DatasetSchema

    db_session = db_session

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    schema_template_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'
    schema = DatasetSchema(
        geid=str(uuid4()),
        name=ConfigClass.ESSENTIALS_NAME,
        dataset_geid=dataset_geid,
        tpl_geid=schema_template_geid,
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
