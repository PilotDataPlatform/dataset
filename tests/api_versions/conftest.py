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

import pytest


@pytest.fixture(autouse=True)
def test_db(db_session):
    yield


@pytest.fixture
def version(db_session):
    from app.models.version_sql import DatasetVersion

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    new_version = DatasetVersion(
        dataset_code='dataset_code',
        dataset_geid=dataset_geid,
        version='2.0',
        created_by='admin',
        location='minio_location',
        notes='test',
    )
    db_session.add(new_version)
    db_session.commit()
    yield new_version.to_dict()
    db_session.delete(new_version)
    db_session.commit()
