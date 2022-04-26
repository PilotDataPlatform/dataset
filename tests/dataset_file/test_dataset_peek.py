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

pytestmark = pytest.mark.asyncio


async def test_get_dataset_peek_should_return_200(client, httpx_mock):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    source_project = '69a0c740-50d2-4b37-b6bb-d1aaa88380ed'
    dataset_code = 'dataset_code'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[{'project_geid': source_project, 'code': dataset_code, 'global_entity_id': dataset_geid}],
    )
    res = await client.get('/v1/dataset-peek/{dataset_code}')
    assert res.status_code == 200


async def test_get_dataset_peek_not_found_should_return_404(client, httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[],
    )
    res = await client.get('/v1/dataset-peek/dataset_code')
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Not Found, invalid dataset code'


async def test_get_dataset_peek_error_should_return_500(client, httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        text='error',
        status_code=500,
    )
    res = await client.get('/v1/dataset-peek/dataset_code')
    assert res.status_code == 500
    assert res.json()['error_msg'] == 'error'
