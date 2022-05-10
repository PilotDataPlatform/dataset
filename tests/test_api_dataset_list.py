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


async def test_get_dataset_list_when_error_should_return_500(client, httpx_mock):
    username = 'admin'
    res = await client.post(f'/v1/users/{username}/datasets', json={'order_by': 'desc'})
    assert res.status_code == 500
    assert res.json() == {
        'code': 500,
        'error_msg': 'Neo4j error: No response can be found for POST request on '
        'http://neo4j_service/v2/neo4j/nodes/query',
        'num_of_pages': 1,
        'page': 0,
        'result': [],
        'total': 1,
    }


async def test_get_dataset_list_should_return_200(client, httpx_mock):
    username = 'admin'
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v2/neo4j/nodes/query',
        json={
            'result': [],
            'total': 1,
        },
    )
    res = await client.post(f'/v1/users/{username}/datasets', json={'order_by': 'desc'})
    assert res.status_code == 200
    assert res.json() == {'code': 200, 'error_msg': '', 'num_of_pages': 1, 'page': 0, 'result': [], 'total': 1}
