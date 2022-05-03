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


@pytest.mark.parametrize(
    'response,status_code,error_msg,expected_status_code,expected_result',
    [
        ({}, 200, '', 200, {}),
        (None, 200, 'Not Found, invalid geid', 404, None),
        (None, 500, '[]', 500, []),
    ],
)
async def test_get_dataset_should_should_respect_response(
    response, status_code, error_msg, expected_status_code, expected_result, client, httpx_mock
):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[] if response is None else [response],
        status_code=status_code,
    )
    res = await client.get(f'/v1/dataset/{dataset_geid}')
    assert res.status_code == expected_status_code
    assert res.json() == {
        'code': expected_status_code,
        'error_msg': error_msg,
        'num_of_pages': 1,
        'page': 0,
        'result': expected_result,
        'total': 1,
    }


@pytest.mark.parametrize(
    'response,status_code,error_msg,expected_status_code,expected_result',
    [
        ({}, 200, '', 200, {}),
        (None, 200, 'Not Found, invalid dataset code', 404, None),
        (None, 500, '[]', 500, []),
    ],
)
async def test_get_dataset_by_code_should_respect_response(
    response, status_code, error_msg, expected_status_code, expected_result, client, httpx_mock
):
    code = 'any'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[] if response is None else [response],
        status_code=status_code,
    )
    res = await client.get(f'/v1/dataset-peek/{code}')
    assert res.status_code == expected_status_code
    assert res.json() == {
        'code': expected_status_code,
        'error_msg': error_msg,
        'num_of_pages': 1,
        'page': 0,
        'result': expected_result,
        'total': 1,
    }
