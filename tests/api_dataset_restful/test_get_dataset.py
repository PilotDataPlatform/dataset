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


async def test_get_dataset_when_dataset_not_found_should_return_404(client, test_db):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    res = await client.get(f'/v1/dataset/{dataset_geid}')
    assert res.status_code == 404
    assert res.json() == {
        'code': 404,
        'error_msg': 'Not Found, invalid geid',
        'num_of_pages': 1,
        'page': 0,
        'result': {},
        'total': 1,
    }


async def test_get_dataset_when_dataset_found_should_return_200(client, test_db, dataset):
    dataset_id = str(dataset.id)
    res = await client.get(f'/v1/dataset/{dataset_id}')
    assert res.status_code == 200
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': {**dataset.to_dict()},
        'total': 1,
    }


async def test_get_dataset_when_exception_should_return_500(
    client,
):
    res = await client.get('/v1/dataset/any')
    assert res.status_code == 500
    assert res.json() == {
        'code': 500,
        'error_msg': 'badly formed hexadecimal UUID string',
        'num_of_pages': 1,
        'page': 0,
        'result': {},
        'total': 1,
    }


async def test_get_dataset_by_code_when_dataset_not_found_should_return_404(client, test_db):
    res = await client.get('/v1/dataset-peek/code')
    assert res.status_code == 404
    assert res.json() == {
        'code': 404,
        'error_msg': 'Not Found, invalid dataset code',
        'num_of_pages': 1,
        'page': 0,
        'result': {},
        'total': 1,
    }


async def test_get_dataset_by_code_when_dataset_found_should_return_200(client, test_db, dataset):
    dataset_code = dataset.code
    res = await client.get(f'/v1/dataset-peek/{dataset_code}')
    assert res.status_code == 200
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': {**dataset.to_dict()},
        'total': 1,
    }


async def test_get_dataset_by_code_when_exception_should_return_500(
    client,
):
    res = await client.get('/v1/dataset-peek/any')
    assert res.status_code == 500
    json_resp = res.json()
    assert json_resp['code'] == 500
    assert json_resp['error_msg']
    assert json_resp['result'] == {}
