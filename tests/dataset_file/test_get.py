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

pytestmark = pytest.mark.asyncio


async def test_get_dataset_files_should_return_404_when_dataset_not_found(client, test_db):
    dataset_id = str(uuid4())
    res = await client.get(f'/v1/dataset/{dataset_id}/files')
    assert res.status_code == 404
    assert res.json() == {
        'code': 404,
        'error_msg': 'Invalid geid for dataset',
        'num_of_pages': 1,
        'page': 0,
        'result': [],
        'total': 1,
    }


async def test_get_dataset_files(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    file = {
        'type': 'file',
        'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
        'id': file_geid,
        'owner': 'me',
        'parent_path': None,
        'parent': None,
        'dataset_code': dataset.code,
    }
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/'
            f'?recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': [file]},
    )

    res = await client.get(f'/v1/dataset/{dataset_geid}/files')
    assert res.status_code == 200
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': {
            'data': [file],
            'route': [],
        },
        'total': 1,
    }
