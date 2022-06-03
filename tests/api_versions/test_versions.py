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
import pytest_asyncio

pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(autouse=True)
def test_db(db_session):
    yield


async def test_publish_version_should_start_background_task_and_return_200(client, httpx_mock, mock_minio, dataset):
    dataset_id = str(dataset.id)
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'

    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search?'
            f'recursive=true&zone=1&container_code={dataset.code}&page_size=100000'
        ),
        json={
            'result': [
                {
                    'id': file_geid,
                    'parent': dataset_id,
                    'type': 'file',
                    'parent_path': 'http://anything.com/bucket/obj/path',
                    'storage': {'location_uri': 'http://anything.com/bucket/obj/path'},
                }
            ],
        },
    )

    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )
    httpx_mock.add_response(
        method='POST',
        url='http://data_ops_util/v2/resource/lock/',
        json={},
    )
    httpx_mock.add_response(
        method='DELETE',
        url='http://data_ops_util/v2/resource/lock/',
        json={},
    )

    payload = {'operator': 'admin', 'notes': 'testing', 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_id}/publish', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['status_id'] == dataset_id

    # Test status
    res = await client.get(f'/v1/dataset/{dataset_id}/publish/status?status_id={dataset_id}')
    assert res.json()['result']['status'] == 'success'


async def test_publish_version_with_large_notes_should_return_400(client, mock_minio, dataset):
    dataset_id = str(dataset.id)
    payload = {'operator': 'admin', 'notes': ''.join(['12345' for i in range(60)]), 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_id}/publish', json=payload)
    assert res.status_code == 400
    assert res.json()['result'] == 'Notes is to large, limit 250 bytes'


async def test_publish_version_with_incorrect_notes_should_return_400(client, mock_minio, dataset):
    dataset_id = str(dataset.id)
    payload = {'operator': 'admin', 'notes': 'test', 'version': 'incorrect'}
    res = await client.post(f'/v1/dataset/{dataset_id}/publish', json=payload)
    assert res.status_code == 400
    assert res.json()['result'] == 'Incorrect version format'


async def test_publish_version_duplicate_should_return_409(client, mock_minio, version):
    dataset_id = version['dataset_geid']
    payload = {'operator': 'admin', 'notes': 'test', 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_id}/publish', json=payload)
    assert res.status_code == 409
    assert res.json()['result'] == 'Duplicate version found for dataset'


async def test_version_list_should_return_200_and_version_in_result(client, version):
    dataset_id = version['dataset_geid']
    payload = {}
    res = await client.get(f'/v1/dataset/{dataset_id}/versions', json=payload)
    assert res.status_code == 200
    assert res.json()['result'][0] == version


async def test_version_not_published_to_dataset_should_return_404(client, dataset):
    dataset_id = str(dataset.id)
    payload = {'version': '2.0'}
    res = await client.get(f'/v1/dataset/{dataset_id}/download/pre', query_string=payload)
    assert res.status_code, 200
    assert res.json() == {
        'code': 404,
        'error_msg': 'No published version found',
        'page': 0,
        'total': 1,
        'num_of_pages': 1,
        'result': [],
    }


async def test_version_list_should_return_200_and_download_hash_as_str(client, version):
    dataset_id = version['dataset_geid']
    payload = {'version': '2.0'}
    res = await client.get(f'/v1/dataset/{dataset_id}/download/pre', query_string=payload)
    assert res.status_code, 200
    assert isinstance(res.json()['result']['download_hash'], str)


async def test_publish_version_when_dataset_not_found_should_return_404(client, mock_minio):
    dataset_id = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    payload = {'operator': 'admin', 'notes': 'test', 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_id}/publish', json=payload)
    assert res.status_code, 404
    assert res.json()['error_msg'], 'Dataset not found'


async def test_version_publish_status_not_found_should_return_404(client):
    dataset_id = str(uuid4())
    res = await client.get(f'/v1/dataset/{dataset_id}/publish/status?status_id={dataset_id}')
    assert res.status_code, 404
    assert res.json()['error_msg'], 'Status not found'
