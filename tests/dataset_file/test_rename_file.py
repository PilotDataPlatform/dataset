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


async def test_rename_file_should_add_file_to_processing_and_return_200(client, httpx_mock, mock_minio, dataset):
    dataset_geid = str(dataset.id)
    file_id = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    file_dict = {
        'id': file_id,
        'parent': None,
        'parent_path': None,
        'restore_path': None,
        'archived': None,
        'type': 'file',
        'zone': 1,
        'name': '164132046.png',
        'size': 2801,
        'owner': 'admin',
        'container_code': 'testdataset202201111',
        'container_type': 'dataset',
        'created_time': '2022-03-04 20:31:11.040611',
        'last_updated_time': '2022-03-04 20:31:11.040872',
        'storage': {
            'id': '1d6a6897-ff0a-4bb3-96ae-e54ee9d379c3',
            'location_uri': 'minio://http://minio.minio:9000/testdataset202201111/data/164132046.png',
            'version': None,
        },
    }
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            f'recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': [file_dict]},
    )

    # background http calls
    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )
    httpx_mock.add_response(
        method='POST',
        url='http://data_ops_util/v1/tasks/',
        json={},
    )
    httpx_mock.add_response(
        method='PUT',
        url='http://data_ops_util/v1/tasks/',
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
    payload = {'new_name': 'new_name', 'operator': 'admin'}
    res = await client.post(
        f'/v1/dataset/{dataset_geid}/files/{file_id}',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    result = res.json()['result']
    assert result['ignored'] == []
    assert result['processing'] == [{**file_dict, 'feedback': 'exist'}]
    assert res.status_code == 200


async def test_rename_file_should_add_file_to_ignoring_when_file_wrong_and_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_id = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            f'recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': []},
    )

    payload = {'new_name': 'new_name', 'operator': 'admin'}
    res = await client.post(
        f'/v1/dataset/{dataset_geid}/files/{file_id}',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    result = res.json()['result']
    assert result['ignored'] == [
        {
            'id': file_id,
            'feedback': 'unauthorized',
        }
    ]
    assert result['processing'] == []
    assert res.status_code == 200


async def test_rename_file_should_add_file_to_ignoring_when_file_duplicated_and_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_id = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    file_dict = {
        'id': file_id,
        'parent': None,
        'parent_path': None,
        'restore_path': None,
        'archived': None,
        'type': 'file',
        'zone': 1,
        'name': '164132046.png',
        'size': 2801,
        'owner': 'admin',
        'container_code': 'testdataset202201111',
        'container_type': 'dataset',
        'created_time': '2022-03-04 20:31:11.040611',
        'last_updated_time': '2022-03-04 20:31:11.040872',
        'storage': {
            'id': '1d6a6897-ff0a-4bb3-96ae-e54ee9d379c3',
            'location_uri': 'minio://http://minio.minio:9000/testdataset202201111/data/164132046.png',
            'version': None,
        },
    }
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            f'recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={
            'result': [
                file_dict,
                {
                    'id': 'f496d010-cb41-4353-bc5b-88f8e03c434d',
                    'name': 'new_name',
                    'parent': None,
                    'parent_path': None,
                    'type': 'file',
                },
            ]
        },
    )

    payload = {'new_name': 'new_name', 'operator': 'admin'}
    res = await client.post(
        f'/v1/dataset/{dataset_geid}/files/{file_id}',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    result = res.json()['result']
    assert result['ignored'] == [{**file_dict, 'feedback': 'exist'}]
    assert result['processing'] == []
    assert res.status_code == 200
