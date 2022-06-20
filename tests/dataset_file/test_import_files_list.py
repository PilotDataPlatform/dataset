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


async def test_import_files_from_source_list_should_return_200(client, httpx_mock, dataset, mock_minio):
    dataset_geid = str(dataset.id)
    source_project = str(dataset.project_id)
    file_id = 'b1064aa6-edbe-4eb6-b560-a8552f2f6162'
    file_dict = {
        'id': file_id,
        'parent': None,
        'parent_path': None,
        'name': '.hidden_file.txt',
        'container_code': 'test202203241',
        'container_type': 'project',
        'type': 'file',
        'storage': {
            'id': 'f2397e68-4e94-4419-bb72-3be532a789b2',
            'location_uri': (
                'minio://http://minio.minio:9000/core-test202203241/admin/test_sub_6'
                ' - Copy/test_sub_delete_6/.hidden_file.txt'
            ),
            'version': None,
        },
    }
    httpx_mock.add_response(
        method='GET',
        url=f'http://project_service/v1/projects/{source_project}',
        json={'code': 'project_code'},
    )
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            'recursive=true&zone=1&container_code=project_code&container_type=project&page_size=100000'
        ),
        json={'result': [file_dict]},
    )
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            f'recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': []},
    )

    # because of the background task
    httpx_mock.add_response(
        method='POST',
        url='http://metadata_service/v1/item/',
        json={'result': {'parent': None}},
    )
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

    payload = {
        'source_list': [
            file_id,
        ],
        'operator': 'admin',
        'project_geid': source_project,
    }
    res = await client.put(
        f'/v1/dataset/{dataset_geid}/files',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    result = res.json()['result']
    assert res.status_code == 200
    assert result.get('ignored') == []
    assert result.get('processing') == [{**file_dict, 'feedback': 'exist'}]


async def test_import_files_from_different_project_return_403(client, dataset):
    dataset_id = str(dataset.id)
    payload = {
        'source_list': [],
        'operator': 'admin',
        'project_geid': 'project_2',
    }
    res = await client.put(f'/v1/dataset/{dataset_id}/files', json=payload)
    assert res.status_code == 403


async def test_import_files_from_non_existing_project_return_404(client, test_db):
    dataset_id = str(uuid4())
    payload = {
        'source_list': [],
        'operator': 'admin',
        'project_geid': 'NOT_EXIST_Project',
    }
    res = await client.put(f'/v1/dataset/{dataset_id}/files', json=payload)
    assert res.status_code == 404


async def test_05_test_import_duplicate(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    source_project = str(dataset.project_id)
    file_id = 'b1064aa6-edbe-4eb6-b560-a8552f2f6162'
    file_dict = {
        'id': file_id,
        'parent': None,
        'parent_path': None,
        'name': '.hidden_file.txt',
        'container_code': 'test202203241',
        'container_type': 'project',
        'type': 'file',
        'storage': {
            'id': 'f2397e68-4e94-4419-bb72-3be532a789b2',
            'location_uri': (
                'minio://http://minio.minio:9000/core-test202203241/admin/test_sub_6'
                ' - Copy/test_sub_delete_6/.hidden_file.txt'
            ),
            'version': None,
        },
    }

    httpx_mock.add_response(
        method='GET',
        url=f'http://project_service/v1/projects/{source_project}',
        json={'code': 'project_code'},
    )
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            'recursive=true&zone=1&container_code=project_code&container_type=project&page_size=100000'
        ),
        json={'result': [file_dict]},
    )
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            f'recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': [file_dict]},
    )

    payload = {
        'source_list': [
            file_id,
        ],
        'operator': 'admin',
        'project_geid': source_project,
    }
    res = await client.put(f'/v1/dataset/{dataset_geid}/files', json=payload)
    result = res.json()['result']
    assert res.status_code == 200
    assert result.get('processing') == []
    assert result.get('ignored') == [{**file_dict, 'feedback': 'duplicate or unauthorized'}]
