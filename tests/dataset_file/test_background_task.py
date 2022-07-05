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

import json
from unittest import mock
from uuid import uuid4

import pytest
import pytest_asyncio

from app.routers.v1.dataset_file import APIImportData
from app.schemas.activity_log import FileFolderActivityLogSchema
from app.services.activity_log import FileFolderActivityLogService

pytestmark = pytest.mark.asyncio

API = APIImportData()
OPER = 'admin'
SESSION_ID = '12345'
ACCESS_TOKEN = 'token'
REFRESH_TOKEN = 'refresh'

root_file = {'id': str(uuid4()), 'parent': None, 'parent_path': None, 'type': 'file', 'name': 'file.txt'}
root_folder = {'id': str(uuid4()), 'parent': None, 'parent_path': None, 'type': 'folder', 'name': 'folderlvl1'}
children_file = {
    'id': str(uuid4()),
    'parent': str(uuid4()),
    'parent_path': 'folder_lvl1',
    'type': 'file',
    'name': 'file.txt',
}
children_folder = {
    'id': str(uuid4()),
    'parent': str(uuid4()),
    'parent_path': 'folder_lvl1',
    'type': 'folder',
    'name': 'folder_lvl2',
}
grandchild_folder = {
    'id': str(uuid4()),
    'parent': str(uuid4()),
    'parent_path': 'folder_lvl1.folder_lvl2',
    'type': 'folder',
    'name': 'folder_lvl3',
}


@pytest_asyncio.fixture
def external_requests(httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json=[],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://data_ops_util/v1/tasks/',
        json=[],
    )


@mock.patch.object(FileFolderActivityLogService, '_message_send')
@mock.patch('app.routers.v1.dataset_file.recursive_lock_import')
async def test_copy_file_worker_should_import_file_succeed(
    mock_recursive_lock_import, mock_kafka_msg, external_requests, httpx_mock, test_db, dataset
):
    source_project_geid = str(uuid4())

    httpx_mock.add_response(
        method='GET',
        url=f'http://project_service/v1/projects/{source_project_geid}',
        json={
            'id': source_project_geid,
            'name': 'source_project',
            'code': 'source_project_code',
        },
    )
    mock_recursive_lock_import.return_value = [], False
    import_list = [
        {
            'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
            'parent': '077fe46b-3bff-4da3-a4fb-4d6cbf9ce470',
            'parent_path': 'test_folder_6',
            'restore_path': None,
            'archived': False,
            'type': 'file',
            'zone': 1,
            'name': 'Dateidaten_für_vretest3',
            'size': 10485760,
            'owner': 'admin',
            'container_code': 'testdataset202201101',
            'container_type': 'dataset',
            'created_time': '2022-01-10 21:44:28.360324',
            'last_updated_time': '2022-01-10 21:44:28.360541',
            'storage': {
                'id': '286e1bd3-33c8-46d5-97ba-a309407d19ed',
                'location_uri': 'minio://http://10.3.7.220/testdataset202201101/data/Dateidaten_für_vretest3',
                'version': None,
            },
            'extended': {
                'id': 'de6200e2-a30c-4c11-a6ec-3887a545da2a',
                'extra': {'tags': [], 'system_tags': [], 'attributes': {}},
            },
        }
    ]
    with mock.patch.object(APIImportData, 'recursive_copy') as mock_recursive_copy:
        mock_recursive_copy.return_value = 1, 1, None
        try:
            await API.copy_files_worker(
                test_db, import_list, dataset, OPER, source_project_geid, SESSION_ID, ACCESS_TOKEN, REFRESH_TOKEN
            )
        except Exception as e:
            pytest.fail(f'copy_files_worker raised {e} unexpectedly')
    assert mock_kafka_msg.call_count == 1
    file_folder = FileFolderActivityLogSchema.parse_obj(mock_kafka_msg.call_args[0][0])
    assert file_folder.activity_type == 'import'


@mock.patch.object(FileFolderActivityLogService, '_message_send')
@pytest.mark.parametrize(
    'target_folder,item_type',
    [
        ({'id': 'any', 'parent_path': None, 'name': 'any_folder'}, children_file),
        ({'id': None, 'name': None, 'parent_path': None, 'name': None}, children_file),
        ({'id': 'any', 'parent_path': None, 'name': 'any_folder'}, children_folder),
        ({'id': None, 'name': None, 'parent_path': None, 'name': None}, children_folder),
        ({'id': 'any', 'parent_path': None, 'name': 'any_folder'}, grandchild_folder),
        ({'id': None, 'name': None, 'parent_path': None, 'name': None}, grandchild_folder),
        ({'id': 'any', 'parent_path': None, 'name': 'any_folder'}, root_file),
        ({'id': 'any', 'parent_path': None, 'name': 'any_folder'}, root_folder),
    ],
)
async def test_move_file_worker_should_move_file_succeed(
    mock_kafka_msg, external_requests, httpx_mock, test_db, dataset, target_folder, item_type
):
    code = 'testdataset202201101'
    httpx_mock.add_response(
        method='POST',
        url='http://data_ops_util/v2/resource/lock/',
        json=[],
    )
    httpx_mock.add_response(
        method='DELETE',
        url='http://data_ops_util/v2/resource/lock/',
        json=[],
    )
    name = item_type['name']
    if item_type['parent_path']:
        minio_path = item_type['parent_path'].replace('.', '/')
        parent_path = item_type['parent_path']
    else:
        minio_path = name
        parent_path = item_type['name']

    if item_type['type'] == 'folder':
        if item_type['parent_path']:
            minio_path += '/' + name
            parent_path += '.' + name
        httpx_mock.add_response(
            method='GET',
            url=(
                'http://metadata_service/v1/items/search/?'
                f'recursive=true&zone=1&container_code={code}&container_type=dataset&page_size=100000'
            ),
            json={
                'result': [],
            },
        )

    move_list = [
        {
            'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
            'parent': item_type['parent'],
            'parent_path': item_type['parent_path'],
            'restore_path': None,
            'archived': False,
            'type': item_type['type'],
            'name': name,
            'owner': 'admin',
            'container_code': code,
            'container_type': 'dataset',
            'storage': {
                'location_uri': f'minio://http://10.3.7.220/testdataset202201101/data/{minio_path}',
            },
        }
    ]

    with mock.patch.object(APIImportData, 'recursive_copy') as mock_recursive_copy:
        mock_recursive_copy.return_value = 1, 1, None
        with mock.patch.object(APIImportData, 'recursive_delete'):
            try:
                await API.move_file_worker(
                    test_db,
                    move_list,
                    dataset,
                    OPER,
                    target_folder,
                    SESSION_ID,
                    ACCESS_TOKEN,
                    REFRESH_TOKEN,
                )
            except Exception as e:
                pytest.fail(f'copy_files_worker raised {e} unexpectedly')
    locks = []
    unlocks = []

    for request in httpx_mock.get_requests():
        if request.url == 'http://data_ops_util/v2/resource/lock/':
            if request.method == 'POST':
                locks.append(json.loads(request.content)['resource_key'])
            else:
                unlocks.append(json.loads(request.content)['resource_key'])

    if item_type['parent_path']:
        parent_path = item_type['parent_path']
        assert f'{code}/data/{minio_path}' in locks
        assert f'{code}/data/{minio_path}' in unlocks
    else:
        assert f'{code}/data/{name}' in locks
        assert f'{code}/data/{name}' in unlocks

    if target_folder['name']:
        target_name = target_folder['name']
        assert f'{code}/data/{target_name}/{name}' in locks
        assert f'{code}/data/{target_name}/{name}' in unlocks
    else:
        assert f'{code}/data/{name}' in locks
        assert f'{code}/data/{name}' in unlocks

    assert mock_kafka_msg.call_count == 1
    file_folder = FileFolderActivityLogSchema.parse_obj(mock_kafka_msg.call_args[0][0])
    assert file_folder.activity_type == 'update'


@mock.patch.object(FileFolderActivityLogService, '_message_send')
@mock.patch('app.routers.v1.dataset_file.recursive_lock_delete')
async def test_delete_files_work_should_delete_file_succeed(
    mock_recursive_lock_delete, mock_kafka_msg, external_requests, httpx_mock, test_db, dataset
):
    mock_recursive_lock_delete.return_value = [], False
    delete_list = [
        {
            'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
            'parent': '077fe46b-3bff-4da3-a4fb-4d6cbf9ce470',
            'parent_path': 'test_folder_6',
            'restore_path': None,
            'archived': False,
            'type': 'file',
            'zone': 1,
            'name': 'Dateidaten_für_vretest3',
            'size': 10485760,
            'owner': 'admin',
            'container_code': 'testdataset202201101',
            'container_type': 'dataset',
            'created_time': '2022-01-10 21:44:28.360324',
            'last_updated_time': '2022-01-10 21:44:28.360541',
            'storage': {
                'id': '286e1bd3-33c8-46d5-97ba-a309407d19ed',
                'location_uri': 'minio://http://10.3.7.220/testdataset202201101/data/Dateidaten_für_vretest3',
                'version': None,
            },
            'extended': {
                'id': 'de6200e2-a30c-4c11-a6ec-3887a545da2a',
                'extra': {'tags': [], 'system_tags': [], 'attributes': {}},
            },
        }
    ]

    with mock.patch.object(APIImportData, 'recursive_delete') as mock_recursive_delete:
        mock_recursive_delete.return_value = 1, 1
        try:
            await API.delete_files_work(test_db, delete_list, dataset, OPER, SESSION_ID, ACCESS_TOKEN, REFRESH_TOKEN)
        except Exception as e:
            pytest.fail(f'copy_delete_work raised {e} unexpectedly')

    assert mock_kafka_msg.call_count == 1
    file_folder = FileFolderActivityLogSchema.parse_obj(mock_kafka_msg.call_args[0][0])
    assert file_folder.activity_type == 'delete'


@mock.patch.object(FileFolderActivityLogService, '_message_send')
@mock.patch('app.routers.v1.dataset_file.recursive_lock_move_rename')
async def test_rename_file_worker_should_rename_file_succeed(
    mock_recursive_lock_move_rename, mock_kafka_msg, external_requests, httpx_mock, test_db, dataset
):
    mock_recursive_lock_move_rename.return_value = [], False
    httpx_mock.add_response(
        method='PUT',
        url='http://data_ops_util/v1/tasks/',
        json=[],
    )

    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/077fe46b-3bff-4da3-a4fb-4d6cbf9ce470/',
        json={
            'result': {
                'id': '077fe46b-3bff-4da3-a4fb-4d6cbf9ce470',
                'name': 'folder',
                'container_code': 'source_project_code',
                'parent_path': None,
                'parent': None,
            }
        },
    )

    old_file = {
        'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
        'parent': '077fe46b-3bff-4da3-a4fb-4d6cbf9ce470',
        'parent_path': 'test_folder_6',
        'restore_path': None,
        'archived': False,
        'type': 'file',
        'zone': 1,
        'name': 'Dateidaten_für_vretest3',
        'size': 10485760,
        'owner': 'admin',
        'container_code': 'testdataset202201101',
        'container_type': 'dataset',
        'created_time': '2022-01-10 21:44:28.360324',
        'last_updated_time': '2022-01-10 21:44:28.360541',
        'storage': {
            'id': '286e1bd3-33c8-46d5-97ba-a309407d19ed',
            'location_uri': 'minio://http://10.3.7.220/testdataset202201101/data/Dateidaten_für_vretest3',
            'version': None,
        },
        'extended': {
            'id': 'de6200e2-a30c-4c11-a6ec-3887a545da2a',
            'extra': {'tags': [], 'system_tags': [], 'attributes': {}},
        },
    }
    new_name = 'new_name'

    with mock.patch.object(APIImportData, 'recursive_copy') as mock_recursive_copy:
        mock_recursive_copy.return_value = 1, 1, [{}]
        with mock.patch.object(APIImportData, 'recursive_delete'):
            try:
                await API.rename_file_worker(old_file, new_name, dataset, OPER, SESSION_ID, ACCESS_TOKEN, REFRESH_TOKEN)
            except Exception as e:
                pytest.fail(f'rename_file_worker raised {e} unexpectedly')

    assert mock_kafka_msg.call_count == 1
    file_folder = FileFolderActivityLogSchema.parse_obj(mock_kafka_msg.call_args[0][0])
    assert file_folder.activity_type == 'update'
