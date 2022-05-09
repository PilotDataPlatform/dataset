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

import pytest

pytestmark = pytest.mark.asyncio

OPER = 'admin'
SESSION_ID = '12345'
ACCESS_TOKEN = 'token'
REFRESH_TOKEN = 'refresh'


@pytest.fixture
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


@mock.patch('app.routers.v1.dataset_file.recursive_lock_import')
async def test_copy_file_worker_should_import_file_succeed(
    mock_recursive_lock_import,
    external_requests,
    httpx_mock,
):
    from app.routers.v1.dataset_file import APIImportData

    api = APIImportData()

    httpx_mock.add_response(
        method='PUT',
        url='http://neo4j_service/v1/neo4j/nodes/Dataset/node/any_id',
        json=[{}],
    )
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/source_geid',
        json=[{'code': 'source_project_code'}],
    )
    mock_recursive_lock_import.return_value = [], False
    import_list = [
        {
            'global_entity_id': 'source_file_2_global_entity_id',
            'display_path': 'any://any/any.any',
            'name': 'test_project',
        }
    ]
    dataset_obj = {'global_entity_id': 'dataset_global_entity_id', 'code': 'datasetcode', 'id': 'any_id'}
    source_project_geid = 'source_geid'

    with mock.patch.object(APIImportData, 'recursive_copy') as mock_recursive_copy:
        mock_recursive_copy.return_value = 1, 1, None
        try:
            api.copy_files_worker(
                import_list, dataset_obj, OPER, source_project_geid, SESSION_ID, ACCESS_TOKEN, REFRESH_TOKEN
            )
        except Exception as e:
            pytest.fail(f'copy_files_worker raised {e} unexpectedly')
    event_status_request = httpx_mock.get_requests()[-1]
    req_res = json.loads(event_status_request.content)
    assert req_res['event_type'] == 'DATASET_FILE_IMPORT_SUCCEED'


@mock.patch('app.routers.v1.dataset_file.recursive_lock_move_rename')
async def test_move_file_worker_should_move_file_succeed(
    mock_recursive_lock_move_rename,
    external_requests,
    httpx_mock,
):
    from app.routers.v1.dataset_file import APIImportData

    api = APIImportData()

    mock_recursive_lock_move_rename.return_value = [], False

    move_list = [
        {
            'global_entity_id': 'source_file_2_global_entity_id',
            'display_path': 'any://any/any/file.any',
            'location': 'any://any/any/file.any',
            'name': 'test_project',
            'labels': ['File'],
        }
    ]
    dataset_obj = {'global_entity_id': 'dataset_global_entity_id', 'code': 'datasetcode', 'id': 'any_id'}
    target_folder = {'folder_relative_path': None, 'name': 'any_folder'}
    target_minio_path = 'mini://anypath/any'

    with mock.patch.object(APIImportData, 'recursive_copy') as mock_recursive_copy:
        mock_recursive_copy.return_value = 1, 1, None
        with mock.patch.object(APIImportData, 'recursive_delete'):
            try:
                api.move_file_worker(
                    move_list,
                    dataset_obj,
                    OPER,
                    target_folder,
                    target_minio_path,
                    SESSION_ID,
                    ACCESS_TOKEN,
                    REFRESH_TOKEN,
                )
            except Exception as e:
                pytest.fail(f'copy_files_worker raised {e} unexpectedly')
    event_status_request = httpx_mock.get_requests()[-1]
    req_res = json.loads(event_status_request.content)
    assert req_res['event_type'] == 'DATASET_FILE_MOVE_SUCCEED'


@mock.patch('app.routers.v1.dataset_file.recursive_lock_delete')
async def test_delete_files_work_should_delete_file_succeed(
    mock_recursive_lock_delete,
    external_requests,
    httpx_mock,
):
    from app.routers.v1.dataset_file import APIImportData

    api = APIImportData()

    mock_recursive_lock_delete.return_value = [], False
    httpx_mock.add_response(
        method='PUT',
        url='http://neo4j_service/v1/neo4j/nodes/Dataset/node/any_id',
        json=[{}],
    )

    delete_list = [
        {
            'global_entity_id': 'source_file_2_global_entity_id',
            'display_path': 'any://any/any/file.any',
            'location': 'any://any/any/file.any',
            'name': 'test_project',
            'labels': ['File'],
        }
    ]
    dataset_obj = {'global_entity_id': 'dataset_global_entity_id', 'code': 'datasetcode', 'id': 'any_id'}

    with mock.patch.object(APIImportData, 'recursive_delete') as mock_recursive_delete:
        mock_recursive_delete.return_value = 1, 1
        try:
            api.delete_files_work(delete_list, dataset_obj, OPER, SESSION_ID, ACCESS_TOKEN, REFRESH_TOKEN)
        except Exception as e:
            pytest.fail(f'copy_delete_work raised {e} unexpectedly')
    event_status_request = httpx_mock.get_requests()[-1]
    req_res = json.loads(event_status_request.content)
    assert req_res['event_type'] == 'DATASET_FILE_DELETE_SUCCEED'


@mock.patch('app.routers.v1.dataset_file.recursive_lock_move_rename')
async def test_rename_file_worker_should_move_file_succeed(
    mock_recursive_lock_move_rename,
    external_requests,
    httpx_mock,
):
    from app.routers.v1.dataset_file import APIImportData

    api = APIImportData()

    mock_recursive_lock_move_rename.return_value = [], False
    httpx_mock.add_response(
        method='PUT',
        url='http://data_ops_util/v1/tasks/',
        json=[],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[
            {
                'start_node': {
                    'global_entity_id': 'source_file_2_global_entity_id',
                    'name': 'node_name',
                }
            }
        ],
    )

    old_file = [
        {
            'global_entity_id': 'source_file_2_global_entity_id',
            'display_path': 'any://any/any/file.any',
            'location': 'any://any/any/file.any',
            'name': 'test_project',
            'labels': ['File'],
        }
    ]
    dataset_obj = {'global_entity_id': 'dataset_global_entity_id', 'code': 'datasetcode', 'id': 'any_id'}
    new_name = 'new_name'

    with mock.patch.object(APIImportData, 'recursive_copy') as mock_recursive_copy:
        mock_recursive_copy.return_value = 1, 1, [{}]
        with mock.patch.object(APIImportData, 'recursive_delete'):
            try:
                api.rename_file_worker(old_file, new_name, dataset_obj, OPER, SESSION_ID, ACCESS_TOKEN, REFRESH_TOKEN)
            except Exception as e:
                pytest.fail(f'rename_file_worker raised {e} unexpectedly')
    event_status_request = httpx_mock.get_requests()[-1]
    req_res = json.loads(event_status_request.content)
    assert req_res['event_type'] == 'DATASET_FILE_RENAME_SUCCEED'
