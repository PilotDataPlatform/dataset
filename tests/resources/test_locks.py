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

from app.resources.locks import lock_resource
from app.resources.locks import unlock_resource

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize('lock_function,request_method', [(lock_resource, 'POST'), (unlock_resource, 'DELETE')])
async def test_lock_resource_should_call_resource_lock_correctly(httpx_mock, lock_function, request_method):
    httpx_mock.add_response(
        method=request_method, url='http://data_ops_util/v2/resource/lock/', status_code=200, json={}
    )
    resp = lock_function('fake_key', 'me')
    assert resp == {}


@pytest.mark.parametrize('lock_function,request_method', [(lock_resource, 'POST'), (unlock_resource, 'DELETE')])
async def test_lock_resource_should_raise_exception_when_lock_request_not_200(
    httpx_mock, lock_function, request_method
):
    httpx_mock.add_response(
        method=request_method, url='http://data_ops_util/v2/resource/lock/', status_code=404, json={}
    )
    with pytest.raises(Exception):
        lock_function('fake_key', 'me')


# @pytest.fixture
# def external_requests(httpx_mock):
#     httpx_mock.add_response(
#         method='POST',
#         url='http://queue_service/v1/broker/pub',
#         json=[],
#     )
#     httpx_mock.add_response(
#         method='POST',
#         url='http://data_ops_util/v1/tasks/',
#         json=[],
#     )
#     httpx_mock.add_response(
#         method='PUT',
#         url='http://data_ops_util/v1/tasks/',
#         json=[],
#     )


# async def test_copy_file(external_requests):
#     from app.routers.v1.dataset_file import APIImportData

#     api = APIImportData()
#     import_list = [
#         {'global_entity_id': 'source_file_1_global_entity_id', 'archived': False},
#         {
#             'global_entity_id': 'source_file_2_global_entity_id',
#             'display_path': '',
#             'uploader': '',
#             'labels': ['File'],   # if not file has to have core- in the project_code
#             'location': '',
#             'name': '',
#             'folder_relative_path': '',
#             'uploader': '',
#         }
#     ]
#     dataset_obj = {
#         'global_entity_id': 'dataset_global_entity_id',
#         'code': 'datasetcode'
#     }
#     oper = 'admin'
#     source_project_geid = 'source_geid'
#     session_id = '12345'
#     access_token = 'token'
#     refresh_token = 'refresh'

#     try:
#         api.copy_files_worker(
#             import_list, dataset_obj, oper, source_project_geid,
#             session_id, access_token, refresh_token
#         )
#     except Exception as e:
#         pytest.fail(f"copy_files_worker raised {e} unexpectedly")
