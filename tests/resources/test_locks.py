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
from app.resources.locks import recursive_lock
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


@pytest.mark.parametrize('archived', [(False), (True)])
async def test_recursive_lock_file(archived):
    nodes = [
        {'archived': archived, 'name': 'node_name_1', 'uploder': 'me', 'global_entity_id': 'any_1', 'labels': ['File']}
    ]
    code = 'any_code'
    root_path = './tests'
    new_name = None
    locked_node, err = recursive_lock(code, nodes, root_path, new_name)
    assert not err
    assert locked_node == []


async def test_recursive_lock_folder(httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[
            {
                'end_node': {
                    'archived': False,
                    'name': 'node_name_2',
                    'uploder': 'me',
                    'global_entity_id': 'any_2',
                    'labels': ['File'],
                }
            }
        ],
    )
    nodes = [
        {'archived': False, 'name': 'node_name_1', 'uploder': 'me', 'global_entity_id': 'any_1', 'labels': ['Folder']}
    ]
    code = 'any_code'
    root_path = './tests'
    new_name = None
    locked_node, err = recursive_lock(code, nodes, root_path, new_name)
    assert not err
    assert locked_node == []
