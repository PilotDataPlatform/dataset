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


async def test_create_root_folder_should_return_200_and_folder_data(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    folder_geid = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/Folder',
        json=[
            {
                'labels': ['Folder'],
                'project_geid': folder_geid,
                'folder_relative_path': '',
                'name': 'test_folder',
                'id': 'folder_id',
                'folder_level': 0,
                'dataset_code': dataset.code,
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/own',
        json=[],
    )
    payload = {
        'folder_name': 'unitest_folder',
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_geid}/folder', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['name'] == 'test_folder'
    assert res.json()['result']['folder_level'] == 0
    assert res.json()['result']['dataset_code'] == dataset.code


async def test_create_duplicate_root_folder_should_return_409(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    folder_geid = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[
            {
                'end_node': {
                    'global_entity_id': folder_geid,
                }
            }
        ],
    )
    payload = {
        'folder_name': 'unitest_folder',
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_geid}/folder', json=payload)
    assert res.status_code == 409
    assert res.json()['error_msg'] == 'folder with that name already exists'


async def test_create_sub_folder_should_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    folder_geid = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/Folder/query',
        json=[
            {
                'labels': ['Folder'],
                'project_geid': '6fa65f15-d6ae-438b-aaaf-a918d522d335',
                'name': 'root_folder',
                'folder_relative_path': 'path/',
                'global_entity_id': 'root_folder_geid',
                'id': 'root_folder_geid',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/Folder',
        json=[
            {
                'labels': ['Folder'],
                'project_geid': folder_geid,
                'folder_relative_path': '',
                'name': 'unitest_folder2',
                'id': 'folder_id',
                'folder_level': 1,
                'dataset_code': dataset.code,
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/own',
        json=[],
    )
    payload = {
        'folder_name': 'unitest_folder2',
        'username': 'admin',
        'parent_folder_geid': folder_geid,
    }
    res = await client.post(f'/v1/dataset/{dataset_geid}/folder', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['name'] == 'unitest_folder2'
    assert res.json()['result']['folder_level'] == 1
    assert res.json()['result']['dataset_code'] == dataset.code


async def test_create_folder_with_invalid_name_should_return_400(client, dataset):
    dataset_geid = str(dataset.id)

    payload = {
        'folder_name': ' unittest_/dataset_folder',
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_geid}/folder', json=payload)
    assert res.status_code == 400
    assert res.json()['error_msg'], 'Invalid folder name'


async def test_create_folder_when_dataset_not_found_should_return_404(client, test_db):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    payload = {
        'folder_name': 'unitest_folder',
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_geid}/folder', json=payload)
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Dataset not found'


async def test_create_sub_folder_when_parent_folder_not_found_should_return_404(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    folder_geid = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/Folder/query',
        json=[],
    )
    payload = {
        'folder_name': 'unitest_folder',
        'username': 'admin',
        'parent_folder_geid': folder_geid,
    }
    res = await client.post(f'/v1/dataset/{dataset_geid}/folder', json=payload)
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Folder not found'
