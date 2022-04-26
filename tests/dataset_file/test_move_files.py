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


async def test_move_file_should_call_background_task_and_add_file_to_processing(client, httpx_mock):
    from app.routers.v1.dataset_file import APIImportData

    test_dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    test_source_project = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    folder_geid = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[{'project_geid': test_source_project}],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/Folder/query',
        json=[{'labels': ['Folder'], 'project_geid': folder_geid, 'folder_relative_path': '', 'name': 'test_folder'}],
    )
    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{file_geid}',
        json=[
            {
                'labels': ['File'],
                'global_entity_id': file_geid,
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
                'name': 'sample.log',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[
            {
                'end_node': {
                    'labels': ['File'],
                    'global_entity_id': 'fake_geid',
                }
            }
        ],
        match_content=json.dumps(
            {
                'label': 'own*',
                'start_label': 'Dataset',
                'end_label': ['File'],
                'start_params': {'global_entity_id': test_dataset_geid},
                'end_params': {'global_entity_id': file_geid},
            }
        ).encode('utf-8'),
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[],
        match_content=json.dumps(
            {
                'label': 'own',
                'start_label': 'Folder',
                'start_params': {'global_entity_id': folder_geid},
                'end_params': {'name': 'sample.log'},
            }
        ).encode('utf-8'),
    )
    payload = {'source_list': [file_geid], 'operator': 'admin', 'target_geid': folder_geid}
    with mock.patch.object(APIImportData, 'move_file_worker') as mock_background_taks:
        res = await client.post(f'/v1/dataset/{test_dataset_geid}/files', json=payload)
    assert res.status_code == 200
    processing_file = [x.get('global_entity_id') for x in res.json().get('result').get('processing')]
    assert processing_file == [file_geid]
    mock_background_taks.asert_called_with()


async def test_move_wrong_file_ignored_when_relation_doesnt_exist(client, httpx_mock):
    from app.routers.v1.dataset_file import APIImportData

    test_source_project = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    test_dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    file_geid = 'random_geid'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[{'project_geid': test_source_project}],
    )
    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{file_geid}',
        json=[
            {
                'labels': ['File'],
                'global_entity_id': file_geid,
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
                'name': 'sample.log',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[],
    )
    payload = {
        'source_list': [file_geid],
        'operator': 'admin',
        'target_geid': test_dataset_geid,
    }
    with mock.patch.object(APIImportData, 'move_file_worker') as mock_background_taks:
        res = await client.post(f'/v1/dataset/{test_dataset_geid}/files', json=payload)

    assert res.status_code == 200
    ignored_file = [x.get('global_entity_id') for x in res.json().get('result').get('ignored')]
    assert ignored_file == [file_geid]
    mock_background_taks.assert_not_called()
