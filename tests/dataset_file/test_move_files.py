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

import pytest

pytestmark = pytest.mark.asyncio


async def test_move_file_should_call_background_task_and_add_file_to_processing(
    client, httpx_mock, mock_minio, dataset
):
    dataset_geid = str(dataset.id)
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    folder_geid = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/Folder/query',
        json=[
            {
                'labels': ['Folder'],
                'project_geid': folder_geid,
                'folder_relative_path': '',
                'name': 'test_folder',
                'dataset_code': dataset.code,
            }
        ],
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
                'start_params': {'global_entity_id': dataset_geid},
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

    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/nodes/File',
        json=[{}],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/own',
        json=[],
    )
    payload = {'source_list': [file_geid], 'operator': 'admin', 'target_geid': folder_geid}
    res = await client.post(f'/v1/dataset/{dataset_geid}/files', json=payload)
    assert res.status_code == 200
    processing_file = [x.get('global_entity_id') for x in res.json().get('result').get('processing')]
    assert processing_file == [file_geid]


async def test_move_wrong_file_ignored_when_relation_doesnt_exist(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_geid = 'random_geid'

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
        'target_geid': dataset_geid,
    }
    res = await client.post(f'/v1/dataset/{dataset_geid}/files', json=payload)

    assert res.status_code == 200
    ignored_file = [x.get('global_entity_id') for x in res.json().get('result').get('ignored')]
    assert ignored_file == [file_geid]
