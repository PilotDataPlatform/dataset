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


async def test_import_files_from_source_list_should_return_200(client, httpx_mock):
    test_dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    test_source_project = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[{'project_geid': test_source_project}],
    )
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/b1064aa6-edbe-4eb6-b560-a8552f2f6162-1626719078',
        json=[
            {
                'labels': ['Core', 'File'],
                'global_entity_id': 'geid_2',
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[
            {
                'end_node': {
                    'labels': ['Core', 'File'],
                    'global_entity_id': 'fake_geid',
                }
            }
        ],
        match_content=json.dumps(
            {
                'label': 'own*',
                'start_label': 'Container',
                'end_label': ['Core', 'File'],
                'start_params': {'global_entity_id': test_dataset_geid},
                'end_params': {'global_entity_id': 'geid_2'},
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
                'start_label': 'Dataset',
                'start_params': {'global_entity_id': test_dataset_geid},
                'end_params': {'name': None},
            }
        ).encode('utf-8'),
    )
    # because of the background task
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
            'b1064aa6-edbe-4eb6-b560-a8552f2f6162-1626719078',
        ],
        'operator': 'admin',
        'project_geid': test_source_project,
    }
    res = await client.put(f'/v1/dataset/{test_dataset_geid}/files', json=payload)
    result = res.json()['result']
    # mock_background_taks.asert_called_with(
    #     [
    #         {
    #             'labels': ['Core', 'File'],
    #             'global_entity_id': 'geid_2',
    #             'location': 'http://anything.com/bucket/obj/path',
    #             'display_path': 'display_path',
    #             'uploader': 'test',
    #             'feedback': 'exist',
    #         }
    #     ],
    #     {'project_geid': '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'},
    #     'admin',
    #     '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058',
    #     None,
    #     None,
    #     None,
    # )
    assert res.status_code == 200
    assert result.get('ignored') == []
    assert result.get('processing') == [
        {
            'labels': ['Core', 'File'],
            'global_entity_id': 'geid_2',
            'location': 'http://anything.com/bucket/obj/path',
            'display_path': 'display_path',
            'uploader': 'test',
            'feedback': 'exist',
        }
    ]


async def test_import_files_from_different_project_return_403(client, httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[{'project_geid': 'project_1'}],
    )
    payload = {
        'source_list': [],
        'operator': 'admin',
        'project_geid': 'project_2',
    }
    res = await client.put('/v1/dataset/1234/files', json=payload)
    assert res.status_code == 403


async def test_import_files_from_non_existing_project_return_404(client, httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json={},
    )
    payload = {
        'source_list': [],
        'operator': 'admin',
        'project_geid': 'NOT_EXIST_Project',
    }
    res = await client.put('/v1/dataset/%s/files' % ('NOT_EXIST_Dataset'), json=payload)
    assert res.status_code == 404


async def test_05_test_import_duplicate(client, httpx_mock):
    test_dataset_geid = '882f4d3f-8466-4961-ba3c-38a1c272e548-1646759169'
    test_source_project = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[{'project_geid': test_source_project}],
    )
    httpx_mock.add_response(
        method='GET',
        url='http://neo4j_service/v1/neo4j/nodes/geid/b1064aa6-edbe-4eb6-b560-a8552f2f6162-1626719078',
        json=[
            {
                'labels': ['Core', 'File'],
                'global_entity_id': 'geid_2',
                'location': 'http://anything.com/bucket/obj/path',
                'display_path': 'display_path',
                'uploader': 'test',
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[
            {
                'end_node': {
                    'labels': ['Core', 'File'],
                    'global_entity_id': 'fake_geid',
                }
            }
        ],
    )

    payload = {
        'source_list': [
            'b1064aa6-edbe-4eb6-b560-a8552f2f6162-1626719078',
        ],
        'operator': 'admin',
        'project_geid': test_source_project,
    }
    res = await client.put(f'/v1/dataset/{test_dataset_geid}/files', json=payload)
    result = res.json()['result']
    assert res.status_code == 200
    assert result.get('processing') == []
    assert result.get('ignored') == [
        {
            'labels': ['Core', 'File'],
            'global_entity_id': 'geid_2',
            'location': 'http://anything.com/bucket/obj/path',
            'display_path': 'display_path',
            'uploader': 'test',
            'feedback': 'duplicate or unauthorized',
        }
    ]
