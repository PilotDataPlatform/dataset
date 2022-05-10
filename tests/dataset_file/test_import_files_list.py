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
from uuid import uuid4

import pytest

pytestmark = pytest.mark.asyncio


async def test_import_files_from_source_list_should_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    source_project = str(dataset.project_id)
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
                'start_params': {'global_entity_id': source_project},
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
                'start_params': {'global_entity_id': dataset_geid},
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

    payload = {
        'source_list': [
            'b1064aa6-edbe-4eb6-b560-a8552f2f6162-1626719078',
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
        'project_geid': source_project,
    }
    res = await client.put(f'/v1/dataset/{dataset_geid}/files', json=payload)
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
