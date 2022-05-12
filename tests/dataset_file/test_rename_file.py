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


async def test_rename_file_should_add_file_to_processing_and_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'

    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{file_geid}',
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
                'start_node': {
                    'labels': ['Core', 'File'],
                    'global_entity_id': file_geid,
                }
            }
        ],
        match_content=json.dumps({'label': 'own', 'end_label': 'Core', 'end_params': {'id': None}}).encode('utf-8'),
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[{}],
        match_content=json.dumps(
            {
                'label': 'own*',
                'start_label': 'Dataset',
                'end_label': ['Core', 'File'],
                'start_params': {'global_entity_id': dataset_geid},
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
                'start_label': 'Core',
                'start_params': {'global_entity_id': file_geid},
                'end_params': {'name': 'new_name'},
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
    payload = {'new_name': 'new_name', 'operator': 'admin'}
    res = await client.post(
        f'/v1/dataset/{dataset_geid}/files/{file_geid}',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    result = res.json()['result']
    assert result['ignored'] == []
    assert result['processing'] == [
        {
            'display_path': 'display_path',
            'feedback': 'exist',
            'global_entity_id': 'geid_2',
            'labels': ['Core', 'File'],
            'location': 'http://anything.com/bucket/obj/path',
            'uploader': 'test',
        }
    ]
    assert res.status_code == 200


async def test_rename_file_should_add_file_to_ignoring_when_file_wrong_and_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'

    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{file_geid}',
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
                'start_node': {
                    'labels': ['Core', 'File'],
                    'global_entity_id': file_geid,
                }
            }
        ],
        match_content=json.dumps({'label': 'own', 'end_label': 'Core', 'end_params': {'id': None}}).encode('utf-8'),
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[],
        match_content=json.dumps(
            {
                'label': 'own*',
                'start_label': 'Dataset',
                'end_label': ['Core', 'File'],
                'start_params': {'global_entity_id': dataset_geid},
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
                'start_label': 'Core',
                'start_params': {'global_entity_id': file_geid},
                'end_params': {'name': 'new_name'},
            }
        ).encode('utf-8'),
    )

    payload = {'new_name': 'new_name', 'operator': 'admin'}
    res = await client.post(
        f'/v1/dataset/{dataset_geid}/files/{file_geid}',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    result = res.json()['result']
    assert result['ignored'] == [{'feedback': 'unauthorized', 'global_entity_id': file_geid}]
    assert result['processing'] == []
    assert res.status_code == 200


async def test_rename_file_should_add_file_to_ignoring_when_file_duplicated_and_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'

    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{file_geid}',
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
                'start_node': {
                    'labels': ['Core', 'File'],
                    'global_entity_id': file_geid,
                }
            }
        ],
        match_content=json.dumps({'label': 'own', 'end_label': 'Core', 'end_params': {'id': None}}).encode('utf-8'),
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[{}],
        match_content=json.dumps(
            {
                'label': 'own*',
                'start_label': 'Dataset',
                'end_label': ['Core', 'File'],
                'start_params': {'global_entity_id': dataset_geid},
                'end_params': {'global_entity_id': 'geid_2'},
            }
        ).encode('utf-8'),
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v1/neo4j/relations/query',
        json=[{}],
        match_content=json.dumps(
            {
                'label': 'own',
                'start_label': 'Core',
                'start_params': {'global_entity_id': file_geid},
                'end_params': {'name': 'new_name'},
            }
        ).encode('utf-8'),
    )

    payload = {'new_name': 'new_name', 'operator': 'admin'}
    res = await client.post(
        f'/v1/dataset/{dataset_geid}/files/{file_geid}',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    result = res.json()['result']
    assert result['ignored'] == [
        {
            'display_path': 'display_path',
            'feedback': 'exist',
            'global_entity_id': 'geid_2',
            'labels': ['Core', 'File'],
            'location': 'http://anything.com/bucket/obj/path',
            'uploader': 'test',
        }
    ]
    assert result['processing'] == []
    assert res.status_code == 200
