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


async def test_file_delete_from_dataset_should_start_background_task_and_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'

    httpx_mock.add_response(
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/nodes/geid/{file_geid}',
        json=[
            {
                'labels': ['File'],
                'global_entity_id': file_geid,
                'id': 'file_id',
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
    payload = {'source_list': [file_geid], 'operator': 'admin'}
    res = await client.delete(f'/v1/dataset/{dataset_geid}/files', json=payload)

    assert res.status_code == 200
    processing_file = [x.get('global_entity_id') for x in res.json().get('result').get('processing')]
    assert processing_file == [file_geid]


async def test_delete_from_not_in_dataset_should_not_reaise_error(client, httpx_mock, dataset):
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

    payload = {'source_list': [file_geid], 'operator': 'admin'}
    res = await client.delete(f'/v1/dataset/{dataset_geid}/files', json=payload)
    assert res.status_code == 200
    ignored_file = [x.get('global_entity_id') for x in res.json().get('result').get('ignored')]
    assert ignored_file == [file_geid]
