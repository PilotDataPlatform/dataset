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


@pytest.fixture(autouse=True)
def test_db(db_session):
    yield


async def test_publish_version_should_start_background_task_and_return_200(client, httpx_mock, mock_minio):
    # mock_upload.side_effect = 'http://minio://fake_version.zip'
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'

    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[
            {
                'global_entity_id': dataset_geid,
                'code': 'dataset_code',
                'id': 'dataset_id',
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
                    'global_entity_id': file_geid,
                }
            }
        ],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://neo4j_service/v2/neo4j/relations/query',
        json={
            'results': [
                {
                    'code': 'any_code',
                    'labels': 'File',
                    'location': 'http://anything.com/bucket/obj/path',
                    'global_entity_id': file_geid,
                    'project_code': '',
                    'operator': 'me',
                    'parent_folder': '',
                    'dataset_code': 'fake_dataset_code',
                }
            ]
        },
    )
    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )

    payload = {'operator': 'admin', 'notes': 'testing', 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_geid}/publish', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['status_id'] == dataset_geid

    # Test status
    res = await client.get(f'/v1/dataset/{dataset_geid}/publish/status?status_id={dataset_geid}')
    assert res.json()['result']['status'] == 'success'


async def test_publish_version_with_large_notes_should_return_400(client, httpx_mock, mock_minio):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'

    payload = {'operator': 'admin', 'notes': ''.join(['12345' for i in range(60)]), 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_geid}/publish', json=payload)
    assert res.status_code == 400
    assert res.json()['result'] == 'Notes is to large, limit 250 bytes'


async def test_publish_version_with_incorrect_notes_should_return_400(client, httpx_mock, mock_minio):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    payload = {'operator': 'admin', 'notes': 'test', 'version': 'incorrect'}
    res = await client.post(f'/v1/dataset/{dataset_geid}/publish', json=payload)
    assert res.status_code == 400
    assert res.json()['result'] == 'Incorrect version format'


async def test_publish_version_duplicate_should_return_409(client, httpx_mock, mock_minio, version):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    payload = {'operator': 'admin', 'notes': 'test', 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_geid}/publish', json=payload)
    assert res.status_code == 409
    assert res.json()['result'] == 'Duplicate version found for dataset'


async def test_version_list_should_return_200_and_version_in_result(client, httpx_mock, version):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    payload = {}
    res = await client.get(f'/v1/dataset/{dataset_geid}/versions', json=payload)
    assert res.status_code == 200
    assert res.json()['result'][0] == version


async def test_version_list_should_return_200_and_download_hash_as_str(client, httpx_mock, version):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[
            {
                'global_entity_id': dataset_geid,
                'code': 'dataset_code',
                'id': 'dataset_id',
            }
        ],
    )
    payload = {'version': '2.0'}
    res = await client.get(f'/v1/dataset/{dataset_geid}/download/pre', query_string=payload)
    assert res.status_code, 200
    assert isinstance(res.json()['result']['download_hash'], str)


async def test_publish_version_when_dataset_not_found_should_return_404(client, httpx_mock, mock_minio):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[],
    )
    payload = {'operator': 'admin', 'notes': 'test', 'version': '2.0'}
    res = await client.post(f'/v1/dataset/{dataset_geid}/publish', json=payload)
    assert res.status_code, 404
    assert res.json()['error_msg'], 'Dataset not found'


async def test_version_publish_status_not_found_should_return_404(client, httpx_mock):
    dataset_geid = 'any'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[
            {
                'global_entity_id': dataset_geid,
                'code': 'dataset_code',
                'id': 'dataset_id',
            }
        ],
    )
    res = await client.get(f'/v1/dataset/{dataset_geid}/publish/status?status_id={dataset_geid}')
    assert res.status_code, 404
    assert res.json()['error_msg'], 'Status not found'
