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


async def test_file_delete_from_dataset_should_start_background_task_and_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_id = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067'
    file_dict = {
        'id': file_id,
        'parent': '81b70730-2bc3-4ffc-9e98-3d0cdeec867b',
        'parent_path': 'admin.test_sub_6 - Copy.test_sub_delete_6',
        'name': '.hidden_file.txt',
        'container_code': 'test202203241',
        'container_type': 'project',
        'type': 'file',
        'storage': {
            'id': 'f2397e68-4e94-4419-bb72-3be532a789b2',
            'location_uri': (
                'minio://http://minio.minio:9000/core-test202203241/admin/test_sub_6'
                ' - Copy/test_sub_delete_6/.hidden_file.txt'
            ),
            'version': None,
        },
    }
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            f'recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': [file_dict]},
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
    payload = {'source_list': [file_id], 'operator': 'admin'}
    res = await client.delete(f'/v1/dataset/{dataset_geid}/files', json=payload)

    assert res.status_code == 200
    processing_file = [x.get('id') for x in res.json().get('result').get('processing')]
    assert processing_file == [file_id]


async def test_delete_from_not_in_dataset_should_not_reaise_error(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    file_geid = 'random_geid'

    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?'
            f'recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': [{'id': '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067'}]},
    )

    payload = {'source_list': [file_geid], 'operator': 'admin'}
    res = await client.delete(f'/v1/dataset/{dataset_geid}/files', json=payload)
    assert res.status_code == 200
    ignored_file = [x.get('id') for x in res.json().get('result').get('ignored')]
    assert ignored_file == [file_geid]
