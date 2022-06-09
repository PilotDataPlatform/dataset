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
    dataset_id = str(dataset.id)
    folder_id = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/'
            f'?recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={'result': []},
    )

    httpx_mock.add_response(
        method='POST',
        url='http://metadata_service/v1/item/',
        json={
            'result': {
                'id': 'bb72b4a6-d2bb-41fa-acaf-19cb7d4fce0f',
                'parent': folder_id,
                'parent_path': 'test_folder.unitest_folder2',
                'name': 'unitest_folder',
                'container_code': dataset.code,
                'container_type': 'dataset',
            }
        },
    )
    payload = {
        'folder_name': 'unitest_folder',
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/folder', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['name'] == 'unitest_folder'
    assert res.json()['result']['container_code'] == dataset.code


async def test_create_duplicate_root_folder_should_return_409(client, httpx_mock, dataset):
    dataset_id = str(dataset.id)
    new_folder_name = 'unitest_folder'
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/'
            f'?recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={
            'result': [
                {
                    'id': 'bb72b4a6-d2bb-41fa-acaf-19cb7d4fce0f',
                    'parent': None,
                    'parent_path': None,
                    'name': new_folder_name,
                    'container_code': dataset.code,
                    'container_type': 'dataset',
                }
            ]
        },
    )

    payload = {
        'folder_name': new_folder_name,
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/folder', json=payload)
    assert res.status_code == 409
    assert res.json()['error_msg'] == 'folder with that name already exists'


async def test_create_sub_folder_should_return_200(client, httpx_mock, dataset):
    dataset_id = str(dataset.id)
    folder_id = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12'

    httpx_mock.add_response(
        method='GET',
        url=f'http://metadata_service/v1/item/{folder_id}/',
        json={
            'result': {
                'id': folder_id,
                'parent': '657694cd-6a1c-4854-bdff-5e5b1d2a999b',
                'parent_path': 'any.test_folder',
                'name': 'test_folder',
                'container_code': dataset.code,
                'container_type': 'dataset',
            }
        },
    )
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/'
            f'?recursive=true&zone=1&container_code={dataset.code}&container_type=dataset&page_size=100000'
        ),
        json={
            'result': [
                {
                    'id': folder_id,
                    'parent': '657694cd-6a1c-4854-bdff-5e5b1d2a999b',
                    'parent_path': 'any.test_folder',
                    'name': 'test_folder',
                    'container_code': dataset.code,
                    'container_type': 'dataset',
                },
                {
                    'id': '657694cd-6a1c-4854-bdff-5e5b1d2a999b',
                    'parent': None,
                    'parent_path': None,
                    'name': 'any',
                    'container_code': dataset.code,
                    'container_type': 'dataset',
                },
            ]
        },
    )

    httpx_mock.add_response(
        method='POST',
        url='http://metadata_service/v1/item/',
        json={
            'result': {
                'id': 'bb72b4a6-d2bb-41fa-acaf-19cb7d4fce0f',
                'parent': folder_id,
                'parent_path': 'any.test_folder.unitest_folder2',
                'name': 'unitest_folder2',
                'container_code': dataset.code,
                'container_type': 'dataset',
            }
        },
    )
    payload = {
        'folder_name': 'unitest_folder2',
        'username': 'admin',
        'parent_folder_geid': folder_id,
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/folder', json=payload)
    assert res.status_code == 200
    assert res.json()['result'] == {
        'id': 'bb72b4a6-d2bb-41fa-acaf-19cb7d4fce0f',
        'parent': folder_id,
        'parent_path': 'any.test_folder.unitest_folder2',
        'name': 'unitest_folder2',
        'container_code': dataset.code,
        'container_type': 'dataset',
    }


async def test_create_folder_with_invalid_name_should_return_400(client, dataset):
    dataset_id = str(dataset.id)

    payload = {
        'folder_name': ' unittest_/dataset_folder',
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/folder', json=payload)
    assert res.status_code == 400
    assert res.json()['error_msg'], 'Invalid folder name'


async def test_create_folder_when_dataset_not_found_should_return_404(client, test_db):
    dataset_id = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    payload = {
        'folder_name': 'unitest_folder',
        'username': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/folder', json=payload)
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Dataset not found'


async def test_create_sub_folder_when_parent_folder_not_found_should_return_404(client, httpx_mock, dataset):
    dataset_id = str(dataset.id)
    folder_id = 'cfa31c8c-ba29-4cdf-b6f2-feef05ec9c12-1648138461'

    httpx_mock.add_response(
        method='GET',
        url=f'http://metadata_service/v1/item/{folder_id}/',
        json={'result': {}},
        status_code=404,
    )
    payload = {
        'folder_name': 'unitest_folder',
        'username': 'admin',
        'parent_folder_geid': folder_id,
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/folder', json=payload)
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Folder not found'
