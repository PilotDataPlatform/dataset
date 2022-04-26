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


async def test_get_dataset_files_should_return_404_when_dataset_not_found(client, httpx_mock):
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[],
    )
    res = await client.get('/v1/dataset/any_geid/files')
    assert res.status_code == 404
    assert res.json() == {
        'code': 404,
        'error_msg': 'Invalid geid for dataset',
        'num_of_pages': 1,
        'page': 0,
        'result': [],
        'total': 1,
    }


async def test_get_dataset_files(client, httpx_mock):
    test_dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    test_source_project = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[{'project_geid': test_source_project}],
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
        method='GET',
        url=f'http://neo4j_service/v1/neo4j/relations/connected/{test_dataset_geid}?direction=input',
        json={
            'results': [
                {
                    'labels': 'Core',
                }
            ]
        },
    )

    res = await client.get(f'/v1/dataset/{test_dataset_geid}/files')
    assert res.status_code == 200
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': {
            'data': [
                {
                    'code': 'any_code',
                    'dataset_code': 'fake_dataset_code',
                    'global_entity_id': file_geid,
                    'labels': 'File',
                    'location': 'http://anything.com/bucket/obj/path',
                    'operator': 'me',
                    'parent_folder': '',
                    'project_code': '',
                }
            ],
            'route': [],
        },
        'total': 1,
    }
