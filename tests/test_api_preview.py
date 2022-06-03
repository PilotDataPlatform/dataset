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

from unittest import mock

import pytest

pytestmark = pytest.mark.asyncio


def get_long_csv():
    data = ''
    for i in range(int(500000 / 4)):
        data += f'\n{i}, {i+1}, {i+2}, {i+3}, {i+4}, {i+5}'
    return bytes(data, 'utf-8')


class CSVMockClient:
    def get_object(self):
        return MockResponse(dataset_type='csv')


class CSV2MockClient:
    def get_object(self):
        return MockResponse(dataset_type='csv2')


class CSVLongMockClient:
    def get_object(self):
        return MockResponse(dataset_type='csvlong')


class TSVMockClient:
    def get_object(self):
        return MockResponse(dataset_type='tsv')


class JSONMockClient:
    def get_object(self):
        return MockResponse(dataset_type='json')


class MockResponse:
    DATASET_TYPE = {
        'csv': b'a,b,c\n1,2,3',
        'csv2': b'a|b|c\n1|2|3',
        'csvlong': get_long_csv(),
        'json': b"{'test': 'test1'}",
        'tsv': b'a\tb\tc\n1\t2\t3',
    }

    def __init__(self, dataset_type=None):
        self.data = self.DATASET_TYPE[dataset_type]


@mock.patch('minio.Minio.get_object')
@pytest.mark.parametrize(
    'file_name,file_obj,expected_content',
    [
        ('test_folder.csv', CSVMockClient().get_object(), 'a,b,c1,2,3'),
        ('test_folder.json', JSONMockClient().get_object(), {'test': 'test1'}),
        ('test_folder.csv2', CSV2MockClient().get_object(), 'a|b|c1|2|3'),
        ('test_folder.tsv', TSVMockClient().get_object(), 'a,b,c1,2,3'),
    ],
)
async def test_preview_should_respect_file_type(mock_minio, client, httpx_mock, file_name, file_obj, expected_content):
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-164.8138467'
    mock_minio.return_value = file_obj
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-164.8138467',
        json={
            'result': {'id': file_geid, 'storage': {'location_uri': 'minio://any/any'}, 'name': file_name, 'size': 1}
        },
    )
    res = await client.get(f'/v1/{file_geid}/preview')
    assert res.status_code == 200
    response_content = res.json()['result']['content'].replace('\n', '').replace('\r', '')
    assert response_content == str(expected_content)


async def test_preview_should_return_404_when_not_found(client, httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/any',
        json={'result': {}},
    )
    res = await client.get('/v1/any/preview')
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'File not found'


@mock.patch('minio.Minio.get_object')
async def test_preview_should_concatenate_true_when_file_size_bigger_than_conf(mock_minio, client, httpx_mock):
    from app.config import ConfigClass

    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-164.8138467'
    mock_minio.return_value = CSVLongMockClient().get_object()
    httpx_mock.add_response(
        method='GET',
        url='http://metadata_service/v1/item/6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-164.8138467',
        json={
            'result': {
                'id': file_geid,
                'storage': {'location_uri': 'minio://any/any'},
                'name': 'test_folder.csv',
                'size': ConfigClass.MAX_PREVIEW_SIZE + 1,
            }
        },
    )
    res = await client.get(f'/v1/{file_geid}/preview')
    assert res.status_code == 200
    assert res.json()['result']['is_concatinated']
    assert res.json()['result']['content'] != get_long_csv()
