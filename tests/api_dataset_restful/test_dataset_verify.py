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


async def test_dataset_verify_when_bids_verification_fails_should_return_500(client, httpx_mock, dataset):
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    dataset_geid = str(dataset.id)
    dataset_code = dataset.code
    payload = {'dataset_geid': dataset_geid, 'type': 'any'}
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/'
            f'?recursive=true&zone=1&container_code={dataset_code}&container_type=dataset&page_size=100000'
        ),
        json={
            'result': [
                {
                    'type': 'file',
                    'storage': {'location_uri': f'http://anything.com/bucket/{dataset_code}/path.nii.gz'},
                    'id': file_geid,
                    'operator': 'me',
                    'parent': '',
                    'code': dataset_geid,
                    'size': 1,
                }
            ]
        },
    )
    res = await client.post('/v1/dataset/verify', json=payload)
    assert res.status_code == 500
    assert res.json() == {
        'code': 500,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': 'failed to validate bids folder',
        'total': 1,
    }


@mock.patch('app.routers.v1.api_dataset_restful.subprocess.run')
async def test_dataset_verify_when_bids_valid_should_return_200(mock_subproc_run, client, httpx_mock, dataset):
    file_geid = '6c99e8bb-ecff-44c8-8fdc-a3d0ed7ac067-1648138467'
    dataset_geid = str(dataset.id)
    dataset_code = dataset.code

    # this part has to be better tested
    obj = mock.MagicMock()
    obj.stdout = '{"bids": "verified"}'
    mock_subproc_run.return_value = obj
    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/'
            f'?recursive=true&zone=1&container_code={dataset_code}&container_type=dataset&page_size=100000'
        ),
        json={
            'result': [
                {
                    'code': 'dataset_geid',
                    'type': 'file',
                    'storage': {'location_uri': f'http://anything.com/bucket/{dataset_code}/path.nii.gz'},
                    'id': file_geid,
                    'operator': 'me',
                    'parent': '',
                    'dataset_code': dataset_geid,
                    'size': 1,
                }
            ]
        },
    )
    payload = {'dataset_geid': dataset_geid, 'type': 'any'}
    res = await client.post('/v1/dataset/verify', json=payload)
    assert res.status_code == 200
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': {'bids': 'verified'},
        'total': 1,
    }


async def test_dataset_verify_pre_should_return_200(client, httpx_mock, dataset):
    dataset_geid = str(dataset.id)
    httpx_mock.add_response(method='POST', url='http://send_message_url/v1/send_message', json={})

    payload = {'dataset_geid': dataset_geid, 'type': 'any'}
    res = await client.post(
        '/v1/dataset/verify/pre',
        headers={'Authorization': 'Barear token', 'Refresh-Token': 'refresh_token'},
        json=payload,
    )
    assert res.status_code == 200
    assert res.json() == {'code': 200, 'error_msg': '', 'num_of_pages': 1, 'page': 0, 'result': {}, 'total': 1}


async def test_get_bids_msg_should_return_error_when_exception_happens(client, httpx_mock):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    res = await client.get(f'/v1/dataset/bids-msg/{dataset_geid}')
    assert res.status_code == 500
    assert 'Psql Error' in res.json()['result']


async def test_get_bids_msg_should_return_200(client, httpx_mock, test_db, bids_results):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    res = await client.get(f'/v1/dataset/bids-msg/{dataset_geid}')
    assert res.status_code == 200
    assert res.json()['result'] == bids_results
