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
import json
pytestmark = pytest.mark.asyncio


async def test_activity_logs_should_get_data_from_elasticsearch(client, httpx_mock):
    query = json.dumps({
        "dataset_geid": {"value": "679e4b11-61fd-4df7-9084-5e0ff0c99b9e-1647523203", "condition": "equal"}
    })
    query_string = f'page_size=10&page=0&order_by=create_timestamp&order_type=desc&query={query}'

    httpx_mock.add_response(
        method='GET',
        url='http://elastic_search_service/activity-logs/_search',
        json={
            'hits': {
                'hits': {'any': 'any'},
                'total': {'value': 1}
            }
        }
    )
    res = await client.get(f'/v1/activity-logs?{query_string}')
    assert res.status_code == 200
    assert res.json()['result'] == {'any': 'any'}
    assert res.json()['total'] == 1


async def test_activity_logs_get_by_version_when_version_doesnt_exist_should_return_200(client, httpx_mock, test_db):
    dataset_geid = '679e4b11-61fd-4df7-9084-5e0ff0c99b9e-1647523203'
    query_string = 'page_size=10&page=0&version=1'

    res = await client.get(f'/v1/activity-logs/{dataset_geid}?{query_string}')
    assert res.status_code == 200
    assert res.json()['result'] == f'there is no version information for dataset {dataset_geid}'


async def test_activity_logs_get_by_version_should_return_200(client, httpx_mock, test_db, version):
    dataset_geid = version['dataset_geid']
    query_string = f'page_size=10&page=0&version={version["version"]}'
    httpx_mock.add_response(
        method='GET',
        url='http://elastic_search_service/activity-logs/_search',
        json={
            'hits': {
                'hits': {'any': 'any'},
                'total': {'value': 1}
            }
        }
    )

    res = await client.get(f'/v1/activity-logs/{dataset_geid}?{query_string}')
    assert res.status_code == 200
    assert res.json()['result'] == {'any': 'any'}
    assert res.json()['total'] == 1
