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


async def test_schema_without_template_should_return_404(client, httpx_mock):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    payload = {
        'name': 'unittestdataset2',
        'dataset_geid': dataset_geid,
        'tpl_geid': 'notfound',
        'standard': 'default',
        'system_defined': True,
        'is_draft': True,
        'content': {},
        'creator': 'admin',
        'activity': [],
    }
    res = await client.post('/v1/schema', json=payload)
    assert res.status_code == 400
    assert res.json()['error_msg'] == 'Template not found'


async def test_schema_should_return_200(client, httpx_mock, schema_template):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    schema_template_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'
    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json={},
    )

    payload = {
        'name': 'unittestdataset',
        'dataset_geid': dataset_geid,
        'tpl_geid': schema_template_geid,
        'standard': 'default',
        'system_defined': True,
        'is_draft': True,
        'content': {},
        'creator': 'admin',
        'activity': [{'action': 'CREATE', 'resource': 'Schema', 'detail': {'name': 'essential.schema.json'}}],
    }
    res = await client.post('/v1/schema', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['name'] == 'unittestdataset'


async def test_create_duplicate_schema_return_409(client, httpx_mock, schema):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    schema_template_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'
    payload = {
        'name': 'unittestdataset',
        'dataset_geid': dataset_geid,
        'tpl_geid': schema_template_geid,
        'standard': 'default',
        'system_defined': True,
        'is_draft': True,
        'content': {},
        'creator': 'admin',
        'activity': [{'action': 'CREATE', 'resource': 'Schema', 'detail': {'name': 'essential.schema.json'}}],
    }
    res = await client.post('/v1/schema', json=payload)
    assert res.status_code == 409
    assert res.json()['error_msg'] == 'Schema with that name already exists'


async def test_get_schema_should_return_200(client, httpx_mock, schema):
    schema_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'
    res = await client.get(f'/v1/schema/{schema_geid}')
    assert res.status_code == 200
    assert res.json()['result'] == schema


async def test_get_schema_not_found_should_return_404(client, httpx_mock):
    schema_geid = 'notfound'
    res = await client.get(f'/v1/schema/{schema_geid}')
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Schema not found'


async def test_update_schema_should_reflect_change_and_return_200(client, httpx_mock, schema):
    schema_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'
    payload = {
        'username': 'admin',
        'content': {'test': 'testing'},
        'activity': [],
    }
    res = await client.put(f'/v1/schema/{schema_geid}', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['content'] == {'test': 'testing'}


async def test_delete_schema_should_return_200(client, httpx_mock, schema):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    schema_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'
    payload = {
        'username': 'admin',
        'dataset_geid': dataset_geid,
        'activity': [],
    }
    res = await client.delete(f'/v1/schema/{schema_geid}', json=payload)
    assert res.status_code == 200
    assert res.json()['result'] == 'success'


async def test_list_schema_should_bring_essential_schema_first(client, httpx_mock, essential_schema, schema):
    from app.config import ConfigClass

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'

    # Get created essential schema
    payload = {
        'dataset_geid': dataset_geid,
        'name': ConfigClass.ESSENTIALS_NAME,
    }
    res = await client.post('/v1/schema/list', json=payload)
    assert res.status_code == 200
    assert res.json()['result'][0]['name'] == 'essential.schema.json'
