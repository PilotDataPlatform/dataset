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

from app.config import ConfigClass

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def test_db(db_session):
    yield db_session


async def test_schema_without_template_should_return_404(client):
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'
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
    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    schema_template_geid = schema_template.geid
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
    dataset_geid = schema.dataset_geid
    schema_template_geid = schema.tpl_geid
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
    schema_geid = schema.geid
    res = await client.get(f'/v1/schema/{schema_geid}')
    assert res.status_code == 200
    assert res.json()['result'] == schema.to_dict()


async def test_get_schema_not_found_should_return_404(client, httpx_mock):
    schema_geid = 'notfound'
    res = await client.get(f'/v1/schema/{schema_geid}')
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Schema not found'


async def test_update_schema_should_reflect_change_and_return_200(client, httpx_mock, schema):
    schema_geid = schema.geid
    payload = {
        'username': 'admin',
        'content': {'test': 'testing'},
        'activity': [],
    }
    res = await client.put(f'/v1/schema/{schema_geid}', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['content'] == {'test': 'testing'}


async def test_update_essential_schema_should_reflect_change_and_return_200(client, httpx_mock, essential_schema):
    schema_geid = essential_schema.geid
    payload = {
        'username': 'admin',
        'content': {
            'dataset_title': 'title',
            'dataset_authors': 'author',
            'dataset_description': 'any',
            'dataset_type': 'bids',
            'dataset_modality': 'any',
        },
        'activity': [],
    }
    res = await client.put(f'/v1/schema/{schema_geid}', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['content'] == {
        'dataset_authors': 'author',
        'dataset_description': 'any',
        'dataset_modality': 'any',
        'dataset_title': 'title',
        'dataset_type': 'bids',
    }


async def test_update_essential_schema_should_have_required_all_fields_return_400(client, httpx_mock, essential_schema):
    schema_geid = essential_schema.geid
    payload = {
        'username': 'admin',
        'content': {'dataset_title': 'title', 'dataset_authors': 'author', 'dataset_description': 'any'},
        'activity': [],
    }
    res = await client.put(f'/v1/schema/{schema_geid}', json=payload)
    assert res.status_code == 400
    assert res.json() == {
        'code': 400,
        'error_msg': 'Missing content field for essential schema: dataset_type',
        'result': '',
    }


async def test_delete_schema_should_return_200(client, schema):
    dataset_geid = schema.dataset_geid
    schema_geid = schema.geid
    payload = {
        'username': 'admin',
        'dataset_geid': dataset_geid,
        'activity': [],
    }
    res = await client.delete(f'/v1/schema/{schema_geid}', json=payload)
    assert res.status_code == 200
    assert res.json()['result'] == 'success'


async def test_list_schema_should_bring_essential_schema_first(client, essential_schema, schema):

    # Get created essential schema
    payload = {
        'dataset_geid': schema.dataset_geid,
        'name': ConfigClass.ESSENTIALS_NAME,
    }
    res = await client.post('/v1/schema/list', json=payload)
    assert res.status_code == 200
    assert res.json()['result'][0]['name'] == 'essential.schema.json'
