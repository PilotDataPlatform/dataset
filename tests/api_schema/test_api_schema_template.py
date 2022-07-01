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
    yield db_session


async def test_schema_template_should_return_200(client, httpx_mock, dataset):
    dataset_id = dataset.id

    payload = {
        'name': 'unittestdataset',
        'standard': 'default',
        'system_defined': True,
        'is_draft': True,
        'content': {},
        'creator': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/schemaTPL', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['name'] == 'unittestdataset'


async def test_schema_template_duplicate_should_return_code_403(client, schema_template):
    dataset_id = schema_template.dataset_geid
    payload = {
        'name': schema_template.name,
        'standard': 'default',
        'system_defined': True,
        'is_draft': True,
        'content': {},
        'creator': 'admin',
    }
    res = await client.post(f'/v1/dataset/{dataset_id}/schemaTPL', json=payload)
    assert res.status_code == 200
    assert res.json()['error_msg'] == 'The template name already exists.'
    assert res.json()['code'] == 403


@pytest.mark.parametrize('dataset_id', [('mock'), ('default')])
async def test_list_schema_template_by_dataset_id_should_return_200(dataset_id, client, schema_template):
    if dataset_id == 'mock':
        dataset_id = schema_template.dataset_geid
    payload = {}
    res = await client.post(f'/v1/dataset/{dataset_id}/schemaTPL/list', json=payload)
    assert res.status_code == 200
    assert res.json()['result'][0] == {
        'geid': schema_template.geid,
        'name': schema_template.name,
        'system_defined': schema_template.system_defined,
        'standard': schema_template.standard,
    }


@pytest.mark.parametrize('dataset_id', [('mock'), ('default')])
async def test_get_schema_template_by_geid_should_return_200(dataset_id, client, schema_template):
    if dataset_id == 'mock':
        dataset_id = schema_template.dataset_geid
    geid = schema_template.geid
    res = await client.get(f'/v1/dataset/{dataset_id}/schemaTPL/{geid}')
    assert res.status_code == 200
    assert res.json()['result'] == schema_template.to_dict()


async def test_update_schema_template_should_return_200(client, httpx_mock, schema_template):
    dataset_id = schema_template.dataset_geid
    geid = schema_template.geid

    payload = {
        'name': 'newname',
        'is_draft': False,
        'activity': [{'action': 'UPDATE', 'resource': 'Schema Template', 'detail': {'name': 'essential.schema.json'}}],
        'content': {'any': 'any'},
    }
    res = await client.put(f'/v1/dataset/{dataset_id}/schemaTPL/{geid}', json=payload)
    assert res.status_code == 200
    assert res.json()['result']['name'] == 'newname'
    assert not res.json()['result']['is_draft']
    assert res.json()['result']['content'] == {'any': 'any'}


async def test_update_schema_template_with_name_that_already_exist_should_return_code_403(client, schema_template):
    dataset_id = schema_template.dataset_geid
    geid = schema_template.geid
    payload = {
        'name': schema_template.name,
        'is_draft': False,
        'activity': [],
        'content': {'any': 'any'},
    }
    res = await client.put(f'/v1/dataset/{dataset_id}/schemaTPL/{geid}', json=payload)
    assert res.status_code == 200
    assert res.json()[0]['error_msg'] == 'The template name already exists.'
    assert res.json()[0]['code'] == 403


async def test_delete_schema_template_by_geid_should_return_200(client, httpx_mock, schema_template):
    dataset_id = schema_template.dataset_geid
    geid = schema_template.geid

    res = await client.delete(f'/v1/dataset/{dataset_id}/schemaTPL/{geid}')
    assert res.status_code == 200
    assert res.json()['result'] == schema_template.to_dict()


async def test_delete_schema_template_by_geid_should_return_404(client):
    dataset_id = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    res = await client.delete(f'/v1/dataset/{dataset_id}/schemaTPL/any')
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'template any is not found'
    assert res.json()['code'] == 404
