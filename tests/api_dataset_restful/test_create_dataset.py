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
from sqlalchemy.future import select

from app.models.dataset import Dataset

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize('code', [('ot'), ('ascbdascbdascbdascbdascbdascbda12'), ('ps!@#'), (' ')])
async def test_create_dataset_invalid_code_should_return_400(client, code, test_db):
    payload = {
        'username': 'amyguindoc14',
        'title': '123',
        'authors': ['123'],
        'type': 'GENERAL',
        'description': '123',
        'code': code,
    }
    res = await client.post('/v1/dataset', json=payload)
    assert res.status_code == 400
    assert res.json()['error_msg'] == 'Invalid code'


async def test_create_dataset_should_return_200(client, httpx_mock, db_session, schema_essential_template, mock_minio):
    httpx_mock.add_response(
        method='POST',
        url='http://cataloguing_service/v1/entity',
        json=[],
    )
    httpx_mock.add_response(
        method='POST',
        url='http://queue_service/v1/broker/pub',
        json=[],
    )

    payload = {
        'username': 'amyguindoc14',
        'title': '123',
        'authors': ['123'],
        'type': 'GENERAL',
        'description': '123',
        'code': 'datasetcode',
    }
    res = await client.post('/v1/dataset', json=payload)
    assert res.status_code == 200
    qyery = select(Dataset)
    created_dataset = (await db_session.execute(qyery)).scalars().one()
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': {**created_dataset.to_dict()},
        'total': 1,
    }
    db_session.delete(created_dataset)
