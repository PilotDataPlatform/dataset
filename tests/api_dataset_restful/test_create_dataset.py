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


@pytest.mark.parametrize('code', [('ot'), ('ascbdascbdascbdascbdascbdascbda12'), ('ps!@#'), (' ')])
async def test_create_dataset_invalid_code_should_return_400(client, httpx_mock, code):
    httpx_mock.add_response(
        method='POST',
        url='http://NEO4J_SERVICE/v1/neo4j/nodes/Dataset/query',
        json=[],
    )

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
