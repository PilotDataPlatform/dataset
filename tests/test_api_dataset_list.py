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


async def test_get_dataset_list_when_error_should_return_500(client):
    username = 'admin'
    res = await client.post(f'/v1/users/{username}/datasets', json={'order_by': 'desc'})
    assert res.status_code == 500
    assert 'error' in res.json()['error_msg']


async def test_get_dataset_list_should_return_200(client, dataset):
    username = 'admin'
    res = await client.post(f'/v1/users/{username}/datasets', json={'order_by': 'desc'})
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 1,
        'result': [dataset.to_dict()],
        'total': 1,
    }
    assert res.status_code == 200
    assert res.json() == {
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 1,
        'result': [dataset.to_dict()],
        'total': 1,
    }
