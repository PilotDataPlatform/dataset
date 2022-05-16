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


async def test_get_dataset_peek_should_return_200(client, dataset):
    dataset_code = dataset.code
    res = await client.get(f'/v1/dataset-peek/{dataset_code}')
    assert res.status_code == 200


async def test_get_dataset_peek_not_found_should_return_404(client, test_db):
    res = await client.get('/v1/dataset-peek/dataset_code')
    assert res.status_code == 404
    assert res.json()['error_msg'] == 'Not Found, invalid dataset code'


async def test_get_dataset_peek_error_should_return_500(client):
    res = await client.get('/v1/dataset-peek/dataset_code')
    assert res.status_code == 500
    assert 'error' in res.json()['error_msg']
