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

from app.resources.es_helper import get_one_by_id
from app.resources.es_helper import insert_one
from app.resources.es_helper import insert_one_by_id

pytestmark = pytest.mark.asyncio


async def test_insert_one_in_elastic_search_should_call_correct_endpoint(httpx_mock):
    httpx_mock.add_response(method='POST', url='http://elastic_search_service/index/type', json={})
    result = insert_one('type', 'index', {'key': 'value'})
    assert result == {}


async def test_insert_one_by_id_in_elastic_search_should_call_correct_endpoint(httpx_mock):
    httpx_mock.add_response(method='PUT', url='http://elastic_search_service/index/type/dataset_id', json={})
    result = insert_one_by_id('type', 'index', {'key': 'value'}, 'dataset_id')
    assert result == {}


async def test_get_one_by_id_in_elastic_search_should_call_correct_endpoint(httpx_mock):
    httpx_mock.add_response(method='GET', url='http://elastic_search_service/index/type/dataset_id', json={})
    result = get_one_by_id('index', 'type', 'dataset_id')
    assert result == {}
