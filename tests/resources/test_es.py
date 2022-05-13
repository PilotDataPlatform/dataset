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

import json

import pytest

from app.resources.es_helper import get_one_by_id
from app.resources.es_helper import insert_one
from app.resources.es_helper import insert_one_by_id
from app.resources.es_helper import search

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


@pytest.mark.parametrize(
    'search_type,query',
    [
        ('should', {'bool': {'should': [{'match': {'attributes.value': '1'}}]}}),
        ('wildcard', {'wildcard': {'attributes.value': '1'}}),
        ('match', {'match': {'attributes.value': '1'}}),
        ('must', {'bool': {'must': [{'match': {'attributes.value': '1'}}]}}),
    ],
)
async def test_search_nested_in_elastic_search_should_call_correct_endpoint(httpx_mock, search_type, query):
    base_query = {
        'query': {
            'bool': {
                'must': [
                    {
                        'nested': {
                            'path': 'field',
                            'query': {
                                'bool': {
                                    'must': [
                                        {'match': {'attributes.name': 'name'}},
                                        {'match': {'attributes.attribute_name': 'type'}},
                                        {**query},
                                    ]
                                }
                            },
                        }
                    }
                ]
            }
        },
        'size': 10,
        'from': 10,
        'sort': [{'null': None}],
    }
    httpx_mock.add_response(
        method='GET',
        url='http://elastic_search_service/index/_search',
        json={},
        match_content=json.dumps(base_query).encode('utf-8'),
    )
    data = [
        {
            'nested': True,
            'name': 'name',
            'attribute_name': 'type',
            'search_type': search_type,
            'value': '1',
            'range': '10',
            'field': 'field',
        }
    ]
    result = await search('index', 1, 10, data, sort_by=None, sort_type=None)
    assert result == {}


async def test_search_range_in_elastic_search_should_call_correct_endpoint(httpx_mock):
    httpx_mock.add_response(
        method='GET',
        url='http://elastic_search_service/index/_search',
        json={},
        match_content=json.dumps(
            {
                'query': {'bool': {'must': [{'range': {'field': {'gte': 1, 'lte': 0}}}]}},
                'size': 10,
                'from': 10,
                'sort': [{'null': None}],
            }
        ).encode('utf-8'),
    )
    data = [
        {
            'nested': False,
            'name': 'name',
            'attribute_name': 'type',
            'value': '1',
            'range': '10',
            'field': 'field',
        }
    ]
    result = await search('index', 1, 10, data, sort_by=None, sort_type=None)
    assert result == {}


@pytest.mark.parametrize(
    'search_type,query',
    [
        ('should', 'should'),
        ('contain', 'must'),
    ],
)
async def test_search_multi_value_in_elastic_search_should_call_correct_endpoint(httpx_mock, search_type, query):
    httpx_mock.add_response(
        method='GET',
        url='http://elastic_search_service/index/_search',
        json={},
        match_content=json.dumps(
            {
                'query': {'bool': {'must': [{'bool': {f'{query}': [{'term': {'field': '1'}}]}}]}},
                'size': 10,
                'from': 10,
                'sort': [{'null': None}],
            }
        ).encode('utf-8'),
    )
    data = [
        {
            'nested': None,
            'multi_values': True,
            'name': 'name',
            'attribute_name': 'type',
            'value': '1',
            'range': None,
            'field': 'field',
            'search_type': search_type,
        }
    ]
    result = await search('index', 1, 10, data, sort_by=None, sort_type=None)
    assert result == {}
