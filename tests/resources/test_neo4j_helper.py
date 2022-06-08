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

from app.resources.utils import get_children_nodes

pytestmark = pytest.mark.asyncio


async def test_get_children_nodes_should_return_the_correct_list(httpx_mock):
    code = 'any'
    father_id = '0defc217-238f-45d0-afff-b2b2cfb294a6'
    expected_child = {
        'id': 'eb4a9b1d-73f5-40d5-bef2-e9f89d533d8b',
        'parent': father_id,
        'parent_path': 'father',
        'name': 'son',
    }

    files_list = [
        {
            'id': father_id,
            'parent': None,
            'parent_path': None,
            'name': 'father',
        },
        {**expected_child},
        {
            'id': '6c8b410d-09de-45dc-b923-66d15955f4c7',
            'parent': 'eb4a9b1d-73f5-40d5-bef2-e9f89d533d8b',
            'parent_path': 'father.son',
            'name': '164132046.png',
        },
        {
            'id': '0efa7de1-4581-4e7e-83da-50c6aa4682d8',
            'parent': None,
            'parent_path': None,
            'name': 'father',
        },
    ]

    httpx_mock.add_response(
        method='GET',
        url=(
            'http://metadata_service/v1/items/search/?' f'recursive=true&zone=1&container_code={code}&page_size=100000'
        ),
        json={'result': files_list},
    )

    returned_list = await get_children_nodes(code, father_id)
    assert returned_list == [expected_child]
