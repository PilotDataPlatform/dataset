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

from typing import Any
from typing import Dict

import httpx

from app.config import ConfigClass

from .base import BaseClient


class MetadataClient(BaseClient):

    BASE_URL = ConfigClass.METADATA_SERVICE
    ITEM_URL = f'{BASE_URL}/v1/item/'
    SEARCH_URL = f'{BASE_URL}/v1/items/search/'

    @classmethod
    async def get_objects(cls, code: str, items_type: str = 'dataset') -> Dict[str, Any]:

        params = {
            'recursive': True,
            'zone': 1,
            'container_type': items_type,
            'page_size': 100000,
            'container_code': code,
        }
        return (await cls.get(cls.SEARCH_URL, params))['result']

    @classmethod
    async def get_by_id(cls, id_: str) -> Dict[str, Any]:
        url = f'{cls.ITEM_URL}{id_}/'
        return (await cls.get(url))['result']

    @classmethod
    async def create_object(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        payload.update({'zone': 1})
        if not payload.get('parent'):
            payload['parent_path'] = None

        async with httpx.AsyncClient() as client:
            response = await client.post(cls.ITEM_URL, json=payload)
        response.raise_for_status()
        return response.json()['result']

    @classmethod
    async def delete_object(cls, id_: str) -> None:
        async with httpx.AsyncClient() as client:
            response = await client.delete(url=cls.ITEM_URL, params={'id': id_})
        response.raise_for_status()
