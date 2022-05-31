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


class MetadataClient:

    BASE_URL = ConfigClass.METADATA_SERVICE

    @classmethod
    async def get(cls, url: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params)
        response.raise_for_status()

        return response.json()['result']

    @classmethod
    async def get_dataset_objects(cls, dataset_code: str) -> Dict[str, Any]:
        url = f'{cls.BASE_URL}/v1/items/search'
        params = {'zone': 1, 'container_type': 'dataset', 'container_code': dataset_code, 'page_size': 100000}
        return await cls.get(url, params)

    @classmethod
    async def get_by_id(cls, id_: str) -> Dict[str, Any]:
        url = f'{cls.BASE_URL}/v1/item/{id_}'
        return await cls.get(url)

    @classmethod
    async def check_duplicate_name(cls, code: str, name: str, parent_id: str) -> bool:
        objects = await cls.get_dataset_objects(code)
        for obj in objects:
            if obj['name'] == name and obj['parent'] == parent_id:
                return True
        return False

    @classmethod
    async def create_object(cls, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f'{cls.BASE_URL}/v1/item'
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload)
        response.raise_for_status()
        return response.json()['result']