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

from app.config import ConfigClass

from .base import BaseClient


class ProjectClient(BaseClient):

    BASE_URL = ConfigClass.PROJECT_SERVICE

    @classmethod
    async def get_by_id(cls, id_: str) -> Dict[str, Any]:
        url = f'{cls.BASE_URL}/v1/projects/{id_}/'
        return await cls.get(url)
