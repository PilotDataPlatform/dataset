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

from pydantic import BaseModel
from pydantic import Field

from .base import APIResponse
from .base import PaginationRequest


class PublishRequest(BaseModel):
    operator: str
    notes: str
    version: str


class PublishResponse(APIResponse):
    result: dict = Field({}, example={'status_id': ''})


class VersionResponse(APIResponse):
    result: dict = Field({}, example={})


class VersionRequest(BaseModel):
    version: str


class VersionListRequest(PaginationRequest):
    sorting: str = 'created_at'
