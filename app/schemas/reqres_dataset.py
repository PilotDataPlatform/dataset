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

from typing import List

from pydantic import BaseModel
from pydantic import Field

from .base import APIResponse


class Activity(BaseModel):
    action: str
    resource: str
    detail = {}


class DatasetPostForm(BaseModel):
    """DatasetPostForm."""

    username: str
    title: str
    code: str
    authors: list
    type: str = 'GENERAL'
    modality: List[str] = []
    collection_method: list = []
    license: str = ''
    tags: list = []
    description: str
    file_count: int = 0
    total_size: int = 0  # unit as byte


class DatasetPostResponse(APIResponse):
    """DatasetPostResponse."""

    result: dict = Field(
        {},
        example={
            'global_entity_id': 'xxxxx',
            'source': 'project_geid',
            'title': 'title',
            'authors': [
                'author1',
            ],
            'code': '(unique identifier)',
            'creator': 'creator',
            'type': 'type',
            'modality': 'modality',
            'collection_method': [
                'collection_method',
            ],
            'license': 'license',
            'tags': [
                'tag',
            ],
            'description': 'description',
            'size': 0,
            'total_files': 0,
        },
    )


class DatasetVerifyForm(BaseModel):
    dataset_geid: str
    type: str


class DatasetListForm(BaseModel):
    filter = {}
    order_by: str
    order_type = 'desc'
    page: int = 0
    page_size: int = 10


class DatasetListResponse(APIResponse):
    """List response."""

    result: dict = Field(
        {},
        example=[
            {
                'global_entity_id': 'xxxxx',
                'source': 'project_geid',
                'title': 'title',
                'authors': [
                    'author1',
                ],
                'code': '(unique identifier)',
                'creator': 'creator',
                'type': 'type',
                'modality': 'modality',
                'collection_method': [
                    'collection_method',
                ],
                'license': 'license',
                'tags': [
                    'tag',
                ],
                'description': 'description',
                'size': 0,
                'total_files': 0,
            }
        ],
    )
