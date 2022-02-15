from enum import Enum
from pydantic import BaseModel, Field
from typing import Optional, List
from .base_models import APIResponse


class Activity(BaseModel):
    action: str
    resource: str
    detail = {}


class DatasetPostForm(BaseModel):
    '''
    DatasetPostForm
    '''
    username: str
    title: str
    code: str
    authors: list
    type: str = "GENERAL"
    modality: List[str] = []
    collection_method: list = []
    license: str = ""
    tags: list = []
    description: str
    file_count: int = 0
    total_size: int = 0  # unit as byte


class DatasetPostResponse(APIResponse):
    '''
    DatasetPostResponse
    '''
    result: dict = Field({}, example={
        "global_entity_id": "xxxxx",
        "source": "project_geid",
        "title": "title",
        "authors": ["author1", ],
        "code": "(unique identifier)",
        "creator": "creator",
        "type": "type",
        "modality": "modality",
        "collection_method": ["collection_method", ],
        "license": "license",
        "tags": ["tag", ],
        "description": "description",
        "size": 0,
        "total_files": 0
    })


class DatasetPutForm(BaseModel):
    '''
    DatasetPostForm
    '''
    title: Optional[str]
    authors: Optional[list]
    modality: Optional[List[str]]
    collection_method: Optional[list]
    license: Optional[str]
    tags: Optional[list]
    description: Optional[str]
    file_count: Optional[int]
    total_size: Optional[int]  # unit as byte
    activity: List[Activity]


class DatasetVerifyForm(BaseModel):
    dataset_geid: str
    type: str


class DatasetListForm(BaseModel):
    filter = {}
    order_by: str
    order_type = "desc"
    page: int = 0
    page_size: int = 10


class DatasetListResponse(APIResponse):
    '''
    List response
    '''
    result: dict = Field({}, example=[
        {
            "global_entity_id": "xxxxx",
            "source": "project_geid",
            "title": "title",
            "authors": ["author1", ],
            "code": "(unique identifier)",
            "creator": "creator",
            "type": "type",
            "modality": "modality",
            "collection_method": ["collection_method", ],
            "license": "license",
            "tags": ["tag", ],
            "description": "description",
            "size": 0,
            "total_files": 0
        }
    ])
