from pydantic import BaseModel, Field
from typing import Optional, List
from .base_models import APIResponse, PaginationRequest


class POSTSchema(BaseModel):
    name: str
    dataset_geid: str
    tpl_geid: str
    standard: str 
    system_defined: bool 
    is_draft: bool 
    content: dict 
    creator: str 
    activity: list 

class POSTSchemaResponse(APIResponse):
    result: dict = Field({}, example={
        'code': 200,
        'error_msg': '',
        'num_of_pages': 1,
        'page': 0,
        'result': {   
            'content': {"testing": "test"},
            'create_timestamp': '2021-08-23T15:57:17.417Z',
            'creator': 'admin',
            'dataset_geid': '9ff8382d-f476-4cdf-a357-66c4babf8320-1626104650',
            'geid': 'da2d18af-23d2-454a-b10e-34ad7bdfa26f-1629734237',
            'is_draft': 'True',
            'name': 'None',
            'standard': 'default',
            'system_defined': 'True',
            'tpl_geid': '3733ba1c-9886-414c-b8f4-3b85507079c6-1629474851',
            'update_timestamp': '2021-08-23T15:57:17.417Z'
        },
        'total': 1
    })

class GETSchemaResponse(APIResponse):
    result: dict = Field({}, example={})

class PUTSchema(BaseModel):
    name: str = None
    dataset_geid: str = None
    tpl_geid: str = None 
    standard: str  = None 
    system_defined: bool = None 
    is_draft: bool = None
    content: dict = None 
    creator: str = None 
    activity: list
    username: str

class PUTSchemaResponse(APIResponse):
    result: dict = Field({}, example={})

class DELETESchemaResponse(APIResponse):
    result: str = Field("", example="success")

class DELETESchema(BaseModel):
    dataset_geid: str
    username: str
    activity: list

class POSTSchemaList(BaseModel):
    name: Optional[str]
    dataset_geid: Optional[str]
    tpl_geid: Optional[str]
    standard: Optional[str]
    system_defined: Optional[bool]
    is_draft: Optional[bool]
    create_timestamp: Optional[float]
    update_timestamp: Optional[float]
    creator: Optional[str]