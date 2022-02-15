from pydantic import BaseModel, Field
from .base_models import APIResponse, PaginationRequest


class PublishRequest(BaseModel):
    operator: str
    notes: str
    version: str

class PublishResponse(APIResponse):
    result: dict = Field({}, example={"status_id": ""})

class VersionResponse(APIResponse):
    result: dict = Field({}, example={})

class VersionRequest(BaseModel):
    version: str

class VersionListRequest(PaginationRequest):
    sorting: str = "created_at"
    
