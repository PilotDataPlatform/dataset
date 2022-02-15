from .base_models import APIResponse
from pydantic import Field, BaseModel

class FolderResponse(APIResponse):
    result: dict = Field({}, example={})

class FolderRequest(BaseModel):
    folder_name: str
    username: str
    parent_folder_geid: str = ""
