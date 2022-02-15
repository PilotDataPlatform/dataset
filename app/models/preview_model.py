from .base_models import APIResponse
from pydantic import Field

class PreviewResponse(APIResponse):
    result: dict = Field({}, example={
        "content": "<csv data>",
        "type": "csv",
        "is_concatinated": True,
})
