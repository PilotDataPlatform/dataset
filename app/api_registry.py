from fastapi import FastAPI
from .routers import api_root
from .routers.v1 import dataset_file
from .routers.v1 import api_dataset_restful
from .routers.v1 import api_dataset_list
from .routers.v1 import api_activity_logs
from .routers.v1 import api_preview
from .routers.v1 import api_dataset_folder
from .routers.v1.api_version import api_version
from .routers.v1.api_schema import api_schema, api_schema_template


def api_registry(app: FastAPI):
    app.include_router(api_root.router, prefix="/v1")
    app.include_router(dataset_file.router, prefix="/v1")
    app.include_router(api_dataset_restful.router)
    app.include_router(api_dataset_list.router)
    app.include_router(api_activity_logs.router, prefix="/v1")
    app.include_router(api_preview.router)
    app.include_router(api_version.router)
    app.include_router(api_dataset_folder.router)
    app.include_router(api_schema_template.router, prefix="/v1")
    app.include_router(api_schema.router)
