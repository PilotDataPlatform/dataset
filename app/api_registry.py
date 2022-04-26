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

from fastapi import FastAPI

from .routers import api_root
from .routers.v1 import api_activity_logs
from .routers.v1 import api_dataset_folder
from .routers.v1 import api_dataset_list
from .routers.v1 import api_dataset_restful
from .routers.v1 import api_preview
from .routers.v1 import dataset_file
from .routers.v1.api_schema import api_schema
from .routers.v1.api_schema import api_schema_template
from .routers.v1.api_version import api_version


def api_registry(app: FastAPI):
    app.include_router(api_root.router, prefix='/v1')
    app.include_router(dataset_file.router, prefix='/v1')
    app.include_router(api_dataset_restful.router)
    app.include_router(api_dataset_list.router)
    app.include_router(api_activity_logs.router, prefix='/v1')
    app.include_router(api_preview.router)
    app.include_router(api_version.router)
    app.include_router(api_dataset_folder.router)
    app.include_router(api_schema_template.router, prefix='/v1')
    app.include_router(api_schema.router)
