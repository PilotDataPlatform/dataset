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

from app.routers import api_health
from app.routers import api_root
from app.routers.v1 import api_dataset_folder
from app.routers.v1 import api_dataset_list
from app.routers.v1 import api_dataset_restful
from app.routers.v1 import api_preview
from app.routers.v1 import dataset_file
from app.routers.v1.api_schema import api_schema
from app.routers.v1.api_schema import api_schema_template
from app.routers.v1.api_version import api_version


def api_registry(app: FastAPI):
    app.include_router(api_health.router)
    app.include_router(api_root.router, prefix='/v1')
    app.include_router(dataset_file.router, prefix='/v1')
    app.include_router(api_dataset_restful.router)
    app.include_router(api_dataset_list.router)
    app.include_router(api_preview.router)
    app.include_router(api_version.router)
    app.include_router(api_dataset_folder.router)
    app.include_router(api_schema_template.router, prefix='/v1')
    app.include_router(api_schema.router)
