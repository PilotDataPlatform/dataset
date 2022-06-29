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

from app.config import ConfigClass
from app.startup import api_registry
from app.startup import create_app
from app.startup import on_startup_event

app: FastAPI = create_app(
    title='Service Dataset',
    description='Service Dataset',
    debug=ConfigClass.DEBUG,
    docs_url='/v1/api-doc',
    version=ConfigClass.VERSION,
)

api_registry(app)


@app.on_event('startup')
async def startup() -> None:
    await on_startup_event(app)
