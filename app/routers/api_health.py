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

import logging

from fastapi import APIRouter
from fastapi import Depends
from fastapi import Response
from fastapi.responses import JSONResponse

from app.clients.kafka import is_kafka_connected
from app.core.db import is_db_connected

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get('/health', summary='Healthcheck if all service dependencies are online.')
async def get_db_status(
    is_db_health: bool = Depends(is_db_connected), is_kafka_health: bool = Depends(is_kafka_connected)
) -> Response:
    """Return response that represents status of the database and kafka connections."""

    if is_db_health and is_kafka_health:
        return Response(status_code=204)
    return JSONResponse(status_code=503)
