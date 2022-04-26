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

import math

import httpx
from fastapi import APIRouter
from fastapi_utils import cbv

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.config import ConfigClass
from app.models.base_models import APIResponse
from app.models.base_models import EAPIResponseCode
from app.models.reqres_dataset import DatasetListForm
from app.models.reqres_dataset import DatasetListResponse
from app.resources.error_handler import catch_internal

router = APIRouter()

_API_TAG = 'V1 Dataset List API'
_API_NAMESPACE = 'api_dataset_list'


@cbv.cbv(router)
class DatasetList:
    """API Dataset List."""

    def __init__(self):
        self.__logger = SrvLoggerFactory(_API_NAMESPACE).get_logger()

    @router.post(
        '/v1/users/{username}/datasets', tags=[_API_TAG], response_model=DatasetListResponse, summary='list datasets.'
    )
    @catch_internal(_API_NAMESPACE)
    async def list_dataset(self, username, request_payload: DatasetListForm):
        """dataset creation api."""
        res = APIResponse()
        page = request_payload.page
        page_size = request_payload.page_size

        page_kwargs = {
            'order_by': request_payload.order_by,
            'order_type': request_payload.order_type,
            'skip': page * page_size,
            'limit': page_size,
        }

        query_payload = {**page_kwargs, 'query': {'creator': username, 'labels': ['Dataset']}}
        try:
            with httpx.Client() as client:
                response = client.post(ConfigClass.NEO4J_SERVICE_V2 + 'nodes/query', json=query_payload)
            if response.status_code != 200:
                error_msg = response.json()
                res.code = EAPIResponseCode.internal_error
                res.error_msg = f'Neo4j error: {error_msg}'
                return res.json_response()
            nodes = response.json()['result']
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.error_msg = 'Neo4j error: ' + str(e)
            return res.json_response()

        res.code = EAPIResponseCode.success
        res.total = response.json()['total']
        res.page = page
        res.num_of_pages = math.ceil(res.total / page_size)
        res.result = nodes
        return res.json_response()
