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

from common import LoggerFactory
from fastapi import APIRouter
from fastapi import Depends
from fastapi_utils import cbv

from app.core.db import get_db_session
from app.resources.error_handler import catch_internal
from app.schemas.base import APIResponse
from app.schemas.base import EAPIResponseCode
from app.schemas.reqres_dataset import DatasetListForm
from app.schemas.reqres_dataset import DatasetListResponse
from app.services.dataset import SrvDatasetMgr

router = APIRouter()

_API_TAG = 'V1 Dataset List API'
_API_NAMESPACE = 'api_dataset_list'


@cbv.cbv(router)
class DatasetList:
    """API Dataset List."""

    def __init__(self):
        self.__logger = LoggerFactory(_API_NAMESPACE).get_logger()

    @router.post(
        '/v1/users/{creator}/datasets', tags=[_API_TAG], response_model=DatasetListResponse, summary='list datasets.'
    )
    @catch_internal(_API_NAMESPACE)
    async def list_dataset(self, creator, request_payload: DatasetListForm, db=Depends(get_db_session)):
        """dataset creation api."""
        res = APIResponse()
        page = request_payload.page if request_payload.page else 1
        page_size = request_payload.page_size

        try:
            srv_dataset = SrvDatasetMgr()
            pagination = await srv_dataset.get_dataset_by_creator(db, creator, page, page_size)
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.error_msg = 'error: ' + str(e)
            return res.json_response()

        res.code = EAPIResponseCode.success
        res.total = pagination.total
        res.page = pagination.page
        res.num_of_pages = math.ceil(pagination.total / page_size)
        res.result = [item.to_dict() for item in pagination.items]
        return res.json_response()
