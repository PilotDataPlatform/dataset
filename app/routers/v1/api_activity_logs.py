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

import json
from datetime import datetime
from datetime import timezone
from typing import Optional

from common import LoggerFactory
from fastapi import APIRouter
from fastapi import Depends
from fastapi_utils import cbv
from sqlalchemy.future import select

from app.core.db import get_db_session
from app.models.version import DatasetVersion
from app.resources.error_handler import catch_internal
from app.resources.es_helper import search
from app.schemas.base import APIResponse
from app.schemas.base import EAPIResponseCode

router = APIRouter()

_API_TAG = 'V1 Activity Logs Query API'
_API_NAMESPACE = 'api_activity_logs'


@cbv.cbv(router)
class ActivityLogs:
    """API Activity Logs."""

    def __init__(self):
        self.__logger = LoggerFactory(_API_NAMESPACE).get_logger()

    @router.get('/activity-logs', tags=[_API_TAG], summary='list activity logs.')
    @catch_internal(_API_NAMESPACE)
    async def query_activity_logs(
        self,
        query: str,
        page: Optional[int] = 0,
        page_size: Optional[int] = 10,
        sort_by: Optional[str] = 'create_timestamp',
        sort_type: Optional[str] = 'desc',
    ):
        response = APIResponse()
        queries = json.loads(query)
        search_params = []

        self.__logger.info('activity logs query: {}'.format(query))

        try:
            for key in queries:
                if key == 'create_timestamp':
                    filed_params = {
                        'nested': False,
                        'field': key,
                        'range': queries[key]['value'],
                        'multi_values': False,
                        'search_type': queries[key]['condition'],
                    }
                    search_params.append(filed_params)
                else:
                    filed_params = {
                        'nested': False,
                        'field': key,
                        'range': False,
                        'multi_values': False,
                        'value': queries[key]['value'],
                        'search_type': queries[key]['condition'],
                    }
                    search_params.append(filed_params)

            res = await search('activity-logs', page, page_size, search_params, sort_by, sort_type)

            self.__logger.info('activity logs result: {}'.format(res))

            response.code = EAPIResponseCode.success
            response.result = res['hits']['hits']
            response.total = res['hits']['total']['value']
            return response
        except Exception as e:
            self.__logger.error('activity logs error: {}'.format(str(e)))
            response.code = EAPIResponseCode.internal_error
            response.result = {'errors': str(e)}
            return response

    @router.get('/activity-logs/{dataset_geid}', tags=[_API_TAG], summary='list activity logs.')
    @catch_internal(_API_NAMESPACE)
    async def query_activity_logs_by_version(
        self,
        dataset_geid: str,
        version: str,
        page: Optional[int] = 0,
        page_size: Optional[int] = 10,
        db=Depends(get_db_session),
    ):
        response = APIResponse()

        try:
            query = (
                select(DatasetVersion)
                .where(DatasetVersion.dataset_geid == dataset_geid, DatasetVersion.version == version)
                .order_by(DatasetVersion.created_at.desc())
            )
            versions = (await db.execute(query)).scalars()
            version_info = versions.first()

            if not version_info:
                response.code = EAPIResponseCode.bad_request
                response.result = 'there is no version information for dataset {}'.format(dataset_geid)
                return response

            version_data = version_info.to_dict()
            created_at = version_data['created_at']
            created_at = created_at[:19]
            if created_at[10] == ' ':
                create_timestamp = (
                    datetime.strptime(created_at, '%Y-%m-%d %H:%M:%S').replace(tzinfo=timezone.utc).timestamp()
                )
            else:
                create_timestamp = (
                    datetime.strptime(created_at, '%Y-%m-%dT%H:%M:%S').replace(tzinfo=timezone.utc).timestamp()
                )

        except Exception as e:
            self.__logger.error('Psql Error: ' + str(e))
            response.code = EAPIResponseCode.internal_error
            response.result = 'Psql Error: ' + str(e)
            return response

        search_params = []

        search_params.append(
            {
                'nested': False,
                'field': 'create_timestamp',
                'range': [int(create_timestamp)],
                'multi_values': False,
                'search_type': 'gte',
            }
        )

        search_params.append(
            {
                'nested': False,
                'field': 'dataset_geid',
                'range': False,
                'multi_values': False,
                'value': dataset_geid,
                'search_type': 'equal',
            }
        )

        try:
            res = await search('activity-logs', page, page_size, search_params, 'create_timestamp', 'desc')
        except Exception as e:
            self.__logger.error('Elastic Search Error: ' + str(e))
            response.code = EAPIResponseCode.internal_error
            response.result = 'Elastic Search Error: ' + str(e)
            return response

        response.code = EAPIResponseCode.success
        response.result = res['hits']['hits']
        response.total = res['hits']['total']['value']
        return response
