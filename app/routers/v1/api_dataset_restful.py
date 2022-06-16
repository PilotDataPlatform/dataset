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
import shutil
import subprocess
import time
from typing import Optional

import httpx
from common import LoggerFactory
from fastapi import APIRouter
from fastapi import Depends
from fastapi import Header
from fastapi_utils import cbv
from sqlalchemy.future import select
from starlette.concurrency import run_in_threadpool

from app.clients import MetadataClient
from app.config import ConfigClass
from app.core.db import get_db_session
from app.models.bids import BIDSResult
from app.resources.error_handler import catch_internal
from app.resources.utils import get_files_all_files
from app.resources.utils import get_node_relative_path
from app.resources.utils import make_temp_folder
from app.schemas.base import APIResponse
from app.schemas.base import EAPIResponseCode
from app.schemas.reqres_dataset import DatasetPostForm
from app.schemas.reqres_dataset import DatasetPostResponse
from app.schemas.reqres_dataset import DatasetVerifyForm
from app.schemas.validator_dataset import DatasetValidator
from app.services.dataset import SrvDatasetMgr

router = APIRouter()

_API_TAG = 'V1 Dataset Restful'
_API_NAMESPACE = 'api_dataset_restful'


@cbv.cbv(router)
class DatasetRestful:
    """API Dataset Restful."""

    def __init__(self):
        self.__logger = LoggerFactory(_API_NAMESPACE).get_logger()

    @router.post('/v1/dataset', tags=[_API_TAG], response_model=DatasetPostResponse, summary='Create a dataset.')
    @catch_internal(_API_NAMESPACE)
    async def create_dataset(self, request_payload: DatasetPostForm, db=Depends(get_db_session)):
        """dataset creation api."""
        res = APIResponse()
        srv_dataset = SrvDatasetMgr()
        check_created = await srv_dataset.get_bycode(db, request_payload.code)
        if check_created:
            res.result = None
            res.error_msg = "[Invalid 'code']: already taken by other dataset."
            res.code = EAPIResponseCode.conflict
            return res.json_response()

        post_dict = request_payload.dict()
        for k, v in post_dict.items():
            if v is not None:
                # use the factory to get the validator function
                validator = DatasetValidator.get(k)
                validation = validator(v)
                if not validation:
                    res.code = EAPIResponseCode.bad_request
                    res.result = None
                    res.error_msg = 'Invalid {}'.format(k)
                    return res.json_response()

        created = await srv_dataset.create(
            db,
            request_payload.username,
            request_payload.code,
            request_payload.title,
            request_payload.authors,
            request_payload.type,
            request_payload.modality,
            request_payload.collection_method,
            request_payload.tags,
            request_payload.license,
            request_payload.description,
        )

        res.code = EAPIResponseCode.success
        res.result = created
        return res.json_response()

    @router.get(
        '/v1/dataset/{dataset_geid}', tags=[_API_TAG], response_model=DatasetPostResponse, summary='Get a dataset.'
    )
    @catch_internal(_API_NAMESPACE)
    async def get_dataset(self, dataset_geid, db=Depends(get_db_session)):
        """dataset creation api."""
        res = APIResponse()
        srv_dataset = SrvDatasetMgr()
        try:
            dataset = await srv_dataset.get_bygeid(db, dataset_geid)
            if dataset:
                res.code = EAPIResponseCode.success
                res.result = dataset.to_dict()
                return res.json_response()
            else:
                res.code = EAPIResponseCode.not_found
                res.result = {}
                res.error_msg = 'Not Found, invalid geid'
                return res.json_response()
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.error_msg = str(e)
            res.result = {}
            return res.json_response()

    @router.get(
        '/v1/dataset-peek/{code}', tags=[_API_TAG], response_model=DatasetPostResponse, summary='Get a dataset.'
    )
    @catch_internal(_API_NAMESPACE)
    async def get_dataset_bycode(self, code, db=Depends(get_db_session)):
        """dataset creation api."""
        res = APIResponse()

        srv_dataset = SrvDatasetMgr()
        try:
            dataset = await srv_dataset.get_bycode(db, code)
            if dataset:
                res.code = EAPIResponseCode.success
                res.result = dataset.to_dict()
                return res.json_response()
            else:
                res.code = EAPIResponseCode.not_found
                res.result = {}
                res.error_msg = 'Not Found, invalid dataset code'
                return res.json_response()
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.error_msg = str(e)
            res.result = {}
            return res.json_response()

    @router.post('/v1/dataset/verify', tags=[_API_TAG], summary='verify a bids dataset.')
    @catch_internal(_API_NAMESPACE)
    async def verify_dataset(self, request_payload: DatasetVerifyForm, db=Depends(get_db_session)):
        res = APIResponse()
        srv_dataset = SrvDatasetMgr()
        payload = request_payload.dict()

        dataset = await srv_dataset.get_bygeid(db, payload['dataset_geid'])
        if not dataset:
            res.code = EAPIResponseCode.bad_request
            res.result = {'result': 'dataset not exist'}
            return res.json_response()
        items = await MetadataClient.get_objects(dataset.code)

        files_info = []
        TEMP_FOLDER = 'temp/'
        for item in items:
            if item['type'].lower() == 'file':
                file_path = get_node_relative_path(dataset.code, item['storage']['location_uri'])
                files_info.append({'file_path': TEMP_FOLDER + dataset.code + file_path, 'file_size': item['size']})

            if item['type'].lower() == 'folder':
                files = await get_files_all_files(item['id'], items)
                for file in files:
                    file_path = get_node_relative_path(dataset.code, item['storage']['location_uri'])
                    files_info.append({'file_path': TEMP_FOLDER + dataset.code + file_path, 'file_size': file['size']})

        try:
            await run_in_threadpool(make_temp_folder, files_info)
        except Exception:
            res.code = EAPIResponseCode.internal_error
            res.result = 'failed to create temp folder for bids'
            return res.json_response()

        try:
            result = subprocess.run(
                [
                    'bids-validator',
                    TEMP_FOLDER + dataset.code,
                    '--json',
                    '--ignoreNiftiHeaders',
                    '--ignoreSubjectConsistency',
                ],
                stdout=subprocess.PIPE,
            )
        except Exception:
            res.code = EAPIResponseCode.internal_error
            res.result = 'failed to validate bids folder'
            return res.json_response()

        try:
            await run_in_threadpool(shutil.rmtree, TEMP_FOLDER + dataset.code)
        except Exception:
            res.code = EAPIResponseCode.internal_error
            res.result = 'failed to remove temp bids folder'
            return res.json_response()

        res.result = json.loads(result.stdout)
        return res.json_response()

    @router.post('/v1/dataset/verify/pre', tags=[_API_TAG], summary='pre verify a bids dataset.')
    @catch_internal(_API_NAMESPACE)
    async def pre_verify_dataset(
        self,
        request_payload: DatasetVerifyForm,
        Authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
        db=Depends(get_db_session),
    ):
        res = APIResponse()
        srv_dataset = SrvDatasetMgr()
        payload = request_payload.dict()

        dataset = await srv_dataset.get_bygeid(db, payload['dataset_geid'])
        if not dataset:
            res.code = EAPIResponseCode.bad_request
            res.result = {'result': 'dataset not exist'}
            return res.json_response()

        access_token = Authorization.split(' ')[1]

        payload = {
            'event_type': 'bids_validate',
            'payload': {
                'dataset_geid': str(dataset.id),
                'access_token': access_token,
                'refresh_token': refresh_token,
                'project': 'dataset',
            },
            'create_timestamp': time.time(),
        }
        url = ConfigClass.SEND_MESSAGE_URL
        self.__logger.info('Sending Message To Queue: ' + str(payload))
        async with httpx.AsyncClient() as client:
            msg_res = await client.post(
                url=url, json=payload, headers={'Content-type': 'application/json; charset=utf-8'}
            )
        if msg_res.status_code != 200:
            res.code = EAPIResponseCode.internal_error
            res.result = {'result': msg_res.text}
            return res.json_response()

        res.code = EAPIResponseCode.success
        res.result = msg_res.json()

        return res.json_response()

    @router.get('/v1/dataset/bids-msg/{dataset_geid}', tags=[_API_TAG], summary='pre verify a bids dataset.')
    @catch_internal(_API_NAMESPACE)
    async def get_bids_msg(self, dataset_geid, db=Depends(get_db_session)):
        api_response = APIResponse()
        try:
            query = (
                select(BIDSResult)
                .where(
                    BIDSResult.dataset_geid == dataset_geid,
                )
                .order_by(BIDSResult.created_time.desc())
            )
            bids_results = (await db.execute(query)).scalars()
            bids_result = bids_results.first()

            if not bids_result:
                api_response.result = {}
                return api_response.json_response()

            bids_result = bids_result.to_dict()
            api_response.result = bids_result
            return api_response.json_response()
        except Exception as e:
            self.__logger.error('Psql Error: ' + str(e))
            api_response.code = EAPIResponseCode.internal_error
            api_response.result = 'Psql Error: ' + str(e)
            return api_response.json_response()
