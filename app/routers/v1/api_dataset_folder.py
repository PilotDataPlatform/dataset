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

import re

from common import LoggerFactory
from fastapi import APIRouter
from fastapi import Depends
from fastapi_utils import cbv

from app.clients import MetadataClient
from app.core.db import get_db_session
from app.resources.error_handler import APIException
from app.resources.utils import create_node
from app.resources.utils import get_node_by_geid
from app.schemas.base import EAPIResponseCode
from app.schemas.folder import FolderRequest
from app.schemas.folder import FolderResponse
from app.services.dataset import SrvDatasetMgr

logger = LoggerFactory('api_preview').get_logger()
router = APIRouter()


@cbv.cbv(router)
class DatasetFolder:
    """Create an empty folder."""

    @router.post(
        '/v1/dataset/{dataset_geid}/folder',
        tags=['V1 DATASET'],
        response_model=FolderResponse,
        summary='Create an empty folder',
    )
    async def create_folder(self, dataset_geid: str, data: FolderRequest, db=Depends(get_db_session)):
        api_response = FolderResponse()
        srv_dataset = SrvDatasetMgr()
        # length 1-20, exclude invalid character, ensure start & end aren't a space
        folder_pattern = re.compile(r'^(?=.{1,20}$)([^\s\/:?*<>|”]{1})+([^\/:?*<>|”])+([^\s\/:?*<>|”]{1})$')
        match = re.search(folder_pattern, data.folder_name)
        if not match:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = 'Invalid folder name'
            logger.info(api_response.error_msg)
            return api_response.json_response()

        dataset = await srv_dataset.get_bygeid(db, dataset_geid)
        if not dataset:
            logger.error(f'Dataset not found: {dataset_geid}')
            raise APIException(error_msg='Dataset not found', status_code=EAPIResponseCode.not_found.value)

        # Folder is being added to the root of the dataset
        parent_path = None
        parent_id = None
        if data.parent_folder_geid:
            # Folder is being added as a subfolder
            try:
                folder_node = await get_node_by_geid(data.parent_folder_geid)
            except Exception:
                logger.error(f'Folder not found: {data.parent_folder_geid}')
                raise APIException(error_msg='Folder not found', status_code=EAPIResponseCode.not_found.value)
            parent_path = folder_node['name']
            if folder_node['parent_path']:
                parent_path = folder_node['parent_path'] + '.' + folder_node['name']
            parent_id = folder_node['id']

        does_name_exist = False
        items = await MetadataClient.get_objects(dataset.code)
        for item in items:
            if item['name'] == data.folder_name and item['parent'] == parent_id:
                does_name_exist = True

        if does_name_exist:
            api_response.code = EAPIResponseCode.conflict
            api_response.error_msg = 'folder with that name already exists'
            logger.error(api_response.error_msg)
            return api_response.json_response()

        # create node in metadata
        payload = {
            'parent': parent_id,
            'parent_path': parent_path,
            'type': 'folder',
            'name': data.folder_name,
            'owner': data.username,
            'container_code': dataset.code,
            'container_type': 'dataset',
        }
        folder_node = await create_node(payload)
        api_response.result = folder_node
        return api_response.json_response()
