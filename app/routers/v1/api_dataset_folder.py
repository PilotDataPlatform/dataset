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

from common import GEIDClient
from common import LoggerFactory
from fastapi import APIRouter
from fastapi import Depends
from fastapi_utils import cbv

from app.core.db import get_db_session
from app.models.dataset import Dataset
from app.resources.error_handler import APIException
from app.resources.neo4j_helper import create_node
from app.resources.neo4j_helper import create_relation
from app.resources.neo4j_helper import get_node_by_geid
from app.resources.neo4j_helper import query_relation
from app.schemas.base import EAPIResponseCode
from app.schemas.folder import FolderRequest
from app.schemas.folder import FolderResponse
from app.services.dataset import SrvDatasetMgr

logger = LoggerFactory('api_preview').get_logger()
router = APIRouter()


@cbv.cbv(router)
class DatasetFolder:
    def __init__(self) -> None:
        self.geid_client = GEIDClient()

    '''
        Create an empty folder
    '''

    @router.post(
        '/v1/dataset/{dataset_geid}/folder',
        tags=['V1 DATASET'],
        response_model=FolderResponse,
        summary='Create an empty folder',
    )
    async def create_folder(self, dataset_geid: str, data: FolderRequest, db=Depends(get_db_session)):
        api_response = FolderResponse()
        srv_dataset = SrvDatasetMgr()

        dataset = srv_dataset.get_bygeid(db, dataset_geid)
        # dataset_node  = await get_node_by_geid(dataset_geid, label='Dataset')

        # length 1-20, exclude invalid character, ensure start & end aren't a space
        folder_pattern = re.compile(r'^(?=.{1,20}$)([^\s\/:?*<>|”]{1})+([^\/:?*<>|”])+([^\s\/:?*<>|”]{1})$')
        match = re.search(folder_pattern, data.folder_name)
        if not match:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = 'Invalid folder name'
            logger.info(api_response.error_msg)
            return api_response.json_response()

        if not dataset:
            logger.error(f'Dataset not found: {dataset_geid}')
            raise APIException(error_msg='Dataset not found', status_code=EAPIResponseCode.not_found.value)

        if data.parent_folder_geid:
            # Folder is being added as a subfolder
            start_label = 'Folder'
            folder_node = await get_node_by_geid(data.parent_folder_geid, label='Folder')
            if not folder_node:
                logger.error(f'Folder not found: {data.parent_folder_geid}')
                raise APIException(error_msg='Folder not found', status_code=EAPIResponseCode.not_found.value)
            folder_relative_path = folder_node['folder_relative_path'] + '/' + folder_node['name']
            parent_node = folder_node
        else:
            # Folder is being added to the root of the dataset
            folder_relative_path = 'data'
            start_label = 'Dataset'
            parent_node = dataset

        # Duplicate name check
        if isinstance(parent_node, Dataset):
            parent_node_id = str(parent_node.id)
            folder_level = 0
        else:
            parent_node_id = parent_node.get('global_entity_id')
            folder_level = parent_node.get('folder_level', -1) + 1

        result = await query_relation(
            'own',
            start_label,
            'Folder',
            start_params={'global_entity_id': parent_node_id},
            end_params={'name': data.folder_name},
        )
        if result:
            api_response.code = EAPIResponseCode.conflict
            api_response.error_msg = 'folder with that name already exists'
            logger.error(api_response.error_msg)
            return api_response.json_response()

        # create node in neo4j
        payload = {
            'name': data.folder_name,
            'create_by': data.username,
            'global_entity_id': self.geid_client.get_GEID(),
            'dataset_code': dataset.code,
            'folder_relative_path': folder_relative_path,
            'folder_level': folder_level,
            'display_path': folder_relative_path + '/' + data.folder_name,
            'archived': False,
        }
        folder_node = await create_node('Folder', payload)

        # Create relation between folder and parent
        relation_payload = {
            'start_id': parent_node_id,
            'end_id': folder_node['id'],
        }
        result = await create_relation('own', relation_payload)
        api_response.result = folder_node
        return api_response.json_response()
