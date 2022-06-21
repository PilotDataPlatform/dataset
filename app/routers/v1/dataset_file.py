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

import copy
import time
from typing import Optional

import httpx
from common import LoggerFactory
from fastapi import APIRouter
from fastapi import BackgroundTasks
from fastapi import Cookie
from fastapi import Depends
from fastapi import Header
from fastapi_utils import cbv

from app.clients import MetadataClient
from app.clients import ProjectClient
from app.config import ConfigClass
from app.core.db import get_db_session
from app.models.dataset import Dataset
from app.resources.error_handler import catch_internal
from app.resources.locks import recursive_lock_delete
from app.resources.locks import recursive_lock_import
from app.resources.locks import recursive_lock_move_rename
from app.resources.locks import unlock_resource
from app.resources.utils import create_file_node
from app.resources.utils import create_folder_node
from app.resources.utils import delete_node
from app.resources.utils import get_children_nodes
from app.resources.utils import get_node_by_geid
from app.resources.utils import get_parent_node
from app.schemas.base import APIResponse
from app.schemas.base import EAPIResponseCode
from app.schemas.import_data import DatasetFileDelete
from app.schemas.import_data import DatasetFileMove
from app.schemas.import_data import DatasetFileRename
from app.schemas.import_data import ImportDataPost
from app.services.dataset import SrvDatasetMgr
from app.services.import_data import SrvDatasetFileMgr

router = APIRouter()

_API_TAG = 'V1 DATASET'
_API_NAMESPACE = 'api_dataset'

HEADERS = {'accept': 'application/json', 'Content-Type': 'application/json'}


@cbv.cbv(router)
class APIImportData:
    """API to import data from project to dataset."""

    def __init__(self):
        self.__logger = LoggerFactory('api_dataset_import').get_logger()
        self.file_act_notifier = SrvDatasetFileMgr()

    @router.put(
        '/dataset/{dataset_id}/files',
        tags=[_API_TAG],  # , response_model=PreUploadResponse,
        summary='API will recieve the file list from a project and \n\
                 Copy them under the dataset.',
    )
    @catch_internal(_API_NAMESPACE)
    async def import_dataset(
        self,
        dataset_id,
        request_payload: ImportDataPost,
        background_tasks: BackgroundTasks,
        sessionId: Optional[str] = Cookie(None),
        Authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
        db=Depends(get_db_session),
    ):
        import_list = request_payload.source_list
        oper = request_payload.operator
        project_id = request_payload.project_geid
        session_id = sessionId
        minio_access_token = Authorization
        minio_refresh_token = refresh_token
        api_response = APIResponse()

        # if dataset not found return 404
        srv_dataset = SrvDatasetMgr()
        dataset = await srv_dataset.get_bygeid(db, dataset_id)
        if dataset is None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'Invalid geid for dataset'
            return api_response.json_response()
        # here we only allow user to import from one project
        # if user try to import from another project block the action
        if dataset.project_id and str(dataset.project_id) != project_id:
            api_response.code = EAPIResponseCode.forbidden
            api_response.error_msg = 'Cannot import from another project'
            return api_response.json_response()

        # check if file is from source project or exist
        project = await ProjectClient.get_by_id(project_id)
        import_list, wrong_file = await self.validate_files_folders(import_list, project['code'], items_type='project')
        for file in import_list:
            file['parent'] = None
            file['parent_path'] = None
        # and check if file has been under the dataset
        duplicate, import_list = await self.remove_duplicate_file(import_list, dataset.code)
        # fomutate the result
        api_response.result = {'processing': import_list, 'ignored': wrong_file + duplicate}
        # start the background job to copy the file one by one

        if len(import_list) > 0:
            background_tasks.add_task(
                self.copy_files_worker,
                db,
                import_list,
                dataset,
                oper,
                project_id,
                session_id,
                minio_access_token,
                minio_refresh_token,
            )

        return api_response.json_response()

    @router.delete(
        '/dataset/{dataset_id}/files',
        tags=[_API_TAG],  # , response_model=PreUploadResponse,
        summary='API will delete file by geid from list',
    )
    @catch_internal(_API_NAMESPACE)
    async def delete_files(
        self,
        dataset_id,
        request_payload: DatasetFileDelete,
        background_tasks: BackgroundTasks,
        sessionId: Optional[str] = Cookie(None),
        Authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
        db=Depends(get_db_session),
    ):
        api_response = APIResponse()
        session_id = sessionId
        minio_access_token = Authorization
        minio_refresh_token = refresh_token

        # validate the dataset if exists
        srv_dataset = SrvDatasetMgr()
        dataset_obj = await srv_dataset.get_bygeid(db, dataset_id)

        if dataset_obj is None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'Invalid geid for dataset'
            return api_response.json_response()

        # validate the file IS from the dataset
        delete_list = request_payload.source_list
        delete_list, wrong_file = await self.validate_files_folders(delete_list, dataset_obj.code)
        # fomutate the result
        api_response.result = {'processing': delete_list, 'ignored': wrong_file}

        # loop over the list and delete the file one by one
        if len(delete_list) > 0:
            background_tasks.add_task(
                self.delete_files_work,
                db,
                delete_list,
                dataset_obj,
                request_payload.operator,
                session_id,
                minio_access_token,
                minio_refresh_token,
            )

        return api_response.json_response()

    @router.get(
        '/dataset/{dataset_geid}/files',
        tags=[_API_TAG],  # , response_model=PreUploadResponse,
        summary='API will list files under the target dataset',
    )
    @catch_internal(_API_NAMESPACE)
    async def list_files(
        self,
        dataset_geid,
        page: int = 0,
        page_size: int = 25,
        order_by: str = 'createTime',
        order_type: str = 'desc',
        query: str = '{}',
        folder_geid: str = None,
        db=Depends(get_db_session),
    ):
        """the api will list the file/folder at level 1 by default.

        If folder_geid is not None, then it will treat the folder_geid as root and find the relative level 1 file/folder
        """
        api_response = APIResponse()
        ret_routing = []
        # validate the dataset if exists
        srv_dataset = SrvDatasetMgr()
        dataset = await srv_dataset.get_bygeid(db, dataset_geid)

        if dataset is None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'Invalid geid for dataset'
            return api_response.json_response()

        # find the root folder node
        root_geid = folder_geid if folder_geid else None

        # then get the first level nodes
        file_folder_nodes = await get_children_nodes(dataset.code, root_geid)

        # then get the routing this will return as parent level
        # like admin->folder1->file1 in UI
        if file_folder_nodes:
            parent_path = file_folder_nodes[0]['parent_path']
            ret_routing = parent_path.split('.') if parent_path else []

        total = len(file_folder_nodes)
        ret = {
            'data': file_folder_nodes,
            'route': ret_routing,
        }
        api_response.result = ret
        api_response.total = total
        return api_response.json_response()

    @router.post(
        '/dataset/{dataset_id}/files',
        tags=[_API_TAG],  # , response_model=PreUploadResponse,
        summary='API will move files within the dataset',
    )
    @catch_internal(_API_NAMESPACE)
    async def move_files(
        self,
        dataset_id,
        request_payload: DatasetFileMove,
        background_tasks: BackgroundTasks,
        sessionId: Optional[str] = Cookie(None),
        Authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
        db=Depends(get_db_session),
    ):
        api_response = APIResponse()
        session_id = sessionId
        minio_access_token = Authorization
        minio_refresh_token = refresh_token
        # validate the dataset if exists
        srv_dataset = SrvDatasetMgr()
        dataset = await srv_dataset.get_bygeid(db, dataset_id)

        if dataset is None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'Invalid geid for dataset'
            return api_response.json_response()

        # first get the target -> the target must be a folder or dataset root
        if request_payload.target_geid == dataset_id:
            target_folder = {
                'id': None,
                'name': None,
                'parent': None,
                'parent_path': None,
                'code': dataset.code,
            }
        else:
            target_folder = await get_node_by_geid(request_payload.target_geid)
            if not target_folder:
                api_response.code = EAPIResponseCode.not_found
                api_response.error_msg = 'The target folder does not exist'
                return api_response.json_response()
            # also the folder MUST under the same dataset
            if target_folder.get('container_code') != dataset.code:
                api_response.code = EAPIResponseCode.not_found
                api_response.error_msg = 'The target folder does not exist'
                return api_response.json_response()

        # validate the file if it is under the dataset
        move_list = request_payload.source_list
        move_list, wrong_file = await self.validate_files_folders(move_list, dataset.code)
        for file in move_list:
            if request_payload.target_geid == dataset_id:
                file['parent'] = None
                file['parent_path'] = None
            else:
                file['parent'] = target_folder['id']
                file['parent_path'] = (target_folder['name'] if target_folder['name'] else '') + (
                    '.' + file['parent_path'] if file['parent_path'] else ''
                )
        duplicate, move_list = await self.remove_duplicate_file(move_list, dataset.code)
        # fomutate the result
        api_response.result = {'processing': move_list, 'ignored': wrong_file + duplicate}

        # start the background job to copy the file one by one
        if len(move_list) > 0:
            background_tasks.add_task(
                self.move_file_worker,
                db,
                move_list,
                dataset,
                request_payload.operator,
                target_folder,
                session_id,
                minio_access_token,
                minio_refresh_token,
            )

        return api_response.json_response()

    @router.post(
        '/dataset/{dataset_id}/files/{target_file}',
        tags=[_API_TAG],
        summary='API will update files within the dataset',
    )
    @catch_internal(_API_NAMESPACE)
    async def rename_file(
        self,
        dataset_id,
        target_file,
        request_payload: DatasetFileRename,
        background_tasks: BackgroundTasks,
        sessionId: Optional[str] = Cookie(None),
        Authorization: Optional[str] = Header(None),
        refresh_token: Optional[str] = Header(None),
        db=Depends(get_db_session),
    ):
        api_response = APIResponse()
        session_id = sessionId
        new_name = request_payload.new_name
        minio_access_token = Authorization
        duplicate = []

        # validate the dataset if exists
        srv_dataset = SrvDatasetMgr()
        dataset = await srv_dataset.get_bygeid(db, dataset_id)

        if dataset is None:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'Invalid geid for dataset'
            return api_response.json_response()

        # TODO filename check? regx

        # validate the file IS from the dataset
        # rename to same name will be blocked

        rename_list, wrong_file = await self.validate_files_folders([target_file], dataset.code)
        if len(rename_list) > 0:
            future_list = copy.deepcopy(rename_list)
            future_list[0]['name'] = new_name

            duplicate, _ = await self.remove_duplicate_file(future_list, dataset.code)
            # fomutate the result
            if len(duplicate) > 0:
                duplicate = rename_list
                rename_list = []
        api_response.result = {'processing': rename_list, 'ignored': wrong_file + duplicate}

        # loop over the list and delete the file one by one
        if len(rename_list) > 0:
            background_tasks.add_task(
                self.rename_file_worker,
                rename_list[0],
                new_name,
                dataset,
                request_payload.operator,
                session_id,
                minio_access_token,
                refresh_token,
            )

        return api_response.json_response()

    ##########################################################################################################
    #
    # the function will walk throught the list and validate
    # if the node is from correct root geid. for example:
    # - PUT: the imported files must from target project
    # - POST: the moved files must from correct dataset
    # - DELETE: the deleted file must from correct dataset
    #
    ################################################################
    # function will return two list:
    # - passed_file is the validated file
    # - not_passed_file is not under the target node
    async def validate_files_folders(self, file_id_list, code, items_type='dataset'):
        passed_file = []
        not_passed_file = []
        duplicate_in_batch_dict = {}
        obj_list = await MetadataClient.get_objects(code, items_type=items_type)
        # creates dict where key is obj.id and value is root_objects index
        object_ids = {obj['id']: obj_list.index(obj) for obj in obj_list}
        # this is to keep track the object in passed_file array
        # and in the duplicate_in_batch_dict it will be {"geid": array_index}
        # and this can help to trace back when duplicate occur
        array_index = 0

        for file_id in file_id_list:
            # get index from dict in root_objects
            root_object_index = object_ids.get(file_id, None)

            # if there is no connect then the node is not correct
            # else it is correct
            if root_object_index is None:
                not_passed_file.append({'id': file_id, 'feedback': 'unauthorized'})

            else:
                current_node = obj_list[root_object_index]
                exist_index = duplicate_in_batch_dict.get(current_node.get('name'), None)
                # if we have process the file with same name in the same BATCH
                # we will try to update name for ALL duplicate file into display_path
                if exist_index is not None:
                    current_node.update(
                        {
                            'feedback': 'duplicate in same batch, update the name',
                            'name': current_node.get('parent_path').replace('.', '_') + current_node.get_name(),
                        }
                    )

                    # if the first node is not updated then use the index to trace back
                    if exist_index != -1:
                        passed_file[exist_index].update(
                            {
                                'name': passed_file[exist_index].get('parent_path').replace('.', '_')
                                + current_node.get_name(),
                                'feedback': 'duplicate in same batch, update the name',
                            }
                        )

                        # and mark the first one
                        first_geid = passed_file[exist_index].get('id')
                        duplicate_in_batch_dict.update({first_geid: -1})

                # else we just record the file for next checking
                else:
                    current_node.update({'feedback': 'exist'})
                    duplicate_in_batch_dict.update({current_node.get('name'): array_index})

                passed_file.append(current_node)
                array_index += 1
        return passed_file, not_passed_file

    # the function will check if the file IS from core
    # and will block other files(greenroom, trashfile...)
    def check_core_file(self, ff_list):
        core_file = []
        not_core_file = []
        for current_node in ff_list:
            if ConfigClass.CORE_ZONE_LABEL not in current_node.get('labels', []):
                current_node.update({'feedback': 'not core file'})
                not_core_file.append(current_node)
            else:
                core_file.append(current_node)

        return core_file, not_core_file

    # the function will reuse the <validate_files_folders> to check
    # if the file already exist directly under the root node
    # return True if duplicate else false
    async def remove_duplicate_file(self, files_list, dataset_code):
        dataset_objects = await MetadataClient.get_objects(dataset_code)
        duplic_file = []
        not_duplic_file = []
        name_parent_dict = {obj['name']: obj['parent_path'] for obj in dataset_objects}
        for file in files_list:
            same_name = name_parent_dict.get(file.get('name'), 'not_found')
            if same_name == 'not_found':
                not_duplic_file.append(file)
            elif same_name == file.get('parent_path'):
                file.update({'feedback': 'duplicate or unauthorized'})
                duplic_file.append(file)
            else:
                not_duplic_file.append(file)

        return duplic_file, not_duplic_file

    # TODO make it into the helper function
    # match (n)-[r:own*]->(f) where n.global_entity_id="9ff8382d-f476-4cdf-a357-66c4babf8320-1626104650" delete
    # FOREACH(r), f
    async def send_notification(
        self, session_id, source_list, action, status, dataset_geid, operator, task_id, payload=None
    ):
        if not payload:
            payload = {}

        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_FILE_NOTIFICATION',
            'payload': {
                'session_id': session_id,
                'task_id': task_id,
                'source': source_list,
                'action': action,
                'status': status,  # INIT/RUNNING/FINISH/ERROR
                'dataset': dataset_geid,
                'operator': operator,
                'payload': payload,
                'update_timestamp': time.time(),
            },
            'binary': True,
            'queue': 'socketio',
            'routing_key': 'socketio',
            'exchange': {'name': 'socketio', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('send_notification() {}: {}'.format(res.status_code, res.text))

        return res

    async def create_job_status(
        self, session_id, source_file, action, status, dataset, operator, task_id, payload=None
    ):
        if not payload:
            payload = {}
        # first send the notification
        dataset_geid = str(dataset.id)
        dataset_code = dataset.code
        await self.send_notification(session_id, source_file, action, status, dataset_geid, operator, task_id)
        # also save to redis for display
        source_geid = source_file.get('id')
        job_id = action + '-' + source_geid + '-' + str(int(time.time()))
        task_url = ConfigClass.DATA_UTILITY_SERVICE + 'tasks/'
        post_json = {
            'session_id': session_id,
            'label': 'Dataset',
            'source': source_geid,
            'task_id': task_id,
            'job_id': job_id,
            'action': action,
            'code': dataset_code,
            'target_status': status,
            'operator': operator,
            'payload': source_file,
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(task_url, json=post_json)
        if res.status_code != 200:
            raise Exception('save redis error {}: {}'.format(res.status_code, res.text))

        return {source_geid: job_id}

    async def update_job_status(
        self, session_id, source_file, action, status, dataset, operator, task_id, job_id, payload=None
    ):
        if not payload:
            payload = {}
        # first send the notification
        dataset_geid = str(dataset.id)
        await self.send_notification(session_id, source_file, action, status, dataset_geid, operator, task_id, payload)

        # also save to redis for display
        task_url = ConfigClass.DATA_UTILITY_SERVICE + 'tasks/'
        post_json = {
            'session_id': session_id,
            'label': 'Dataset',
            'task_id': task_id,
            'job_id': job_id,
            'status': status,
            'add_payload': payload,
        }
        async with httpx.AsyncClient() as client:
            res = await client.put(task_url, json=post_json)
        if res.status_code != 200:
            raise Exception('save redis error {}: {}'.format(res.status_code, res.text))

        return res

    # the function will call the create_job_status to initialize
    # the job status in the redis and prepare for update in copy/delete
    # the function will return the job object include:
    #   - session id: fetch from frontend
    #   - task id: random generate for batch operation
    #   - action: the file action name
    #   - job id mapping: dictionary for tracking EACH file progress
    async def initialize_file_jobs(self, session_id, action, batch_list, dataset_obj, oper):
        # use the dictionary to keep track the file action with
        session_id = 'local_test' if not session_id else session_id
        # action = "dataset_file_import"
        task_id = action + '-' + str(int(time.time()))
        job_tracker = {'session_id': session_id, 'task_id': task_id, 'action': action, 'job_id': {}}
        for file_object in batch_list:
            tracker = await self.create_job_status(session_id, file_object, action, 'INIT', dataset_obj, oper, task_id)
            job_tracker['job_id'].update(tracker)

        return job_tracker

    ###########################################################################################

    async def recursive_copy(
        self,
        current_nodes,
        dataset,
        oper,
        current_root_path,
        parent_node,
        access_token,
        refresh_token,
        job_tracker=None,
        new_name=None,
    ):
        num_of_files = 0
        total_file_size = 0
        # this variable DOESNOT contain the child nodes
        new_lv1_nodes = []
        # copy the files under the project neo4j node to dataset node
        for ff_object in current_nodes:
            ff_geid = ff_object.get('id')
            new_node = None

            # update here if the folder/file is archieved then skip
            if ff_object.get('archived', False):
                continue

            # here ONLY the first level file/folder will trigger the notification&job status
            if job_tracker:
                job_id = job_tracker['job_id'].get(ff_geid)
                await self.update_job_status(
                    job_tracker['session_id'],
                    ff_object,
                    job_tracker['action'],
                    'RUNNING',
                    dataset,
                    oper,
                    job_tracker['task_id'],
                    job_id,
                )

            ################################################################################################
            # recursive logic below

            if ff_object.get('type').lower() == 'file':
                # TODO simplify here
                minio_path = ff_object.get('storage').get('location_uri').split('//')[-1]
                _, bucket, old_path = tuple(minio_path.split('/', 2))
                if isinstance(parent_node, Dataset):
                    current_root_path = None
                else:
                    current_root_path = parent_node.get('parent_path')

                # create the copied node
                new_node, _ = await create_file_node(
                    dataset,
                    ff_object,
                    oper,
                    parent_node,
                    current_root_path,
                    access_token,
                    refresh_token,
                    new_name,
                )
                # update for number and size
                num_of_files += 1
                total_file_size += ff_object.get('size', 0)
                new_lv1_nodes.append(new_node)

            # else it is folder will trigger the recursive
            elif ff_object.get('type').lower() == 'folder':
                # first create the folder
                new_node, _ = await create_folder_node(
                    dataset.code, ff_object, oper, parent_node, current_root_path, new_name
                )
                new_lv1_nodes.append(new_node)

                # seconds recursively go throught the folder/subfolder by same proccess
                # also if we want the folder to be renamed if new_name is not None
                if new_name:
                    filename = new_name
                else:
                    filename = ff_object.get('name')

                if current_root_path == ConfigClass.DATASET_FILE_FOLDER:
                    next_root_path = filename
                else:
                    next_root_path = current_root_path + '.' + filename
                children_nodes = await get_children_nodes(
                    ff_object['container_code'], ff_object.get('id', None), ff_object['container_type']
                )
                num_of_child_files, num_of_child_size, _ = await self.recursive_copy(
                    children_nodes, dataset, oper, next_root_path, new_node, access_token, refresh_token
                )

                # append the log together
                num_of_files += num_of_child_files
                total_file_size += num_of_child_size
            ##########################################################################################################

            # here after all use the geid to mark the job done for either first level folder/file
            # if the geid is not in the tracker then it is child level ff. ignore them
            if job_tracker:
                job_id = job_tracker['job_id'].get(ff_geid)
                await self.update_job_status(
                    job_tracker['session_id'],
                    ff_object,
                    job_tracker['action'],
                    'FINISH',
                    dataset,
                    oper,
                    job_tracker['task_id'],
                    job_id,
                    payload=new_node,
                )

        return num_of_files, total_file_size, new_lv1_nodes

    async def recursive_delete(
        self, current_nodes, dataset, oper, parent_node, access_token, refresh_token, job_tracker=None
    ):
        num_of_files = 0
        total_file_size = 0
        # copy the files under the project neo4j node to dataset node
        for ff_object in current_nodes:
            ff_geid = ff_object.get('id')

            # update here if the folder/file is archieved then skip
            if ff_object.get('archived', False):
                continue

            # here ONLY the first level file/folder will trigger the notification&job status
            if job_tracker:
                job_id = job_tracker['job_id'].get(ff_geid)
                await self.update_job_status(
                    job_tracker['session_id'],
                    ff_object,
                    job_tracker['action'],
                    'RUNNING',
                    dataset,
                    oper,
                    job_tracker['task_id'],
                    job_id,
                )

            ################################################################################################
            if ff_object.get('type').lower() == 'file':

                # lock the resource
                # minio location is minio://http://<end_point>/bucket/user/object_path
                minio_path = ff_object.get('storage').get('location_uri').split('//')[-1]
                _, bucket, obj_path = tuple(minio_path.split('/', 2))

                # for file we can just disconnect and delete
                # TODO MOVE OUTSIDE <=============================================================
                await MetadataClient.delete_object(ff_object.get('id'))
                await delete_node(ff_object, access_token, refresh_token)

                # update for number and size
                num_of_files += 1
                total_file_size += ff_object.get('size', 0)

            # else it is folder will trigger the recursive
            elif ff_object.get('type').lower() == 'folder':

                # for folder, we have to disconnect all child node then
                # disconnect it from parent
                children_nodes = await get_children_nodes(ff_object.get('container_code'), ff_object.get('id'))
                num_of_child_files, num_of_child_size = await self.recursive_delete(
                    children_nodes, dataset, oper, ff_object, access_token, refresh_token
                )

                # after the child has been deleted then we disconnect current node
                await MetadataClient.delete_object(ff_object.get('id'))
                await delete_node(ff_object, access_token, refresh_token)

                # append the log together
                num_of_files += num_of_child_files
                total_file_size += num_of_child_size
            ##########################################################################################

            # here after all use the geid to mark the job done for either first level folder/file
            # if the geid is not in the tracker then it is child level ff. ignore them
            if job_tracker:
                job_id = job_tracker['job_id'].get(ff_geid)
                await self.update_job_status(
                    job_tracker['session_id'],
                    ff_object,
                    job_tracker['action'],
                    'FINISH',
                    dataset,
                    oper,
                    job_tracker['task_id'],
                    job_id,
                )

        return num_of_files, total_file_size

    ######################################################################################################

    async def copy_files_worker(
        self, db, import_list, dataset_obj, oper, source_project_geid, session_id, access_token, refresh_token
    ):
        # TODO:
        # replace source_project_geid with the result from that query already requested.
        # This avoid an unnecessary request.
        action = 'dataset_file_import'
        job_tracker = await self.initialize_file_jobs(session_id, action, import_list, dataset_obj, oper)
        root_path = ConfigClass.DATASET_FILE_FOLDER
        try:
            # mark the source tree as read, destination as write
            locked_node, err = await recursive_lock_import(dataset_obj.code, import_list, root_path)
            if err:
                raise err

            # recursively go throught the folder level by level
            num_of_files, total_file_size, _ = await self.recursive_copy(
                import_list, dataset_obj, oper, root_path, {}, access_token, refresh_token, job_tracker
            )

            # after all update the file number/total size/project geid
            srv_dataset = SrvDatasetMgr()
            update_attribute = {
                'total_files': dataset_obj.total_files + num_of_files,
                'size': dataset_obj.size + total_file_size,
                'project_id': source_project_geid,
            }
            await srv_dataset.update(db, dataset_obj, update_attribute)
            # also update the log
            dataset_geid = str(dataset_obj.id)
            source_project = await ProjectClient.get_by_id(source_project_geid)
            import_logs = [source_project.get('code') + '/' + (x.get('parent_path') or '') for x in import_list]
            project = source_project.get('name', '')
            project_code = source_project.get('code', '')
            await self.file_act_notifier.on_import_event(dataset_geid, oper, import_logs, project, project_code)
        except Exception as e:
            # here batch deny the operation
            error_message = {'err_message': str(e)}
            # loop over all existing job and send error
            for ff_object in import_list:
                job_id = job_tracker['job_id'].get(ff_object.get('id'))
                await self.update_job_status(
                    job_tracker['session_id'],
                    ff_object,
                    job_tracker['action'],
                    'CANCELLED',
                    dataset_obj,
                    oper,
                    job_tracker['task_id'],
                    job_id,
                    payload=error_message,
                )
        finally:
            # unlock the nodes if we got blocked
            for resource_key, operation in locked_node:
                await unlock_resource(resource_key, operation)

        return

    async def move_file_worker(
        self,
        db,
        move_list,
        dataset_obj,
        oper,
        target_folder,
        session_id,
        access_token,
        refresh_token,
    ):
        dataset_geid = str(dataset_obj.id)
        action = 'dataset_file_move'
        job_tracker = await self.initialize_file_jobs(session_id, action, move_list, dataset_obj, oper)
        try:
            # then we mark both source node tree and target nodes as write
            locked_node, err = await recursive_lock_move_rename(move_list, ConfigClass.DATASET_FILE_FOLDER)
            if err:
                raise err

            # but note here the job tracker is not pass into the function
            # we only let the delete to state the finish
            if not target_folder.get('id'):
                target_folder = {}
                target_folder_name = ConfigClass.DATASET_FILE_FOLDER
            else:
                target_folder_name = target_folder['name']
            _, _, _ = await self.recursive_copy(
                move_list, dataset_obj, oper, target_folder_name, target_folder, access_token, refresh_token
            )

            # delete the old one
            await self.recursive_delete(
                move_list, dataset_obj, oper, target_folder, access_token, refresh_token, job_tracker=job_tracker
            )

            # generate the activity log
            dff = ConfigClass.DATASET_FILE_FOLDER
            for ff_geid in move_list:
                if ff_geid.get('type').lower() == 'file':
                    # minio location is minio://http://<end_point>/bucket/user/object_path
                    minio_path = ff_geid.get('storage').get('location_uri').split('//')[-1]
                    _, bucket, old_path = tuple(minio_path.split('/', 2))
                    old_path = old_path.replace(dff, '', 1)

                    # format new path if the temp is None then the path is from
                    if target_folder.get('parent_path'):
                        parent_path = target_folder.get('parent_path').replace('.', '/')
                        parent_path += '/' + target_folder_name
                    else:
                        parent_path = target_folder_name
                    new_path = '/' + parent_path + '/' + ff_geid.get('name')
                # else we mark the folder as deleted
                else:
                    # update the relative path by remove `data` at begining
                    old_path = ff_geid.get('parent_path', '.').replace('.', '/')

                    new_path = target_folder.get('parent_path', '.').replace('.', '/') + ff_geid.get('name')

                # send to the es for logging
                await self.file_act_notifier.on_move_event(dataset_geid, oper, old_path, new_path)

        except Exception as e:
            # here batch deny the operation
            error_message = {'err_message': str(e)}
            # loop over all existing job and send error
            for ff_object in move_list:
                job_id = job_tracker['job_id'].get(ff_object.get('id'))
                await self.update_job_status(
                    job_tracker['session_id'],
                    ff_object,
                    job_tracker['action'],
                    'CANCELLED',
                    dataset_obj,
                    oper,
                    job_tracker['task_id'],
                    job_id,
                    payload=error_message,
                )
        finally:
            # unlock the nodes if we got blocked
            for resource_key, operation in locked_node:
                await unlock_resource(resource_key, operation)

        return

    async def delete_files_work(self, db, delete_list, dataset_obj, oper, session_id, access_token, refresh_token):
        deleted_files = []  # for logging action
        action = 'dataset_file_delete'
        job_tracker = await self.initialize_file_jobs(session_id, action, delete_list, dataset_obj, oper)
        try:
            # mark both source&destination as write lock
            locked_node, err = await recursive_lock_delete(delete_list)
            if err:
                raise err

            num_of_files, total_file_size = await self.recursive_delete(
                delete_list, dataset_obj, oper, dataset_obj, access_token, refresh_token, job_tracker
            )

            # TODO try to embed with the notification&job status
            # generate log path
            for ff_geid in delete_list:
                if ff_geid.get('type').lower() == 'file':
                    # minio location is minio://http://<end_point>/bucket/user/object_path
                    minio_path = ff_geid.get('storage').get('location_uri').split('//')[-1]
                    _, bucket, obj_path = tuple(minio_path.split('/', 2))

                    # update metadata
                    dff = ConfigClass.DATASET_FILE_FOLDER + '/'
                    obj_path = obj_path[: len(dff)].replace(dff, '') + obj_path[len(dff) :]
                    deleted_files.append(obj_path)

                # else we mark the folder as deleted
                else:
                    # update the relative path by remove `data` at begining
                    dff = ConfigClass.DATASET_FILE_FOLDER
                    temp = ff_geid.get('parent_path')

                    # consider the root level delete will need to remove the data path at begining
                    frp = ''
                    if dff != temp:
                        dff = dff + '/'
                        frp = temp.replace(dff, '', 1)
                    deleted_files.append(frp + ff_geid.get('name'))

            # after all update the file number/total size/project geid
            srv_dataset = SrvDatasetMgr()
            update_attribute = {
                'total_files': dataset_obj.total_files - num_of_files,
                'size': dataset_obj.size - total_file_size,
            }
            await srv_dataset.update(db, dataset_obj, update_attribute)

            # also update the message to service queue
            dataset_geid = str(dataset_obj.id)
            await self.file_act_notifier.on_delete_event(dataset_geid, oper, deleted_files)

        except Exception as e:
            # here batch deny the operation
            error_message = {'err_message': str(e)}
            # loop over all existing job and send error
            for ff_object in delete_list:
                job_id = job_tracker['job_id'].get(ff_object.get('id'))
                await self.update_job_status(
                    job_tracker['session_id'],
                    ff_object,
                    job_tracker['action'],
                    'CANCELLED',
                    dataset_obj,
                    oper,
                    job_tracker['task_id'],
                    job_id,
                    payload=error_message,
                )
        finally:
            # unlock the nodes if we got blocked
            for resource_key, operation in locked_node:
                await unlock_resource(resource_key, operation)

        return

    # the rename worker will reuse the recursive_copy&recursive_delete
    # with only one file. the old_file is the node object and update
    # attribute to new name
    async def rename_file_worker(self, old_file, new_name, dataset, oper, session_id, access_token, refresh_token):
        action = 'dataset_file_rename'
        job_tracker = await self.initialize_file_jobs(session_id, action, [old_file], dataset, oper)
        # since the renanme will be just one file set to the running now
        job_id = job_tracker['job_id'].get(old_file.get('id'))
        await self.update_job_status(
            job_tracker['session_id'],
            old_file,
            job_tracker['action'],
            'RUNNING',
            dataset,
            oper,
            job_tracker['task_id'],
            job_id,
        )

        # minio move update the arribute
        # find the parent node for path
        parent_node = await get_parent_node(old_file.get('parent'))
        parent_path = parent_node.get('parent_path')
        parent_path = parent_path + '/' + parent_node.get('name') if parent_path else ConfigClass.DATASET_FILE_FOLDER

        try:
            # then we mark both source node tree and target nodes as write
            locked_node, err = await recursive_lock_move_rename([old_file], parent_path, new_name=new_name)
            if err:
                raise err
            # same here the job tracker is not pass into the function
            # we only let the delete to state the finish
            _, _, new_nodes = await self.recursive_copy(
                [old_file], dataset, oper, parent_path, parent_node, access_token, refresh_token, new_name=new_name
            )

            # delete the old one
            await self.recursive_delete([old_file], dataset, oper, parent_node, access_token, refresh_token)

            # after deletion set the status using new node
            await self.update_job_status(
                job_tracker['session_id'],
                old_file,
                job_tracker['action'],
                'FINISH',
                dataset,
                oper,
                job_tracker['task_id'],
                job_id,
                new_nodes[0],
            )

            # update es & log
            dataset_geid = str(dataset.id)
            old_file_name = old_file.get('name')
            # remove the /data in begining ONLY once
            frp = ''
            if ConfigClass.DATASET_FILE_FOLDER != parent_path:
                frp = parent_path.replace(ConfigClass.DATASET_FILE_FOLDER + '/', '', 1) + '/'
            await self.file_act_notifier.on_rename_event(dataset_geid, oper, frp + old_file_name, frp + new_name)

        except Exception as e:
            # send the cancelled
            error_message = {'err_message': str(e)}
            await self.update_job_status(
                job_tracker['session_id'],
                old_file,
                job_tracker['action'],
                'CANCELLED',
                dataset,
                oper,
                job_tracker['task_id'],
                job_id,
                payload=error_message,
            )
        finally:
            # unlock the nodes if we got blocked
            for resource_key, operation in locked_node:
                await unlock_resource(resource_key, operation)

        return
