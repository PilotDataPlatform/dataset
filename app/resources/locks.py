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

from typing import Union

import httpx
from common import LoggerFactory

from app.config import ConfigClass
from app.resources.neo4j_helper import get_children_nodes

logger = LoggerFactory(__name__).get_logger()


async def lock_resource(resource_key: str, operation: str) -> dict:
    # operation can be either read or write
    logger.info('Lock resource:', extra={'resource_key': resource_key})
    url = ConfigClass.DATA_UTILITY_SERVICE_v2 + 'resource/lock/'
    post_json = {'resource_key': resource_key, 'operation': operation}
    async with httpx.AsyncClient() as client:
        response = await client.post(url, json=post_json)
    if response.status_code != 200:
        raise Exception('resource %s already in used' % resource_key)

    return response.json()


async def unlock_resource(resource_key: str, operation: str) -> dict:
    # operation can be either read or write
    logger.info('Unlock resource:', extra={'resource_key': resource_key})
    url = ConfigClass.DATA_UTILITY_SERVICE_v2 + 'resource/lock/'
    post_json = {'resource_key': resource_key, 'operation': operation}

    async with httpx.AsyncClient() as client:
        response = await client.request(url=url, json=post_json, method='DELETE')
    if response.status_code != 200:
        raise Exception('Error when unlock resource %s' % resource_key)

    return response.json()


# TODO somehow do the factory here?
def recursive_lock(code: str, nodes, root_path, new_name: str = None) -> Union[list, Exception]:
    """this function seems not doing anything.

    Because 'locked_node' is never set
    """
    locked_node, err = [], None

    def recur_walker(currenct_nodes, current_root_path, new_name=None):
        """recursively trace down the node tree and run the lock function."""
        for ff_object in currenct_nodes:
            # update here if the folder/file is archieved then skip
            if ff_object.get('archived', False):
                continue

            # conner case here, we DONT lock the name folder
            if ff_object.get('name') != ff_object.get('uploader'):
                pass

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get('labels'):
                next_root = current_root_path + '/' + (new_name if new_name else ff_object.get('name'))
                children_nodes = get_children_nodes(ff_object.get('global_entity_id', None))
                recur_walker(children_nodes, next_root)

        return

    # start here
    try:
        recur_walker(nodes, root_path, new_name)
    except Exception as e:
        err = e

    return locked_node, err


# TODO the issue here is how to raise the lock conflict
class lock_factory:  # pragma no cover
    def __init__(self, action: str) -> None:
        self.locked_node = []
        self.action = action
        self.lock_function = None

    def _lock_resource(self, bucket: str, path: str, lock: str = 'read') -> tuple:
        '''
        Summary:
            the function is just a wrap up for the lock_resource,
        Parameter:
            - bucket: the minio bucket
            - path: the minio path for the object
            - lock: the indication for "read"/"write" lock action
        Return:
            pair for the lock info -> tuple(<bukcet/path>, <r/w lock>)
        '''
        resource_key = '{}/{}'.format(bucket, path)
        lock_resource(resource_key, lock)
        return (resource_key, lock)

    def import_lock(self, code, source_node, current_root_path, new_name=None) -> list:
        # bucket, minio_obj_path = None, None
        # locked_node = []
        # err = None

        # if "File" in ff_object.get('labels'):
        #     minio_path = ff_object.get('location').split("//")[-1]
        #     _, bucket, minio_obj_path = tuple(minio_path.split("/", 2))
        # else:
        #     bucket = "core-"+ff_object.get('project_code')
        #     minio_obj_path = "%s/%s"%(ff_object.get('folder_relative_path'),
        #         ff_object.get('name'))

        bucket = 'core-' + source_node.get('project_code')
        minio_obj_path = source_node.get('display_path')

        # source is from project
        # source_key = "{}/{}".format(bucket, minio_obj_path)
        # lock_resource(source_key, "read")
        # self._lock_resource(bucket, minio_obj_path)
        self.locked_node.append(self._lock_resource(bucket, minio_obj_path))

        # destination is in the dataset
        # target_key = "{}/{}".format(code, minio_obj_path)
        # lock_resource(target_key, "write")
        # locked_node.append((target_key,"write"))
        self.locked_node.append(self._lock_resource(code, minio_obj_path, lock='write'))

        return

    def lock_delete(self, source_node):
        # bucket, minio_obj_path = None, None
        # if "File" in ff_object.get('labels'):
        #     minio_path = ff_object.get('location').split("//")[-1]
        #     _, bucket, minio_obj_path = tuple(minio_path.split("/", 2))
        # else:
        #     bucket = ff_object.get('dataset_code')
        #     minio_obj_path = "%s/%s"%(ff_object.get('folder_relative_path'),
        #         ff_object.get('name'))

        bucket = source_node.get('dataset_code')
        minio_obj_path = source_node.get('display_path')

        # source_key = "{}/{}".format(bucket, minio_obj_path)
        # lock_resource(source_key, "write")
        # locked_node.append((source_key, "write"))
        self.locked_node.append(self._lock_resource(bucket, minio_obj_path, lock='write'))

        return

    def lock_move_rename(self, source_node, current_root_path, new_name=None):
        # bucket, minio_obj_path = None, None

        # if "File" in ff_object.get('labels'):
        #     minio_path = ff_object.get('location').split("//")[-1]
        #     _, bucket, minio_obj_path = tuple(minio_path.split("/", 2))
        # else:
        #     bucket = ff_object.get('dataset_code')
        #     minio_obj_path = "%s/%s"%(ff_object.get('folder_relative_path'),
        #         ff_object.get('name'))

        bucket = source_node.get('dataset_code')
        minio_obj_path = source_node.get('display_path')
        target_path = current_root_path + '/' + (new_name if new_name else source_node.get('name'))

        # source_key = "{}/{}".format(bucket, minio_obj_path)
        # lock_resource(source_key, "write")
        # locked_node.append((source_key, "write"))
        self.locked_node.append(self._lock_resource(bucket, minio_obj_path, lock='write'))

        # target_key = "{}/{}".format(bucket, minio_obj_path)
        # lock_resource(target_key, "write")
        # locked_node.append((target_key,"write"))
        self.locked_node.append(self._lock_resource(bucket, target_path, lock='write'))

        return

    async def lock_publish(self, source_node):
        # bucket, minio_obj_path = None, None
        locked_node = []
        err = None

        # if "File" in ff_object.get('labels'):
        #     minio_path = ff_object.get('location').split("//")[-1]
        #     _, bucket, minio_obj_path = tuple(minio_path.split("/", 2))
        # else:
        #     bucket = ff_object.get('dataset_code')
        #     minio_obj_path = "%s/%s"%(ff_object.get('folder_relative_path'),
        #         ff_object.get('name'))

        bucket = source_node.get('dataset_code')
        minio_obj_path = source_node.get('display_path')

        try:
            source_key = '{}/{}'.format(bucket, minio_obj_path)
            await lock_resource(source_key, 'read')
            locked_node.append((source_key, 'read'))
        except Exception as e:
            err = e

        return locked_node, err


async def recursive_lock_import(dataset_code, nodes, root_path):
    """the function will recursively lock the node tree OR unlock the tree base on the parameter.

    - if lock = true then perform the lock
    - if lock = false then perform the unlock
    """

    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    async def recur_walker(currenct_nodes, current_root_path, new_name=None):
        """recursively trace down the node tree and run the lock function."""

        for ff_object in currenct_nodes:
            # update here if the folder/file is archieved then skip
            if ff_object.get('archived', False):
                continue

            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source as read operation,
            # and the target will be write operation
            if ff_object.get('display_path') != ff_object.get('uploader'):
                bucket, minio_obj_path = None, None
                if 'File' in ff_object.get('labels'):
                    minio_path = ff_object.get('location').split('//')[-1]
                    _, bucket, minio_obj_path = tuple(minio_path.split('/', 2))
                else:
                    bucket = 'core-' + ff_object.get('project_code')
                    minio_obj_path = '%s/%s' % (ff_object.get('folder_relative_path'), ff_object.get('name'))
                # source is from project
                source_key = '{}/{}'.format(bucket, minio_obj_path)
                await lock_resource(source_key, 'read')
                locked_node.append((source_key, 'read'))

                # destination is in the dataset
                target_key = '{}/{}/{}'.format(
                    dataset_code, current_root_path, new_name if new_name else ff_object.get('name')
                )
                await lock_resource(target_key, 'write')
                locked_node.append((target_key, 'write'))

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get('labels'):
                next_root = current_root_path + '/' + (new_name if new_name else ff_object.get('name'))
                children_nodes = get_children_nodes(ff_object.get('global_entity_id', None))
                await recur_walker(children_nodes, next_root)

        return

    # start here
    try:
        await recur_walker(nodes, root_path)
    except Exception as e:
        err = e

    return locked_node, err


async def recursive_lock_delete(nodes, new_name=None):

    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    async def recur_walker(currenct_nodes, new_name=None):
        """recursively trace down the node tree and run the lock function."""

        for ff_object in currenct_nodes:
            # update here if the folder/file is archieved then skip
            if ff_object.get('archived', False):
                continue

            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source as read operation,
            # and the target will be write operation
            if ff_object.get('display_path') != ff_object.get('uploader'):
                bucket, minio_obj_path = None, None
                if 'File' in ff_object.get('labels'):
                    minio_path = ff_object.get('location').split('//')[-1]
                    _, bucket, minio_obj_path = tuple(minio_path.split('/', 2))
                else:
                    bucket = ff_object.get('dataset_code')
                    minio_obj_path = '%s/%s' % (ff_object.get('folder_relative_path'), ff_object.get('name'))

                source_key = '{}/{}'.format(bucket, minio_obj_path)
                await lock_resource(source_key, 'write')
                locked_node.append((source_key, 'write'))

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get('labels'):
                # next_root = current_root_path+"/"+(new_name if new_name else ff_object.get("name"))
                children_nodes = get_children_nodes(ff_object.get('global_entity_id', None))
                await recur_walker(children_nodes)

        return

    # start here
    try:
        await recur_walker(nodes, new_name)
    except Exception as e:
        err = e

    return locked_node, err


async def recursive_lock_move_rename(nodes, root_path, new_name=None):

    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    # TODO lock

    async def recur_walker(currenct_nodes, current_root_path, new_name=None):
        """recursively trace down the node tree and run the lock function."""

        for ff_object in currenct_nodes:
            # update here if the folder/file is archieved then skip
            if ff_object.get('archived', False):
                continue

            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source as read operation,
            # and the target will be write operation
            if ff_object.get('display_path') != ff_object.get('uploader'):
                bucket, minio_obj_path = None, None
                if 'File' in ff_object.get('labels'):
                    minio_path = ff_object.get('location').split('//')[-1]
                    _, bucket, minio_obj_path = tuple(minio_path.split('/', 2))
                else:
                    bucket = ff_object.get('dataset_code')
                    minio_obj_path = '%s/%s' % (ff_object.get('folder_relative_path'), ff_object.get('name'))
                source_key = '{}/{}'.format(bucket, minio_obj_path)
                await lock_resource(source_key, 'write')
                locked_node.append((source_key, 'write'))

                target_key = '{}/{}/{}'.format(
                    bucket, current_root_path, new_name if new_name else ff_object.get('name')
                )
                await lock_resource(target_key, 'write')
                locked_node.append((target_key, 'write'))

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get('labels'):
                next_root = current_root_path + '/' + (new_name if new_name else ff_object.get('name'))
                children_nodes = get_children_nodes(ff_object.get('global_entity_id', None))
                await recur_walker(children_nodes, next_root)

        return

    # start here
    try:
        await recur_walker(nodes, root_path, new_name)
    except Exception as e:
        err = e

    return locked_node, err


async def recursive_lock_publish(nodes):

    # this is for crash recovery, if something trigger the exception
    # we will unlock the locked node only. NOT the whole tree. The example
    # case will be copy the same node, if we unlock the whole tree in exception
    # then it will affect the processing one.
    locked_node, err = [], None

    async def recur_walker(currenct_nodes):
        """recursively trace down the node tree and run the lock function."""

        for ff_object in currenct_nodes:
            # update here if the folder/file is archieved then skip
            if ff_object.get('archived', False):
                continue

            # conner case here, we DONT lock the name folder
            # for the copy we will lock the both source as read operation,
            # and the target will be write operation
            if ff_object.get('display_path') != ff_object.get('uploader'):
                bucket, minio_obj_path = None, None
                if 'File' in ff_object.get('labels'):
                    minio_path = ff_object.get('location').split('//')[-1]
                    _, bucket, minio_obj_path = tuple(minio_path.split('/', 2))
                else:
                    bucket = ff_object.get('dataset_code')
                    minio_obj_path = '%s/%s' % (ff_object.get('folder_relative_path'), ff_object.get('name'))

                source_key = '{}/{}'.format(bucket, minio_obj_path)
                await lock_resource(source_key, 'read')
                locked_node.append((source_key, 'read'))

            # open the next recursive loop if it is folder
            if 'Folder' in ff_object.get('labels'):
                # next_root = current_root_path+"/"+(new_name if new_name else ff_object.get("name"))
                children_nodes = get_children_nodes(ff_object.get('global_entity_id', None))
                await recur_walker(children_nodes)

        return

    # start here
    try:
        await recur_walker(nodes)
    except Exception as e:
        err = e

    return locked_node, err
