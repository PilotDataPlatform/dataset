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

from uuid import uuid4

import httpx
from common import GEIDClient
from common import LoggerFactory

from app.clients.metadata import MetadataClient
from app.commons.service_connection.minio_client import Minio_Client_
from app.config import ConfigClass
from app.resources.error_handler import APIException
from app.schemas.base import EAPIResponseCode

logger = LoggerFactory('api_dataset_import').get_logger()


async def create_node(payload):
    try:
        created_obj = await MetadataClient.create_object(payload)
    except Exception as e:
        raise APIException(
            error_msg=f'Error calling neo4j node API: {str(e)}', status_code=EAPIResponseCode.internal_error.value
        )
    return created_obj


async def get_node_by_geid(obj_id: str):
    try:
        return await MetadataClient.get_by_id(obj_id)
    except Exception as e:
        raise APIException(f'Error calling metadata API: {str(e)}', EAPIResponseCode.internal_error.value)


async def get_parent_node(parent_id):
    # objects = await metadata_client.get_objects_by_code(code)
    # for key, value in objects.items():
    #     if key == 'parent_id'
    # here we have to find the parent node and delete the relationship
    if parent_id:
        return await MetadataClient.get_by_id(parent_id)
    else:
        return {'parent': None, 'parent_path': None}


async def get_children_nodes(code: str, father_id: str):
    items = await MetadataClient.get_objects(code)
    children_items = []

    for item in items:
        if item['parent'] == father_id:
            children_items.append(item)

    return children_items


async def delete_relation_bw_nodes(start_id, end_id):
    # then delete the relationship between all the fils
    relation_delete_url = ConfigClass.NEO4J_SERVICE + 'relations'
    delete_params = {
        'start_id': start_id,
        'end_id': end_id,
    }
    async with httpx.AsyncClient() as client:
        response = await client.request(url=relation_delete_url, params=delete_params, method='DELETE')
    return response


async def delete_node(target_node, access_token, refresh_token):
    node_id = target_node.get('id')
    node_delete_url = ConfigClass.NEO4J_SERVICE + 'nodes/node/%s' % (node_id)
    async with httpx.AsyncClient() as client:
        client.delete(node_delete_url)

    # delete the file in minio if it is the file
    if target_node.get('type') == 'File':
        try:
            mc = Minio_Client_(access_token, refresh_token)

            # minio location is minio://http://<end_point>/bucket/user/object_path
            minio_path = target_node.get('location').split('//')[-1]
            _, bucket, obj_path = tuple(minio_path.split('/', 2))

            mc.delete_object(bucket, obj_path)
            logger.info('Minio %s/%s Delete Success' % (bucket, obj_path))

        except Exception as e:
            logger.error('error when deleting: ' + str(e))


async def create_file_node(
    dataset_code, source_file, operator, parent_id, relative_path, access_token, refresh_token, new_name=None
):
    # fecth the geid from common service
    id_ = uuid4()
    file_name = new_name if new_name else source_file.get('name')
    # generate minio object path
    fuf_path = relative_path + '/' + file_name

    minio_http = ('https://' if ConfigClass.MINIO_HTTPS else 'http://') + ConfigClass.MINIO_ENDPOINT
    location = 'minio://%s/%s/%s' % (minio_http, dataset_code, fuf_path)

    # then copy the node under the dataset
    file_attribute = {
        'file_size': source_file.get('file_size', -1),  # if the folder then it is -1
        'operator': operator,
        'name': file_name,
        'global_entity_id': id_,
        'location': location,
        'dataset_code': dataset_code,
        'display_path': fuf_path,
    }

    new_file_node, new_relation = await create_node_with_parent('File', file_attribute, parent_id)

    # make minio copy
    try:
        mc = Minio_Client_(access_token, refresh_token)
        # minio location is minio://http://<end_point>/bucket/user/object_path
        minio_path = source_file.get('storage').get('location_uri').split('//')[-1]
        _, bucket, obj_path = tuple(minio_path.split('/', 2))

        mc.copy_object(dataset_code, fuf_path, bucket, obj_path)
        logger.info('Minio Copy %s/%s Success' % (dataset_code, fuf_path))
    except Exception as e:
        logger.error('error when uploading: ' + str(e))

    return {}, {}


async def create_folder_node(dataset_code, source_folder, operator, parent_node, relative_path, new_name=None):
    # fecth the geid from common service
    geid = GEIDClient().get_GEID()
    folder_name = new_name if new_name else source_folder.get('name')

    # then copy the node under the dataset
    folder_attribute = {
        'create_by': operator,
        'name': folder_name,
        'global_entity_id': geid,
        'folder_relative_path': relative_path,
        'folder_level': parent_node.get('folder_level', -1) + 1,
        'dataset_code': dataset_code,
        'display_path': relative_path + '/' + folder_name,
    }
    folder_node, relation = await create_node_with_parent('Folder', folder_attribute, parent_node.get('id'))

    return folder_node, relation


# this function will help to create a target node
# and connect to parent with "own" relationship
async def create_node_with_parent(node_label, node_property, parent_id):
    # create the node with following attribute
    # - global_entity_id: unique identifier
    # - create_by: who import the files
    # - size: file size in byte (if it is a folder then size will be -1)
    # - create_time: neo4j timeobject (API will create but not passed in api)
    # - location: indicate the minio location as minio://http://<domain>/object
    create_node_url = ConfigClass.NEO4J_SERVICE + 'nodes/' + node_label
    async with httpx.AsyncClient() as client:
        response = await client.post(create_node_url, json=node_property)
    new_node = response.json()[0]

    # now create the relationship
    # the parent can be two possible: 1.dataset 2.folder under it
    create_node_url = ConfigClass.NEO4J_SERVICE + 'relations/own'
    async with httpx.AsyncClient() as client:
        new_relation = await client.post(create_node_url, json={'start_id': parent_id, 'end_id': new_node.get('id')})

    return new_node, new_relation
