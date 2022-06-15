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
import os

from common import LoggerFactory

from app.clients import MetadataClient
from app.commons.service_connection.minio_client import Minio_Client_
from app.config import ConfigClass
from app.resources.error_handler import APIException
from app.schemas.base import EAPIResponseCode

logger = LoggerFactory('api_dataset_import').get_logger()


async def get_files_recursive(folder_geid, items, all_files=None):
    if all_files is None:
        all_files = []

    items = await get_children_nodes(folder_geid)
    """get all files from dataset."""
    while items is not []:
        for item in items:
            if item['type'] == 'file':
                all_files.append(item)
            else:
                items = await get_files_recursive(item[id])
    return all_files


async def get_related_nodes(dataset_id):
    return await MetadataClient.get_objects(dataset_id)


def get_node_relative_path(dataset_code, location):
    return location.split(dataset_code)[1]


json_data = {
    'BIDSVersion': '1.0.0',
    'Name': 'False belief task',
    'Authors': ['Moran, J.M.', 'Jolly, E.', 'Mitchell, J.P.'],
    'ReferencesAndLinks': [
        (
            'Moran, J.M. Jolly, E., Mitchell, J.P. (2012). Social-cognitive deficits in normal aging. J Neurosci,',
            ' 32(16):5553-61. doi: 10.1523/JNEUROSCI.5511-11.2012',
        )
    ],
}


def make_temp_folder(files):
    for file in files:
        if not os.path.exists(os.path.dirname(file['file_path'])):
            os.makedirs(os.path.dirname(file['file_path']))
            extension = os.path.splitext(file['file_path'])[1]

            if extension == '.json':
                with open(file['file_path'], 'wb') as outfile:
                    json.dump(json_data, outfile)
            else:
                f = open(file['file_path'], 'wb')
                f.seek(file['file_size'])
                f.write(b'\0')
                f.close()
        else:
            if not os.path.exists(file['file_path']):
                extension = os.path.splitext(file['file_path'])[1]

                if extension == '.json':
                    with open('data.txt', 'w') as outfile:
                        json.dump(json_data, outfile)
                else:
                    f = open(file['file_path'], 'wb')
                    f.seek(file['file_size'])
                    f.write(b'\0')
                    f.close()


async def create_node(payload):
    try:
        created_obj = await MetadataClient.create_object(payload)
    except Exception as e:
        raise APIException(
            error_msg=f'Error calling neo4j node API: {str(e)}', status_code=EAPIResponseCode.internal_error.value
        )
    return created_obj


# FIXME: Refactor to get rid of final references to nodes and geid that are left over from Neo4J
async def get_node_by_geid(obj_id: str):
    try:
        return await MetadataClient.get_by_id(obj_id)
    except Exception as e:
        raise APIException(f'Error calling metadata API: {str(e)}', EAPIResponseCode.internal_error.value)


async def get_parent_node(parent_id):
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


async def delete_node(target_node, access_token, refresh_token):
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
    dataset, source_file, operator, parent, relative_path, access_token, refresh_token, new_name=None
):
    # generate minio object path
    file_name = new_name if new_name else source_file.get('name')

    parent_path = parent.get('parent_path')
    if parent and dataset.id != parent['id']:
        if parent_path:
            parent_path = parent_path + '.' + parent['name']
        else:
            parent_path = parent['name']

    if parent_path:
        fuf_path = parent_path.replace('.', '/') + '/' + file_name
    else:
        fuf_path = file_name
    minio_http = ('https://' if ConfigClass.MINIO_HTTPS else 'http://') + ConfigClass.MINIO_ENDPOINT
    location = 'minio://%s/%s/%s/%s' % (minio_http, dataset.code, ConfigClass.DATASET_FILE_FOLDER, fuf_path)

    payload = {
        'parent': parent.get('id'),
        'parent_path': parent_path,
        'type': 'file',
        'size': source_file.get('size', 0),
        'name': file_name,
        'owner': operator,
        'container_code': dataset.code,
        'container_type': 'dataset',
        'location_uri': location,
    }
    folder_node = await create_node(payload)

    # make minio copy
    try:
        mc = Minio_Client_(access_token, refresh_token)
        # minio location is minio://http://<end_point>/bucket/user/object_path
        minio_path = source_file.get('storage').get('location_uri').split('//')[-1]
        _, bucket, obj_path = tuple(minio_path.split('/', 2))

        mc.copy_object(dataset.code, fuf_path, bucket, obj_path)
        logger.info('Minio Copy %s/%s Success' % (dataset.code, fuf_path))
    except Exception as e:
        logger.error('error when uploading: ' + str(e))

    return folder_node, folder_node['parent']


async def create_folder_node(dataset_code, source_folder, operator, parent_node, relative_path, new_name=None):
    folder_name = new_name if new_name else source_folder.get('name')
    # create node in metadata
    payload = {
        'parent': parent_node['id'],
        'parent_path': relative_path,
        'type': 'folder',
        'name': folder_name,
        'owner': operator,
        'container_code': dataset_code,
        'container_type': 'dataset',
    }
    folder_node = await create_node(payload)
    return folder_node, folder_node['parent']
