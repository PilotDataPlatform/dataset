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

import httpx

from app.config import ConfigClass


async def get_files_recursive(folder_geid, all_files=None):
    if not all_files:
        all_files = []
    query = {
        'start_label': 'Folder',
        'end_labels': ['File', 'Folder'],
        'query': {
            'start_params': {
                'global_entity_id': folder_geid,
            },
            'end_params': {},
        },
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(ConfigClass.NEO4J_SERVICE_V2 + 'relations/query', json=query)
    for node in resp.json()['results']:
        if 'File' in node['labels']:
            all_files.append(node)
        else:
            await get_files_recursive(node['global_entity_id'], all_files=all_files)
    return all_files


async def get_related_nodes(dataset_geid):
    query = {
        'start_label': 'Dataset',
        'end_labels': ['File', 'Folder'],
        'query': {
            'start_params': {
                'global_entity_id': str(dataset_geid),
            },
            'end_params': {},
        },
    }
    async with httpx.AsyncClient() as client:
        resp = await client.post(ConfigClass.NEO4J_SERVICE_V2 + 'relations/query', json=query)

    return resp.json()['results']


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
