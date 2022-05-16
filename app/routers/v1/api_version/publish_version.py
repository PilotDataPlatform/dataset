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
import shutil
import time
from datetime import datetime

import httpx
from aioredis import StrictRedis
from common import GEIDClient
from common import LoggerFactory
from sqlalchemy.future import select

from app.commons.service_connection.minio_client import Minio_Client
from app.config import ConfigClass
from app.models.schema import DatasetSchema
from app.models.version import DatasetVersion
from app.resources.locks import recursive_lock_publish
from app.resources.locks import unlock_resource
from app.resources.neo4j_helper import get_children_nodes

logger = LoggerFactory('api_version').get_logger()


def parse_minio_location(location):
    minio_path = location.split('//')[-1]
    _, bucket, obj_path = tuple(minio_path.split('/', 2))
    return {'bucket': bucket, 'path': obj_path}


class PublishVersion(object):
    def __init__(self, dataset_node, operator, notes, status_id, version):
        self.operator = operator
        self.notes = notes
        self.dataset_node = dataset_node
        self.dataset_geid = dataset_node['id']
        self.dataset_files = []
        tmp_base = '/tmp/'
        self.tmp_folder = tmp_base + str(time.time()) + '/'
        self.zip_path = tmp_base + dataset_node['code'] + '_' + str(datetime.now())
        self.mc = Minio_Client()
        self.redis_client = StrictRedis(
            host=ConfigClass.REDIS_HOST,
            port=ConfigClass.REDIS_PORT,
            password=ConfigClass.REDIS_PASSWORD,
            db=ConfigClass.REDIS_DB,
        )
        self.status_id = status_id
        self.version = version

        self.geid_client = GEIDClient()

    async def publish(self, db):
        try:
            # TODO some merge needed here since get_children_nodes and
            # get_dataset_files_recursive both get the nodes under the dataset

            # lock file here
            level1_nodes = await get_children_nodes(self.dataset_geid, start_label='Dataset')
            locked_node, err = await recursive_lock_publish(level1_nodes)
            if err:
                raise err

            await self.get_dataset_files_recursive(self.dataset_geid)
            self.download_dataset_files()
            await self.add_schemas(db)
            self.zip_files()
            minio_location = self.upload_version()
            try:
                dataset_version = DatasetVersion(
                    dataset_code=self.dataset_node['code'],
                    dataset_geid=self.dataset_geid,
                    version=str(self.version),
                    created_by=self.operator,
                    location=minio_location,
                    notes=self.notes,
                )
                db.add(dataset_version)
                await db.commit()
            except Exception as e:
                logger.error('Psql Error: ' + str(e))
                raise e

            logger.info(f'Successfully published {self.dataset_geid} version {self.version}')
            await self.update_activity_log()
            await self.update_status('success')
        except Exception as e:
            error_msg = f'Error publishing {self.dataset_geid}: {str(e)}'
            logger.error(error_msg)
            await self.update_status('failed', error_msg=error_msg)
        finally:
            # unlock the nodes if we got blocked
            for resource_key, operation in locked_node:
                await unlock_resource(resource_key, operation)

        return

    async def update_activity_log(self):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_PUBLISH_SUCCEED',
            'payload': {
                'dataset_geid': self.dataset_geid,
                'act_geid': self.geid_client.get_GEID(),
                'operator': self.operator,
                'action': 'PUBLISH',
                'resource': 'Dataset',
                'detail': {'source': self.version},
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            logger.error(error_msg)
            raise Exception(error_msg)
        return res

    async def update_status(self, status, error_msg=''):
        """Updates job status in redis."""
        redis_status = json.dumps(
            {
                'status': status,
                'error_msg': error_msg,
            }
        )
        await self.redis_client.set(self.status_id, redis_status, ex=1 * 60 * 60)

    async def get_dataset_files_recursive(self, geid, start_label='Dataset'):
        """get all files from dataset."""
        query = {
            'start_label': start_label,
            'end_labels': ['File', 'Folder'],
            'query': {
                'start_params': {
                    'global_entity_id': geid,
                },
                'end_params': {
                    'archived': False,
                },
            },
        }
        async with httpx.AsyncClient() as client:
            resp = await client.post(ConfigClass.NEO4J_SERVICE_V2 + 'relations/query', json=query)
        for node in resp.json()['results']:
            if 'File' in node['labels']:
                self.dataset_files.append(node)
            else:
                await self.get_dataset_files_recursive(node['global_entity_id'], start_label='Folder')
        return self.dataset_files

    def download_dataset_files(self):
        """Download files from minio."""
        file_paths = []
        for file in self.dataset_files:
            location_data = parse_minio_location(file['location'])
            try:
                self.mc.client.fget_object(
                    location_data['bucket'], location_data['path'], self.tmp_folder + '/' + location_data['path']
                )
                file_paths.append(self.tmp_folder + '/' + location_data['path'])
            except Exception as e:
                error_msg = f'Error download files from minio: {str(e)}'
                logger.error(error_msg)
                raise Exception(error_msg)
        return file_paths

    def zip_files(self):
        shutil.make_archive(self.zip_path, 'zip', self.tmp_folder)
        return self.zip_path

    async def add_schemas(self, db):
        """Saves schema json files to folder that will zipped."""
        if not os.path.isdir(self.tmp_folder):
            os.mkdir(self.tmp_folder)
            os.mkdir(self.tmp_folder + '/data')

        query = select(DatasetSchema).where(
            DatasetSchema.dataset_geid == self.dataset_geid, DatasetSchema.is_draft.is_(False)
        )
        query_default = query.where(DatasetSchema.standard == 'default')
        query_open_minds = query.where(DatasetSchema.standard == 'open_minds')

        schemas_default = (await db.execute(query_default)).scalars().all()
        schemas_open_minds = (await db.execute(query_open_minds)).scalars().all()

        for schema in schemas_default:
            with open(self.tmp_folder + '/default_' + schema.name, 'w') as w:
                w.write(json.dumps(schema.content, indent=4, ensure_ascii=False))

        for schema in schemas_open_minds:
            with open(self.tmp_folder + '/openMINDS_' + schema.name, 'w') as w:
                w.write(json.dumps(schema.content, indent=4, ensure_ascii=False))

    def upload_version(self):
        """Upload version zip to minio."""
        bucket = self.dataset_node['code']
        path = 'versions/' + self.zip_path.split('/')[-1] + '.zip'
        try:
            self.mc.client.fput_object(
                bucket,
                path,
                self.zip_path + '.zip',
            )
            minio_http = ('https://' if ConfigClass.MINIO_HTTPS else 'http://') + ConfigClass.MINIO_ENDPOINT
            minio_location = f'minio://{minio_http}/{bucket}/{path}'
        except Exception as e:
            error_msg = f'Error uploading files to minio: {str(e)}'
            logger.error(error_msg)
            raise Exception(error_msg)
        return minio_location
