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

from aioredis import StrictRedis
from common import LoggerFactory
from sqlalchemy.future import select
from starlette.concurrency import run_in_threadpool

from app.clients.metadata import MetadataClient
from app.commons.service_connection.minio_client import Minio_Client
from app.config import ConfigClass
from app.models.schema import DatasetSchema
from app.models.version import DatasetVersion
from app.resources.locks import recursive_lock_publish
from app.resources.locks import unlock_resource
from app.resources.utils import get_children_nodes
from app.services.activity_log import DatasetActivityLogService

logger = LoggerFactory('api_version').get_logger()


def parse_minio_location(location):
    minio_path = location.split('//')[-1]
    _, bucket, obj_path = tuple(minio_path.split('/', 2))
    return {'bucket': bucket, 'path': obj_path}


class PublishVersion(object):
    def __init__(self, dataset, operator, notes, status_id, version):
        self.activity_log = DatasetActivityLogService()
        self.operator = operator
        self.notes = notes
        self.dataset = dataset
        self.dataset_files = []
        tmp_base = '/tmp/'
        self.tmp_folder = tmp_base + str(time.time()) + '/'
        self.zip_path = tmp_base + dataset.code + '_' + str(datetime.now())
        self.mc = Minio_Client()
        self.redis_client = StrictRedis(
            host=ConfigClass.REDIS_HOST,
            port=ConfigClass.REDIS_PORT,
            password=ConfigClass.REDIS_PASSWORD,
            db=ConfigClass.REDIS_DB,
        )
        self.status_id = status_id
        self.version = version

    async def publish(self, db):
        try:
            # lock file here
            level1_nodes = await get_children_nodes(self.dataset.code, None)
            locked_node, err = await recursive_lock_publish(level1_nodes)
            if err:
                raise err
            items = await MetadataClient.get_objects(self.dataset.code)
            await self.get_dataset_files(items)
            self.download_dataset_files()
            await self.add_schemas(db)
            await run_in_threadpool(self.zip_files)
            minio_location = await self.upload_version()
            try:
                dataset_version = DatasetVersion(
                    dataset_code=self.dataset.code,
                    dataset_geid=str(self.dataset.id),
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

            logger.info(f'Successfully published {self.dataset.id} version {self.version}')
            await self.activity_log.send_publish_version_succeed(dataset_version, self.dataset)
            await self.update_status('success')
        except Exception as e:
            error_msg = f'Error publishing {self.dataset.id}: {str(e)}'
            logger.error(error_msg)
            await self.update_status('failed', error_msg=error_msg)
        finally:
            # unlock the nodes if we got blocked
            for resource_key, operation in locked_node:
                await unlock_resource(resource_key, operation)

        return

    async def update_status(self, status, error_msg=''):
        """Updates job status in redis."""
        redis_status = json.dumps(
            {
                'status': status,
                'error_msg': error_msg,
            }
        )
        await self.redis_client.set(self.status_id, redis_status, ex=1 * 60 * 60)

    async def get_dataset_files(self, nodes):
        """get all files from dataset."""
        for item in nodes:
            if item['type'] == 'file':
                self.dataset_files.append(item)
        return self.dataset_files

    def download_dataset_files(self):
        """Download files from minio."""
        file_paths = []
        for file in self.dataset_files:
            location_data = parse_minio_location(file['storage']['location_uri'])
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
            DatasetSchema.dataset_geid == str(self.dataset.id), DatasetSchema.is_draft.is_(False)
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

    async def upload_version(self):
        """Upload version zip to minio."""
        bucket = self.dataset.code
        path = 'versions/' + self.zip_path.split('/')[-1] + '.zip'
        try:
            await run_in_threadpool(
                self.mc.client.fput_object,
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
