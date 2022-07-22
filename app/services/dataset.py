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
import time
from typing import Optional
from uuid import UUID
from uuid import uuid4

import httpx
from common import LoggerFactory
from fastapi import Query
from fastapi_pagination import Params as BaseParams
from fastapi_pagination.ext.async_sqlalchemy import paginate
from minio.sseconfig import Rule
from minio.sseconfig import SSEConfig
from sqlalchemy import desc
from sqlalchemy import update
from sqlalchemy.future import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound
from starlette.concurrency import run_in_threadpool

from app.commons.service_connection.dataset_policy_template import (
    create_dataset_policy_template,
)
from app.commons.service_connection.minio_client import Minio_Client
from app.config import ConfigClass
from app.models.dataset import Dataset
from app.models.schema import DatasetSchema
from app.models.schema import DatasetSchemaTemplate
from app.services.activity_log import DatasetActivityLogService

ESSENTIALS_TPL_NAME = ConfigClass.ESSENTIALS_TPL_NAME
ESSENTIALS_NAME = ConfigClass.ESSENTIALS_NAME


class Params(BaseParams):
    size: int = Query(10, ge=1, le=999, description='Page size')


class SrvDatasetMgr:
    """Service for Dataset Entity INFO Manager."""

    logger = LoggerFactory('SrvDatasetMgr').get_logger()

    async def create(
        self,
        db,
        username,
        code,
        title,
        authors,
        dataset_type,
        modality,
        collection_method,
        tags,
        dataset_license,
        description,
    ):
        """Create File Data Entity V2."""
        post_json_form = {
            'source': '',
            'title': title,
            'authors': authors,
            'code': code,
            'type': dataset_type,
            'modality': modality,
            'collection_method': collection_method,
            'license': dataset_license,
            'tags': tags,
            'description': description,
            'size': 0,
            'total_files': 0,
            'creator': username,
        }
        self.logger.debug('SrvDatasetMgr post_json_form' + str(post_json_form))
        dataset_schema = Dataset(**post_json_form)
        dataset = await db_add_operation(dataset_schema, db)
        global_entity_id = str(dataset.id)
        await self.__create_atlas_node(global_entity_id, username)
        await self.__create_essentials(
            db,
            global_entity_id,
            code,
            title,
            authors,
            dataset_type,
            modality,
            collection_method,
            dataset_license,
            description,
            tags,
            username,
        )
        await self.__on_create_event(dataset)
        # and also create minio bucket with the dataset code
        try:
            # TODO: IO-blocking code, also review this add policy logic.
            #       We are getting an keyError exception when we try to
            #       append the new resource. After "fixing" that we get
            #       this error:
            #       mc: <ERROR> Unable to add new policy. Policy has invalid resource.
            mc = Minio_Client()
            await run_in_threadpool(mc.client.make_bucket, code)
            await run_in_threadpool(mc.client.set_bucket_encryption, code, SSEConfig(Rule.new_sse_s3_rule()))

            self.logger.info('createing the policy')
            # also use the lazy loading to create the policy in minio
            stream = os.popen(f'mc admin policy info minio {username}')
            output = stream.read()
            policy_file_name = None
            try:
                policy = json.loads(output)
                # if there is a policy then we append the new to the resource
                policy['Statement'][0]['Resource'].append('arn:aws:s3:::%s/*' % (code))
                policy_file_name = create_dataset_policy_template(code, json.dumps(policy))
            except json.decoder.JSONDecodeError:
                # if not found then we just create a new one for user
                policy_file_name = create_dataset_policy_template(code)

            stream = os.popen('mc admin policy add minio %s %s' % (username, policy_file_name))
            output = stream.read()
            # then remove the policy file until the os is finish
            # otherwise there will be the racing issue
            os.remove(policy_file_name)

        except Exception as e:
            self.logger.error('error when creating minio: ' + str(e))
        return dataset.to_dict()

    async def update(self, db, current_node, update_json):
        try:
            await db.execute(update(Dataset).where(Dataset.id == current_node.id).values({**update_json}))
            await db.commit()
        except Exception as e:
            await db.rollback()
            error_msg = f'Psql Error: {str(e)}'
            raise Exception(error_msg)
        return current_node.to_dict()

    async def get_bygeid(self, db: Session, geid: str) -> Dataset:
        return await db.get(Dataset, UUID(geid))

    async def get_bycode(self, db: Session, code: str) -> Optional[Dataset]:
        try:
            query = select(Dataset).where(Dataset.code == code)
            result = (await db.execute(query)).scalars().one()
            return result
        except NoResultFound:
            return

    async def get_dataset_by_creator(self, db, creator, page, page_size):
        return await paginate(
            db,
            select(Dataset).where(Dataset.creator == creator).order_by(desc(Dataset.created_at)),
            Params(page=page, size=page_size),
        )

    async def __create_atlas_node(self, geid, username):
        res = await create_atlas_dataset(geid, username)
        if res.status_code != 200:
            raise Exception('__create_atlas_node {}: {}'.format(res.status_code, res.text))
        return res

    async def __create_essentials(
        self,
        db,
        dataset_geid,
        code,
        title,
        authors,
        dataset_type,
        modality,
        collection_method,
        dataset_license,
        description,
        tags,
        creator,
    ):
        async def get_essential_tpl() -> DatasetSchemaTemplate:
            query = select(DatasetSchemaTemplate).where(DatasetSchemaTemplate.name == ESSENTIALS_TPL_NAME)
            etpl_result = (await db.execute(query)).scalars().all()
            if not etpl_result:
                raise Exception('{} template not found in database.'.format(ESSENTIALS_TPL_NAME))
            etpl_result = etpl_result[0]
            return etpl_result

        etpl = await get_essential_tpl()
        model_data = {
            'geid': str(uuid4()),
            'name': ESSENTIALS_NAME,
            'dataset_geid': dataset_geid,
            'tpl_geid': etpl.geid,
            'standard': etpl.standard,
            'system_defined': etpl.system_defined,
            'is_draft': False,
            'content': {
                'dataset_title': title,
                'dataset_authors': authors,
                'dataset_type': dataset_type,
                'dataset_modality': modality,
                'dataset_collection_method': collection_method,
                'dataset_license': dataset_license,
                'dataset_description': description,
                'dataset_tags': tags,
                'dataset_code': code,
            },
            'creator': creator,
        }
        schema = DatasetSchema(**model_data)
        schema = await db_add_operation(schema, db)
        return schema.to_dict()

    async def __on_create_event(self, dataset: Dataset):
        activitity_log = DatasetActivityLogService()
        return await activitity_log.send_dataset_on_create_event(dataset)


async def db_add_operation(schema, db):
    try:
        db.add(schema)
        await db.commit()
        await db.refresh(schema)
    except Exception as e:
        await db.rollback()
        error_msg = f'Psql Error: {str(e)}'
        raise Exception(error_msg)
    return schema


async def create_atlas_dataset(geid, operator):
    attrs = {
        'global_entity_id': geid,
        'qualifiedName': geid,
        'name': geid,
        'createTime': time.time(),
        'modifiedTime': 0,
        'replicatedTo': None,
        'userDescription': None,
        'isFile': False,
        'numberOfReplicas': 0,
        'replicatedFrom': None,
        'displayName': None,
        'extendedAttributes': None,
        'nameServiceId': None,
        'posixPermissions': None,
        'clusterName': None,
        'isSymlink': False,
        'group': None,
    }
    atlas_post_form_json = {
        'referredEntities': {},
        'entity': {
            'typeName': 'dataset',
            'attributes': attrs,
            'isIncomplete': False,
            'status': 'ACTIVE',
            'createdBy': operator,
            'version': 0,
            'relationshipAttributes': {'schema': [], 'inputToProcesses': [], 'meanings': [], 'outputFromProcesses': []},
            'customAttributes': {},
            'labels': [],
        },
    }
    url = ConfigClass.LINEAGE_SERVICE + '/entity'
    async with httpx.AsyncClient() as client:
        res = await client.post(url, json=atlas_post_form_json)
    return res
