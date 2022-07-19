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

from common import LoggerFactory
from fastapi import APIRouter
from fastapi import Depends
from fastapi_utils import cbv
from sqlalchemy.future import select

from app.config import ConfigClass
from app.core.db import get_db_session
from app.models.schema import DatasetSchema
from app.models.schema import DatasetSchemaTemplate
from app.resources.error_handler import APIException
from app.resources.error_handler import catch_internal
from app.schemas.base import APIResponse
from app.schemas.base import EAPIResponseCode
from app.schemas.schema import DELETESchema
from app.schemas.schema import DELETESchemaResponse
from app.schemas.schema import GETSchemaResponse
from app.schemas.schema import POSTSchema
from app.schemas.schema import POSTSchemaList
from app.schemas.schema import POSTSchemaResponse
from app.schemas.schema import PUTSchema
from app.schemas.schema import PUTSchemaResponse
from app.services.activity_log import DatasetActivityLogService
from app.services.dataset import SrvDatasetMgr

logger = LoggerFactory('api_schema').get_logger()
router = APIRouter()
ESSENTIALS_NAME = ConfigClass.ESSENTIALS_NAME


@cbv.cbv(router)
class Schema:
    ACTIVITY_LOG = DatasetActivityLogService()
    SRV_DATASET = SrvDatasetMgr()

    async def update_dataset_node(self, db, dataset_geid, content):
        # Update dataset neo4j entry

        dataset = await self.SRV_DATASET.get_bygeid(db, dataset_geid)
        payload = {}
        required_fields = ['dataset_title', 'dataset_authors', 'dataset_description', 'dataset_type']
        optional_fields = ['dataset_modality', 'dataset_collection_method', 'dataset_license', 'dataset_tags']
        for field in required_fields:
            if field not in content:
                raise APIException(
                    error_msg=f'Missing content field for essential schema: {field}',
                    status_code=EAPIResponseCode.bad_request.value,
                )
            payload[field.replace('dataset_', '')] = content[field]
        for field in optional_fields:
            if field in content:
                payload[field.replace('dataset_', '')] = content[field]

        # Frontend can't easily pass a blank string if license
        # should be removed, so update it to blank if it exists
        # on the node and doesn't exist in payload
        if dataset.license and 'license' not in payload:
            payload['license'] = ''

        await self.SRV_DATASET.update(db, dataset, payload)

    async def db_add_operation(self, schema, db):
        try:
            db.add(schema)
            await db.commit()
            await db.refresh(schema)
        except Exception as e:
            error_msg = f'Psql Error: {str(e)}'
            await db.rollback()
            logger.error(error_msg)
            raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
        return schema

    async def db_delete_operation(self, schema, db):
        try:
            await db.delete(schema)
            await db.commit()
        except Exception as e:
            error_msg = f'Psql Error: {str(e)}'
            await db.rollback()
            logger.error(error_msg)
            raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)

    async def get_schema_or_404(self, schema_geid, db):
        try:
            query = select(DatasetSchema).where(DatasetSchema.geid == schema_geid)
            schema = (await db.execute(query)).scalars().first()

            if not schema:
                logger.info('Schema not found')
                raise APIException(error_msg='Schema not found', status_code=EAPIResponseCode.not_found.value)
        except APIException as e:
            raise e
        except Exception as e:
            error_msg = f'Psql Error: {str(e)}'
            logger.error(error_msg)
            raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
        return schema

    async def duplicate_check(self, name, dataset_geid, db):
        query = select(DatasetSchema).where(DatasetSchema.name == name, DatasetSchema.dataset_geid == dataset_geid)
        result = (await db.execute(query)).scalars()
        if result.first():
            error_msg = 'Schema with that name already exists'
            logger.info(error_msg)
            raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.conflict.value)

    @router.post('/v1/schema', tags=['schema'], response_model=POSTSchemaResponse, summary='Create a new schema')
    async def create(self, data: POSTSchema, db=Depends(get_db_session)):
        logger.info('Calling schema create')
        api_response = POSTSchemaResponse()

        await self.duplicate_check(data.name, data.dataset_geid, db)
        query = select(DatasetSchemaTemplate).where(DatasetSchemaTemplate.geid == data.tpl_geid)
        result = (await db.execute(query)).scalars().first()
        if not result:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = 'Template not found'
            logger.info(api_response.error_msg)
            return api_response.json_response()

        model_data = {
            'geid': str(uuid4()),
            'name': data.name,
            'dataset_geid': data.dataset_geid,
            'tpl_geid': data.tpl_geid,
            'standard': data.standard,
            'system_defined': data.system_defined,
            'is_draft': data.is_draft,
            'content': data.content,
            'creator': data.creator,
        }
        schema = DatasetSchema(**model_data)
        schema = await self.db_add_operation(schema, db)
        api_response.result = schema.to_dict()

        dataset = await self.SRV_DATASET.get_bygeid(db, schema.dataset_geid)
        await self.ACTIVITY_LOG.send_schema_create_event(schema, dataset, data.creator)

        return api_response.json_response()

    @router.get('/v1/schema/{schema_geid}', tags=['schema'], response_model=GETSchemaResponse, summary='Get a schema')
    async def get(self, schema_geid: str, db=Depends(get_db_session)):
        logger.info('Calling schema get')
        api_response = POSTSchemaResponse()
        schema = await self.get_schema_or_404(schema_geid, db)
        api_response.result = schema.to_dict()
        return api_response.json_response()

    @router.put(
        '/v1/schema/{schema_geid}', tags=['schema'], response_model=PUTSchemaResponse, summary='update a schema'
    )
    async def update(self, schema_geid: str, data: PUTSchema, db=Depends(get_db_session)):
        logger.info('Calling schema update')
        api_response = POSTSchemaResponse()
        schema = await self.get_schema_or_404(schema_geid, db)

        if data.name is not None:
            if data.name != schema.name:
                await self.duplicate_check(data.name, schema.dataset_geid)

        fields = ['name', 'standard', 'is_draft', 'content']
        for field in fields:
            if getattr(data, field) is not None:
                setattr(schema, field, getattr(data, field))

        schema = await self.db_add_operation(schema, db)
        api_response.result = schema.to_dict()
        dataset = await self.SRV_DATASET.get_bygeid(db, schema.dataset_geid)

        changes = []
        if data.activity:
            detail = data['activity'].get('detail', {})
            for target in detail['targets']:
                changes.append(
                    {
                        'property': target.lower(),
                    }
                )
        await self.ACTIVITY_LOG.send_schema_update_event(schema, dataset, data.username, changes)
        if schema.name == 'essential.schema.json':
            await self.update_dataset_node(db, schema.dataset_geid, data.content)
        return api_response.json_response()

    @router.delete(
        '/v1/schema/{schema_geid}', tags=['schema'], response_model=DELETESchemaResponse, summary='Delete a schema'
    )
    async def delete(self, schema_geid: str, data: DELETESchema, db=Depends(get_db_session)):
        logger.info('Calling schema delete')
        api_response = POSTSchemaResponse()
        schema = await self.get_schema_or_404(schema_geid, db)
        await self.db_delete_operation(schema, db)

        dataset = await self.SRV_DATASET.get_bygeid(db, schema.dataset_geid)
        await self.ACTIVITY_LOG.send_schema_delete_event(schema, dataset, data.username)

        api_response.result = 'success'
        return api_response.json_response()

    @router.post('/v1/schema/list', tags=['schema'], summary='API will list the schema by condition')
    @catch_internal('schema')
    async def list_schema(self, request_payload: POSTSchemaList, db=Depends(get_db_session)):
        api_response = APIResponse()
        result = None
        filter_allowed = [
            'name',
            'dataset_geid',
            'tpl_geid',
            'standard',
            'system_defined',
            'is_draft',
            'create_timestamp',
            'update_timestamp',
            'creator',
        ]
        query = select(DatasetSchema)
        for key in filter_allowed:
            filter_val = getattr(request_payload, key)
            if filter_val is not None:
                query = query.where(getattr(DatasetSchema, key) == filter_val)
        result = (await db.execute(query)).scalars()
        schemas_fetched = result.all()
        result = [record.to_dict() for record in schemas_fetched] if schemas_fetched else []
        # essentials rank top
        essentials = [record for record in result if record['name'] == ESSENTIALS_NAME]
        not_essentails = [record for record in result if record['name'] != ESSENTIALS_NAME]
        if len(essentials) > 0:
            essentials_schema = essentials[0]
            not_essentails.insert(0, essentials_schema)
        # response 200
        api_response.code = EAPIResponseCode.success
        api_response.result = not_essentails

        return api_response.json_response()
