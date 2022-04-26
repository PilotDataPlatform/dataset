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

import httpx
from common import GEIDClient
from fastapi import APIRouter
from fastapi_sqlalchemy import db
from fastapi_utils import cbv

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.config import ConfigClass
from app.models.base_models import APIResponse
from app.models.base_models import EAPIResponseCode
from app.models.schema_models import DELETESchema
from app.models.schema_models import DELETESchemaResponse
from app.models.schema_models import GETSchemaResponse
from app.models.schema_models import POSTSchema
from app.models.schema_models import POSTSchemaList
from app.models.schema_models import POSTSchemaResponse
from app.models.schema_models import PUTSchema
from app.models.schema_models import PUTSchemaResponse
from app.models.schema_sql import DatasetSchema
from app.models.schema_sql import DatasetSchemaTemplate
from app.resources.error_handler import APIException
from app.resources.error_handler import catch_internal

logger = SrvLoggerFactory('api_schema').get_logger()
router = APIRouter()
ESSENTIALS_NAME = ConfigClass.ESSENTIALS_NAME


@cbv.cbv(router)
class Schema:
    def __init__(self) -> None:
        self.geid_client = GEIDClient()

    def update_dataset_node(self, dataset_geid, content):
        # Update dataset neo4j entry
        dataset_node = self.get_dataset_by_geid(dataset_geid)
        dataset_id = dataset_node['id']

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
        if dataset_node.get('license') and 'license' not in payload:
            payload['license'] = ''
        with httpx.Client() as client:
            response = client.put(ConfigClass.NEO4J_SERVICE + f'nodes/Dataset/node/{dataset_id}', json=payload)
        if response.status_code != 200:
            logger.error(response.json())
            raise APIException(error_msg=response.json(), status_code=response.status_code)

    def get_dataset_by_geid(self, dataset_geid):
        payload = {'global_entity_id': dataset_geid}
        with httpx.Client() as client:
            response = client.post(ConfigClass.NEO4J_SERVICE + 'nodes/Dataset/query', json=payload)
        if not response.json():
            raise APIException(status_code=404, error_msg='Dataset not found')
        return response.json()[0]

    def update_activity_log(self, activity_data):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': activity_data['event_type'],
            'payload': {
                'dataset_geid': activity_data['dataset_geid'],
                'act_geid': self.geid_client.get_GEID(),
                'operator': activity_data['username'],
                'action': activity_data['action'],
                'resource': activity_data['resource'],
                'detail': activity_data['detail'],
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        with httpx.Client() as client:
            res = client.post(url, json=post_json)
        if res.status_code != 200:
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            logger.error(error_msg)
            raise Exception(error_msg)
        return res

    def db_add_operation(self, schema):
        try:
            db.session.add(schema)
            db.session.commit()
            db.session.refresh(schema)
        except Exception as e:
            error_msg = f'Psql Error: {str(e)}'
            db.session.rollback()
            logger.error(error_msg)
            raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)
        return schema

    def db_delete_operation(self, schema):
        try:
            db.session.delete(schema)
            db.session.commit()
        except Exception as e:
            error_msg = f'Psql Error: {str(e)}'
            db.session.rollback()
            logger.error(error_msg)
            raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.internal_error.value)

    def get_schema_or_404(self, schema_geid):
        try:
            schema = db.session.query(DatasetSchema).filter_by(geid=schema_geid).first()
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

    def duplicate_check(self, name, dataset_geid):
        if db.session.query(DatasetSchema).filter_by(name=name, dataset_geid=dataset_geid).first():
            error_msg = 'Schema with that name already exists'
            logger.info(error_msg)
            raise APIException(error_msg=error_msg, status_code=EAPIResponseCode.conflict.value)

    @router.post('/v1/schema', tags=['schema'], response_model=POSTSchemaResponse, summary='Create a new schema')
    async def create(self, data: POSTSchema):
        logger.info('Calling schema create')
        api_response = POSTSchemaResponse()

        self.duplicate_check(data.name, data.dataset_geid)

        if not db.session.query(DatasetSchemaTemplate).filter_by(geid=data.tpl_geid).first():
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = 'Template not found'
            logger.info(api_response.error_msg)
            return api_response.json_response()

        model_data = {
            'geid': self.geid_client.get_GEID(),
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
        schema = self.db_add_operation(schema)
        api_response.result = schema.to_dict()

        for activity in data.activity:
            activity_data = {
                'dataset_geid': data.dataset_geid,
                'username': data.creator,
                'event_type': 'SCHEMA_CREATE',
                **activity,
            }
            self.update_activity_log(activity_data)

        return api_response.json_response()

    @router.get('/v1/schema/{schema_geid}', tags=['schema'], response_model=GETSchemaResponse, summary='Get a schema')
    async def get(self, schema_geid: str):
        logger.info('Calling schema get')
        api_response = POSTSchemaResponse()
        schema = self.get_schema_or_404(schema_geid)
        api_response.result = schema.to_dict()
        return api_response.json_response()

    @router.put(
        '/v1/schema/{schema_geid}', tags=['schema'], response_model=PUTSchemaResponse, summary='update a schema'
    )
    async def update(self, schema_geid: str, data: PUTSchema):
        logger.info('Calling schema update')
        api_response = POSTSchemaResponse()
        schema = self.get_schema_or_404(schema_geid)

        if data.name is not None:
            if data.name != schema.name:
                self.duplicate_check(data.name, schema.dataset_geid)

        fields = ['name', 'standard', 'is_draft', 'content']
        for field in fields:
            if getattr(data, field) is not None:
                setattr(schema, field, getattr(data, field))

        schema = self.db_add_operation(schema)
        api_response.result = schema.to_dict()
        for activity in data.activity:
            activity_data = {
                'dataset_geid': data.dataset_geid,
                'username': data.username,
                'event_type': 'SCHEMA_UPDATE',
                **activity,
            }
            self.update_activity_log(activity_data)
        if schema.name == 'essential.schema.json':
            self.update_dataset_node(schema.dataset_geid, data.content)
        return api_response.json_response()

    @router.delete(
        '/v1/schema/{schema_geid}', tags=['schema'], response_model=DELETESchemaResponse, summary='Delete a schema'
    )
    async def delete(self, schema_geid: str, data: DELETESchema):
        logger.info('Calling schema delete')
        api_response = POSTSchemaResponse()
        schema = self.get_schema_or_404(schema_geid)
        schema = self.db_delete_operation(schema)

        for activity in data.activity:
            activity_data = {
                'dataset_geid': data.dataset_geid,
                'username': data.username,
                'event_type': 'SCHEMA_DELETE',
                **activity,
            }
            self.update_activity_log(activity_data)

        api_response.result = 'success'
        return api_response.json_response()

    @router.post('/v1/schema/list', tags=['schema'], summary='API will list the schema by condition')
    @catch_internal('schema')
    async def list_schema(self, request_payload: POSTSchemaList):
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
        query = db.session.query(DatasetSchema)
        for key in filter_allowed:
            filter_val = getattr(request_payload, key)
            if filter_val is not None:
                query = query.filter(getattr(DatasetSchema, key) == filter_val)

        schemas_fetched = query.all()
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
