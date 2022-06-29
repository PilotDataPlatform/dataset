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
from sqlalchemy.orm.exc import NoResultFound

from app.core.db import get_db_session
from app.models.schema import DatasetSchemaTemplate
from app.resources.error_handler import catch_internal
from app.schemas.base import APIResponse
from app.schemas.base import EAPIResponseCode
from app.schemas.schema_template import SchemaTemplateList
from app.schemas.schema_template import SchemaTemplatePost
from app.schemas.schema_template import SchemaTemplatePut
from app.services.activity_log import DatasetActivityLogService

router = APIRouter()

_API_TAG = 'V1 DATASET'
_API_NAMESPACE = 'api_dataset'

HEADERS = {'accept': 'application/json', 'Content-Type': 'application/json'}


# this function will check if the template name already exist
async def check_template_name(name, dataset_geid, db):
    try:
        query = (
            select(DatasetSchemaTemplate)
            .where(DatasetSchemaTemplate.name == name)
            .where(DatasetSchemaTemplate.dataset_geid == dataset_geid)
        )
        (await db.execute(query)).scalars().one()
    except NoResultFound:
        return False

    return True


@cbv.cbv(router)
class APISchemaTemplate:
    """API for dataset schema template."""

    def __init__(self):
        self.__logger = LoggerFactory('api_dataset_schema_template').get_logger()
        self.__activity_manager = DatasetActivityLogService()

    @router.post(
        '/dataset/{dataset_geid}/schemaTPL', tags=[_API_TAG], summary='API will create the new schema template'
    )
    @catch_internal(_API_NAMESPACE)
    async def create_schema_template(
        self, dataset_geid, request_payload: SchemaTemplatePost, db=Depends(get_db_session)
    ):
        api_response = APIResponse()
        # here we enforce the uniqueness of the name within dataset_geid
        exist = await check_template_name(request_payload.name, dataset_geid, db)
        if exist:
            api_response.code = EAPIResponseCode.forbidden
            api_response.error_msg = 'The template name already exists.'
            return api_response
        try:
            new_template = DatasetSchemaTemplate(
                geid=str(uuid4()),
                name=request_payload.name,
                dataset_geid=dataset_geid,
                standard=request_payload.standard,
                system_defined=request_payload.system_defined,
                is_draft=request_payload.is_draft,
                content=request_payload.content,
                creator=request_payload.creator,
            )

            db.add(new_template)
            await db.commit()
            api_response.result = new_template.to_dict()

            # create the log activity
            await self.__activity_manager.send_schema_template_on_create_event(
                dataset_geid, new_template.geid, request_payload.creator, request_payload.name
            )
        except Exception as e:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = str(e)
            await db.rollback()

        return api_response.json_response()

    @router.post(
        '/dataset/{dataset_geid}/schemaTPL/list',
        tags=[_API_TAG],  # , response_model=PreUploadResponse,
        summary='API will list the template by condition',
    )
    @catch_internal(_API_NAMESPACE)
    async def list_schema_template(self, dataset_geid, request_payload: SchemaTemplateList, db=Depends(get_db_session)):
        api_response = APIResponse()
        result = None

        try:
            if dataset_geid == 'default':
                query = select(DatasetSchemaTemplate).where(DatasetSchemaTemplate.system_defined.is_(True))
            else:
                query = select(DatasetSchemaTemplate).where(DatasetSchemaTemplate.dataset_geid == dataset_geid)
            result = (await db.execute(query)).scalars().all()

            ret = []
            for x in result:
                ret.append(
                    {
                        'geid': x.geid,
                        'name': x.name,
                        'system_defined': x.system_defined,
                        'standard': x.standard,
                    }
                )
            api_response.result = ret
        except Exception as e:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = str(e)

        return api_response

    ##########################################################################################################

    @router.get(
        '/dataset/{dataset_geid}/schemaTPL/{template_geid}',
        tags=[_API_TAG],
        summary='API will get the template by geid',
    )
    @catch_internal(_API_NAMESPACE)
    async def get_schema_template(self, dataset_geid, template_geid, db=Depends(get_db_session)):

        api_response = APIResponse()
        try:
            query = select(DatasetSchemaTemplate).where(DatasetSchemaTemplate.geid == template_geid)
            if dataset_geid == 'default':
                query = query.where(DatasetSchemaTemplate.system_defined.is_(True))
            else:
                query = query.where(DatasetSchemaTemplate.dataset_geid == dataset_geid)
            result = (await db.execute(query)).scalars().one()

            api_response.result = result.to_dict()
        except NoResultFound:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'template %s is not found' % template_geid
            await db.rollback()
        except Exception as e:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = str(e)

        return api_response.json_response()

    @router.put(
        '/dataset/{dataset_geid}/schemaTPL/{template_geid}',
        tags=[_API_TAG],
        summary='API will create the new schema template',
    )
    @catch_internal(_API_NAMESPACE)
    async def update_schema_template(
        self, template_geid, dataset_geid, request_payload: SchemaTemplatePut, db=Depends(get_db_session)
    ):

        api_response = APIResponse()

        # here we enforce the uniqueness of the name with in dataset_geid
        exist = await check_template_name(request_payload.name, dataset_geid, db)
        if exist:
            api_response.code = EAPIResponseCode.forbidden
            api_response.error_msg = 'The template name already exists.'
            return api_response, EAPIResponseCode.forbidden

        try:
            query = (
                select(DatasetSchemaTemplate)
                .where(DatasetSchemaTemplate.geid == template_geid)
                .where(DatasetSchemaTemplate.dataset_geid == dataset_geid)
            )
            result = (await db.execute(query)).scalars().one()

            # update the row if we find it
            result.name = request_payload.name
            result.content = request_payload.content
            result.is_draft = request_payload.is_draft
            await db.commit()
            api_response.result = result.to_dict()

            # based on the frontend infomation, create the log activity
            activities = request_payload.activity
            for act in activities:
                await self.__activity_manager.send_schema_template_on_update_event(
                    dataset_geid, template_geid, result.creator, act.get('action'), act.get('detail', {})
                )
        except NoResultFound:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'template %s is not found' % template_geid
            await db.rollback()
        except Exception as e:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = str(e)
            await db.rollback()

        return api_response.json_response()

    @router.delete(
        '/dataset/{dataset_geid}/schemaTPL/{template_geid}',
        tags=[_API_TAG],
        summary='API will create the new schema template',
    )
    @catch_internal(_API_NAMESPACE)
    async def remove_schema_template(self, dataset_geid, template_geid, db=Depends(get_db_session)):

        api_response = APIResponse()

        # delete the row if we find it
        try:
            query = select(DatasetSchemaTemplate).where(DatasetSchemaTemplate.geid == template_geid)
            result = (await db.execute(query)).scalars().one()
            await db.delete(result)
            await db.commit()
            api_response.result = result.to_dict()

            # create the log activity
            await self.__activity_manager.send_schema_template_on_delete_event(
                result.dataset_geid, template_geid, result.creator, result.name
            )

        except NoResultFound:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = 'template %s is not found' % template_geid
            await db.rollback()
        except Exception as e:
            api_response.code = EAPIResponseCode.bad_request
            api_response.error_msg = str(e)
            await db.rollback()

        return api_response.json_response()
