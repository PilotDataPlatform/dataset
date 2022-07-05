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
import io
from typing import Any
from typing import Dict
from typing import List
from uuid import UUID

from common import LoggerFactory
from fastavro import schema
from fastavro import schemaless_writer

from app.clients.kafka import get_kafka_client
from app.config import ConfigClass
from app.models.dataset import Dataset
from app.models.schema import DatasetSchema
from app.models.schema import DatasetSchemaTemplate
from app.models.version import DatasetVersion
from app.schemas.activity_log import DatasetActivityLogSchema
from app.schemas.activity_log import FileFolderActivityLogSchema


class ActivityLogService:

    logger = LoggerFactory('ActivityLogService').get_logger()
    queue_url = ConfigClass.QUEUE_SERVICE + 'broker/pub'

    async def _message_send(self, data: Dict[str, Any] = None) -> dict:
        self.logger.info('Sending socket notification: ' + str(data))
        loaded_schema = schema.load_schema(self.avro_schema_path)
        bio = io.BytesIO()
        try:
            schemaless_writer(bio, loaded_schema, data)
            msg = bio.getvalue()
        except ValueError as e:
            self.logger.exception('error during the AVRO validation', extra={'error_msg': str(e)})
        client = await get_kafka_client()
        await client.send(self.topic, msg)


class FileFolderActivityLogService(ActivityLogService):

    logger = LoggerFactory('ActivityLogService').get_logger()
    topic = 'items-activity-logs'
    avro_schema_path = 'app/schemas/metadata.items.activity.avsc'

    async def send_on_import_event(
        self, dataset: Dataset, project: Dict[str, Any], imported_list: List[str], user: str
    ):
        for item in imported_list:
            log_schema = FileFolderActivityLogSchema(
                container_code=dataset.code,
                user=user,
                activity_type='import',
                item_id=UUID(item['id']),
                item_type=item['type'],
                item_name=item['name'],
                imported_from=project['code'],
            )
            await self._message_send(log_schema.dict())

    async def send_on_delete_event(self, dataset: Dataset, source_list: List[str], user: str):
        for item in source_list:
            log_schema = FileFolderActivityLogSchema(
                container_code=dataset.code,
                user=user,
                activity_type='delete',
                item_parent_path=item['parent_path'] or '',
                item_id=UUID(item['id']),
                item_type=item['type'],
                item_name=item['name'],
                changes=[{'source_list': item['name']}],
            )
            await self._message_send(log_schema.dict())

    async def send_on_move_event(self, dataset: Dataset, item: Dict[str, Any], user: str, old_path: str, new_path: str):
        # even thought the event is UPDATE there is no update in metadata service.
        # as of today, when one item is moved, the item is deleted and a new one is created in the new path.
        log_schema = FileFolderActivityLogSchema(
            container_code=dataset.code,
            user=user,
            activity_type='update',
            item_parent_path=item['parent_path'] or '',
            item_id=UUID(item['id']),
            item_type=item['type'],
            item_name=item['name'],
            changes=[{'item_property': 'parent_path', 'old_value': old_path, 'new_value': new_path}],
        )
        await self._message_send(log_schema.dict())

    async def send_on_rename_event(self, dataset: Dataset, source_list: List[str], user: str, new_name: str):
        # even thought the event is UPDATE there is no update in metadata service.
        # as of today, when one item is moved, the item is deleted and a new one is created in the new name.
        for item in source_list:
            log_schema = FileFolderActivityLogSchema(
                container_code=dataset.code,
                user=user,
                activity_type='update',
                item_parent_path=item['parent_path'] or '',
                item_id=UUID(item['id']),
                item_type=item['type'],
                item_name=item['name'],
                changes=[{'item_property': 'name', 'old_value': item['name'], 'new_value': new_name}],
            )
            await self._message_send(log_schema.dict())


class DatasetActivityLogService(ActivityLogService):

    logger = LoggerFactory('ActivityLogService').get_logger()
    log_schema = DatasetActivityLogSchema
    topic = 'datasets-activity-logs'
    avro_schema_path = 'app/schemas/dataset.activity.avsc'

    async def send_dataset_on_create_event(self, dataset: Dataset):
        log_schema = DatasetActivityLogSchema(activity_type='create', container_code=dataset.code, user=dataset.creator)

        return await self._message_send(log_schema.dict())

    async def send_publish_version_succeed(self, version: DatasetVersion, dataset: Dataset):
        log_schema = DatasetActivityLogSchema(
            activity_type='release', version=version.version, container_code=dataset.code, user=version.created_by
        )

        return await self._message_send(log_schema.dict())

    async def send_schema_create_event(self, schema: DatasetSchema, dataset: Dataset, username: str):
        log_schema = DatasetActivityLogSchema(
            activity_type='schema_create', container_code=dataset.code, user=username, target_name=schema.name
        )
        return await self._message_send(log_schema.dict())

    async def send_schema_update_event(
        self, schema: DatasetSchema, dataset: Dataset, username: str, changes: List[Dict[str, Any]] = None
    ):
        log_schema = DatasetActivityLogSchema(
            activity_type='schema_update',
            container_code=dataset.code,
            user=username,
            target_name=schema.name,
            changes=changes,
        )
        return await self._message_send(log_schema.dict())

    async def send_schema_delete_event(self, schema: DatasetSchema, dataset: Dataset, username: str):
        log_schema = DatasetActivityLogSchema(
            activity_type='schema_delete',
            container_code=dataset.code,
            user=username,
            target_name=schema.name,
        )
        return await self._message_send(log_schema.dict())

    async def send_schema_template_on_create_event(self, schema_template: DatasetSchemaTemplate, dataset: Dataset):
        log_schema = DatasetActivityLogSchema(
            activity_type='template_create',
            container_code=dataset.code,
            user=schema_template.creator,
            target_name=schema_template.name,
        )
        return await self._message_send(log_schema.dict())

    async def send_schema_template_on_update_event(
        self, schema_template: DatasetSchemaTemplate, dataset: Dataset, changes: List[Dict[str, Any]] = None
    ):
        log_schema = DatasetActivityLogSchema(
            activity_type='template_update',
            container_code=dataset.code,
            user=schema_template.creator,
            target_name=schema_template.name,
            changes=changes,
        )
        return await self._message_send(log_schema.dict())

    async def send_schema_template_on_delete_event(self, schema_template: DatasetSchemaTemplate, dataset: Dataset):
        log_schema = DatasetActivityLogSchema(
            activity_type='template_delete',
            container_code=dataset.code,
            user=schema_template.creator,
            target_name=schema_template.name,
        )
        return await self._message_send(log_schema.dict())
