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
# import io
from typing import Any
from typing import Dict
from uuid import uuid4

import httpx
from common import LoggerFactory

from app.config import ConfigClass
from app.schemas.activity_log import DatasetActivityLogSchema
from app.schemas.activity_log import ItemActivityLogSchema

# from fastavro import schema
# from fastavro import schemaless_writer


# from aiokafka import AIOKafkaProducer


class ActivityLogService:

    logger = LoggerFactory('ActivityLogService').get_logger()
    queue_url = ConfigClass.QUEUE_SERVICE + 'broker/pub'

    async def _message_send(
        self,
        geid: str,
        operator: str,
        action: str,
        event_type: str,
        detail: dict,
        resource: str = 'Dataset',
        extra: Dict[str, Any] = None,
    ) -> dict:

        msg_dict = {
            'event_type': event_type,
            'payload': {
                'dataset_geid': geid,
                'act_geid': str(uuid4()),
                'operator': operator,
                'action': action,
                'resource': resource,
                'detail': detail,
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        if extra:
            msg_dict['payload'].update(**extra)

        # self.aioproducer = AIOKafkaProducer(bootstrap_servers=[ConfigClass.KAFKA_URL])
        # self.logger.info('Sending socket notification: ' + str(msg_dict))
        # loaded_schema = schema.load_schema(self.avro_schema_path)
        # msg_bytes = io.BytesIO(msg_dict)
        # msg = schemaless_writer(msg_bytes, loaded_schema)
        # try:
        #     self.aioproducer.start()
        #     await self.aioproducer.send(self.topic, msg)
        # finally:
        #     self.aioproducer.stop()

        async with httpx.AsyncClient() as client:
            res = await client.post(self.queue_url, json=msg_dict)
        if res.status_code != 200:
            error_msg = 'on_{}_event {}: {}'.format(event_type, res.status_code, res.text)
            self.logger.error(error_msg)
            raise Exception(error_msg)
        return res.json()


class FileFolderActivityLogService(ActivityLogService):

    logger = LoggerFactory('ActivityLogService').get_logger()
    log_schema = ItemActivityLogSchema
    topic = 'items-activity-logs'
    avro_schema_path = 'app/schemas/metadata.items.activity.avsc'

    async def on_import_event(self, geid, username, source_list, project='', project_code=''):
        detail = {
            'source_list': source_list,  # list of file name
            'project': project,
            'project_code': project_code,
        }
        return await self._message_send(geid, username, 'ADD', 'DATASET_FILE_IMPORT_SUCCEED', detail)

    async def on_delete_event(self, geid, username, source_list):

        detail = {'source_list': source_list}  # list of file name
        return await self._message_send(geid, username, 'REMOVE', 'DATASET_FILE_DELETE_SUCCEED', detail)

    # this function will be per file/folder since the batch display
    # is not human readable
    async def on_move_event(self, geid, username, source, target):

        detail = {'from': source, 'to': target}
        return await self._message_send(geid, username, 'MOVE', 'DATASET_FILE_MOVE_SUCCEED', detail)

    async def on_rename_event(self, geid, username, source, target):

        detail = {'from': source, 'to': target}
        return await self._message_send(geid, username, 'UPDATE', 'DATASET_FILE_RENAME_SUCCEED', detail)


class DatasetActivityLogService(ActivityLogService):

    logger = LoggerFactory('ActivityLogService').get_logger()
    log_schema = DatasetActivityLogSchema
    topic = 'datasets-activity-logs'
    avro_schema_path = 'app/schemas/dataset.activity.avsc'

    async def send_schema_create_event(self, activity_data: Dict[str, Any]):
        return await self._message_send(
            activity_data['dataset_geid'],
            activity_data['username'],
            'CREATE',
            'SCHEMA_CREATE',
            activity_data['detail'],
        )

    async def send_schema_update_event(self, activity_data: Dict[str, Any]):
        return await self._message_send(
            activity_data['dataset_geid'],
            activity_data['username'],
            'UPDATE',
            'SCHEMA_UPDATE',
            activity_data['detail'],
        )

    async def send_schema_delete_event(self, activity_data: Dict[str, Any]):
        return await self._message_send(
            activity_data['dataset_geid'],
            activity_data['username'],
            'REMOVE',
            'SCHEMA_DELETE',
            activity_data['detail'],
        )

    async def send_schema_template_on_create_event(self, dataset_geid, template_geid, username, template_name):
        return await self._message_send(
            dataset_geid,
            username,
            'CREATE',
            'DATASET_SCHEMA_TEMPLATE_CREATE',
            {'name': template_name},
            'Dataset.Schema.Template',
            extra={'schema_template_geid': template_geid},
        )

    async def send_publish_version_succeed(self, dataset_schema):
        return await self._message_send(
            dataset_schema.dataset_geid,
            dataset_schema.operator,
            'PUBLISH',
            'DATASET_PUBLISH_SUCCEED',
            {'source': dataset_schema.version},
        )

    async def send_schema_template_on_update_event(
        self, dataset_geid, template_geid, username, attribute_action, attributes
    ):
        return await self._message_send(
            dataset_geid,
            username,
            'UPDATE',
            'DATASET_SCHEMA_TEMPLATE_UPDATE',
            attributes,
            'Dataset.Schema.Template.Attributes',
            extra={'schema_template_geid': template_geid},
        )

    async def send_schema_template_on_delete_event(self, dataset_geid, template_geid, username, template_name):
        return await self._message_send(
            dataset_geid,
            username,
            'REMOVE',
            'DATASET_SCHEMA_TEMPLATE_DELETE',
            {'name': template_name},
            'Dataset.Schema.Template',
            extra={'schema_template_geid': template_geid},
        )

    async def send_dataset_on_create_event(self, geid, username):
        return await self._message_send(geid, username, 'CREATE', 'DATASET_CREATE_SUCCEED', {'source': geid})
