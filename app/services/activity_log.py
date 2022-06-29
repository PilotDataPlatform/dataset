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

from typing import Any
from typing import Dict
from uuid import uuid4

import httpx
from common import LoggerFactory

from app.config import ConfigClass


class ActivityLogService:

    logger = LoggerFactory('ActivityLogService').get_logger()
    queue_url = ConfigClass.QUEUE_SERVICE + 'broker/pub'

    event_action_map = {
        'DATASET_FILE_IMPORT': 'ADD',
        'DATASET_FILE_DELETE': 'REMOVE',
        'DATASET_FILE_MOVE': 'MOVE',
        'DATASET_FILE_RENAME': 'UPDATE',
    }

    async def on_import_event(self, geid, username, source_list, project='', project_code=''):
        detail = {
            'source_list': source_list,  # list of file name
            'project': project,
            'project_code': project_code,
        }
        event_type = 'DATASET_FILE_IMPORT'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = await self._message_send(geid, username, action, message_event, detail)

        return res

    async def on_delete_event(self, geid, username, source_list):

        detail = {'source_list': source_list}  # list of file name
        event_type = 'DATASET_FILE_DELETE'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = await self._message_send(geid, username, action, message_event, detail)

        return res

    # this function will be per file/folder since the batch display
    # is not human readable
    async def on_move_event(self, geid, username, source, target):

        detail = {'from': source, 'to': target}
        event_type = 'DATASET_FILE_MOVE'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = await self._message_send(geid, username, action, message_event, detail)

        return res

    async def on_rename_event(self, geid, username, source, target):

        detail = {'from': source, 'to': target}
        event_type = 'DATASET_FILE_RENAME'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = await self._message_send(geid, username, action, message_event, detail)

        return res

    async def send_schema_log(self, activity_data):
        return await self._message_send(
            activity_data['dataset_geid'],
            activity_data['username'],
            activity_data['action'],
            activity_data['event_type'],
            activity_data['detail'],
        )

    async def send_publish_version_succeed(self, dataset_schema):
        return await self._message_send(
            dataset_schema.dataset_geid,
            dataset_schema.operator,
            'PUBLISH',
            'DATASET_PUBLISH_SUCCEED',
            {'source': dataset_schema.version},
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

    async def send_schema_template_on_update_event(
        self, dataset_geid, template_geid, username, attribute_action, attributes
    ):
        return await self._message_send(
            dataset_geid,
            username,
            attribute_action,
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

        post_json = {
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
            post_json['payload'].update(**extra)
        self.logger.info('Sending socket notification: ' + str(post_json))
        async with httpx.AsyncClient() as client:
            res = await client.post(self.queue_url, json=post_json)
        if res.status_code != 200:
            error_msg = 'on_{}_event {}: {}'.format(event_type, res.status_code, res.text)
            self.logger.error(error_msg)
            raise Exception(error_msg)
        return res.json()
