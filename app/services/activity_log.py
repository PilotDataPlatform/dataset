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

    async def _message_send(self, geid: str, operator: str, action: str, event_type: str, detail: dict) -> dict:

        post_json = {
            'event_type': event_type,
            'payload': {
                'dataset_geid': geid,
                'act_geid': str(uuid4()),
                'operator': operator,
                'action': action,
                'resource': 'Dataset',
                'detail': detail,
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        self.logger.info('Sending socket notification: ' + str(post_json))
        async with httpx.AsyncClient() as client:
            res = await client.post(self.queue_url, json=post_json)
        if res.status_code != 200:
            raise Exception('on_{}_event {}: {}'.format(event_type, res.status_code, res.text))
        return res.json()

    async def send_schema_log(self, activity_data):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': activity_data['event_type'],
            'payload': {
                'dataset_geid': activity_data['dataset_geid'],
                'act_geid': str(uuid4()),
                'operator': activity_data['username'],
                'action': activity_data['action'],
                'resource': activity_data['resource'],
                'detail': activity_data['detail'],
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            self.logger.error(error_msg)
            raise Exception(error_msg)
        return res

    async def send_publish_version_succeed(self, dataset_schema):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_PUBLISH_SUCCEED',
            'payload': {
                'dataset_geid': dataset_schema.dataset_geid,
                'act_geid': str(uuid4()),
                'operator': dataset_schema.operator,
                'action': 'PUBLISH',
                'resource': 'Dataset',
                'detail': {'source': dataset_schema.version},
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            error_msg = 'update_activity_log {}: {}'.format(res.status_code, res.text)
            self.logger.error(error_msg)
            raise Exception(error_msg)
        return res

    async def send_schema_template_on_create_event(self, dataset_geid, template_geid, username, template_name):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_SCHEMA_TEMPLATE_CREATE',
            'payload': {
                'dataset_geid': dataset_geid,  # None if it is default template
                'schema_template_geid': template_geid,
                'act_geid': str(uuid4()),
                'operator': username,
                'action': 'CREATE',
                'resource': 'Dataset.Schema.Template',
                'detail': {'name': template_name},  # list of file name
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res

    async def send_schema_template_on_update_event(
        self, dataset_geid, template_geid, username, attribute_action, attributes
    ):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_SCHEMA_TEMPLATE_UPDATE',
            'payload': {
                'dataset_geid': dataset_geid,  # None if it is default template
                'schema_template_geid': template_geid,
                'act_geid': str(uuid4()),
                'operator': username,
                'action': attribute_action,
                'resource': 'Dataset.Schema.Template.Attributes',
                'detail': attributes,
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res

    async def send_schema_template_on_delete_event(self, dataset_geid, template_geid, username, template_name):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_SCHEMA_TEMPLATE_DELETE',
            'payload': {
                'dataset_geid': dataset_geid,  # None if it is default template
                'schema_template_geid': template_geid,  # None if the
                'act_geid': str(uuid4()),
                'operator': username,
                'action': 'REMOVE',
                'resource': 'Dataset.Schema.Template',
                'detail': {'name': template_name},  # list of file name
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res

    async def send_dataset_on_create_event(self, geid, username):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_CREATE_SUCCEED',
            'payload': {
                'dataset_geid': geid,
                'act_geid': str(uuid4()),
                'operator': username,
                'action': 'CREATE',
                'resource': 'Dataset',
                'detail': {'source': geid},
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        async with httpx.AsyncClient() as client:
            res = await client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_create_event {}: {}'.format(res.status_code, res.text))
        return res
