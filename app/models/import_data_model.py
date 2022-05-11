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
from common import LoggerFactory
from pydantic import BaseModel

from app.config import ConfigClass


class ImportDataPost(BaseModel):
    """the post request payload for import data from project."""

    source_list: list
    operator: str
    project_geid: str


class DatasetFileDelete(BaseModel):
    """the delete request payload for dataset to delete files."""

    source_list: list
    operator: str


class DatasetFileMove(BaseModel):
    """the post request payload for dataset to move files under the dataset."""

    source_list: list
    operator: str
    target_geid: str


class DatasetFileRename(BaseModel):
    """the post request payload for dataset to move files under the dataset."""

    new_name: str
    operator: str


######################################################################
class SrvDatasetFileMgr:

    logger = LoggerFactory('SrvDatasetFileMgr').get_logger()
    queue_url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
    geid_client = GEIDClient()

    event_action_map = {
        'DATASET_FILE_IMPORT': 'ADD',
        'DATASET_FILE_DELETE': 'REMOVE',
        'DATASET_FILE_MOVE': 'MOVE',
        'DATASET_FILE_RENAME': 'UPDATE',
    }

    def on_import_event(self, geid, username, source_list, project='', project_code=''):
        detail = {
            'source_list': source_list,  # list of file name
            'project': project,
            'project_code': project_code,
        }
        event_type = 'DATASET_FILE_IMPORT'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = self._message_send(geid, username, action, message_event, detail)

        return res

    def on_delete_event(self, geid, username, source_list):

        detail = {'source_list': source_list}  # list of file name
        event_type = 'DATASET_FILE_DELETE'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = self._message_send(geid, username, action, message_event, detail)

        return res

    # this function will be per file/folder since the batch display
    # is not human readable
    def on_move_event(self, geid, username, source, target):

        detail = {'from': source, 'to': target}
        event_type = 'DATASET_FILE_MOVE'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = self._message_send(geid, username, action, message_event, detail)

        return res

    def on_rename_event(self, geid, username, source, target):

        detail = {'from': source, 'to': target}
        event_type = 'DATASET_FILE_RENAME'
        action = self.event_action_map.get(event_type)
        message_event = event_type + '_SUCCEED'
        res = self._message_send(geid, username, action, message_event, detail)

        return res

    def _message_send(self, geid: str, operator: str, action: str, event_type: str, detail: dict) -> dict:
        post_json = {
            'event_type': event_type,
            'payload': {
                'dataset_geid': geid,
                'act_geid': self.geid_client.get_GEID(),
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
        with httpx.Client() as client:
            res = client.post(self.queue_url, json=post_json)
        if res.status_code != 200:
            raise Exception('on_{}_event {}: {}'.format(event_type, res.status_code, res.text))
        return res.json()
