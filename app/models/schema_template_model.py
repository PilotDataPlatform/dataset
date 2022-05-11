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


class SchemaTemplatePost(BaseModel):
    """the post request payload for import data from project."""

    name: str
    standard: str
    system_defined: bool
    is_draft: bool
    content: dict
    creator: str


class SchemaTemplatePut(BaseModel):
    name: str
    is_draft: bool
    content: dict
    activity: list


class SchemaTemplateList(BaseModel):
    # dataset_geid : Optional[str] = None
    pass


class SrvDatasetSchemaTemplateMgr:

    logger = LoggerFactory('SrvDatasetSchemaTemplateMgr').get_logger()
    geid_client = GEIDClient()

    def on_create_event(self, dataset_geid, template_geid, username, template_name):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_SCHEMA_TEMPLATE_CREATE',
            'payload': {
                'dataset_geid': dataset_geid,  # None if it is default template
                'schema_template_geid': template_geid,
                'act_geid': self.geid_client.get_GEID(),
                'operator': username,
                'action': 'CREATE',
                'resource': 'Dataset.Schema.Template',
                'detail': {'name': template_name},  # list of file name
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        with httpx.Client() as client:
            res = client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res

    # this will adapt to add/delete the attributes
    def on_update_event(self, dataset_geid, template_geid, username, attribute_action, attributes):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_SCHEMA_TEMPLATE_UPDATE',
            'payload': {
                'dataset_geid': dataset_geid,  # None if it is default template
                'schema_template_geid': template_geid,
                'act_geid': self.geid_client.get_GEID(),
                'operator': username,
                'action': attribute_action,
                'resource': 'Dataset.Schema.Template.Attributes',
                'detail': attributes,
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        with httpx.Client() as client:
            res = client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res

    def on_delete_event(self, dataset_geid, template_geid, username, template_name):
        url = ConfigClass.QUEUE_SERVICE + 'broker/pub'
        post_json = {
            'event_type': 'DATASET_SCHEMA_TEMPLATE_DELETE',
            'payload': {
                'dataset_geid': dataset_geid,  # None if it is default template
                'schema_template_geid': template_geid,  # None if the
                'act_geid': self.geid_client.get_GEID(),
                'operator': username,
                'action': 'REMOVE',
                'resource': 'Dataset.Schema.Template',
                'detail': {'name': template_name},  # list of file name
            },
            'queue': 'dataset_actlog',
            'routing_key': '',
            'exchange': {'name': 'DATASET_ACTS', 'type': 'fanout'},
        }
        with httpx.Client() as client:
            res = client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res
