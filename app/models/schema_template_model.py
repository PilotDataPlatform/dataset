from pydantic import BaseModel, Field
from typing import Optional
import requests

from app.resources.helpers import get_geid
from app.config import ConfigClass
from app.commons.logger_services.logger_factory_service import SrvLoggerFactory


class SchemaTemplatePost(BaseModel):
    '''
    the post request payload for import data from project
    '''
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


class SrvDatasetSchemaTemplateMgr():

    logger = SrvLoggerFactory('SrvDatasetSchemaTemplateMgr').get_logger()


    def on_create_event(self, dataset_geid, template_geid, username, template_name):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_SCHEMA_TEMPLATE_CREATE",
            "payload": {
                "dataset_geid": dataset_geid, # None if it is default template
                "schema_template_geid": template_geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": "CREATE",
                "resource": "Dataset.Schema.Template",
                "detail": {
                    "name": template_name #list of file name
                }
            },
            "queue": "dataset_actlog",
            "routing_key": "",
            "exchange": {
                "name": "DATASET_ACTS",
                "type": "fanout"
            }
        }
        res = requests.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res


    # this will adapt to add/delete the attributes
    def on_update_event(self, dataset_geid, template_geid, username, attribute_action, attributes):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_SCHEMA_TEMPLATE_UPDATE",
            "payload": {
                "dataset_geid": dataset_geid, # None if it is default template
                "schema_template_geid": template_geid,
                "act_geid": get_geid(),
                "operator": username,
                "action": attribute_action,
                "resource": "Dataset.Schema.Template.Attributes",
                "detail": attributes
            },
            "queue": "dataset_actlog",
            "routing_key": "",
            "exchange": {
                "name": "DATASET_ACTS",
                "type": "fanout"
            }
        }
        res = requests.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res


    def on_delete_event(self, dataset_geid, template_geid, username, template_name):
        url = ConfigClass.QUEUE_SERVICE + "broker/pub"
        post_json = {
            "event_type": "DATASET_SCHEMA_TEMPLATE_DELETE",
            "payload": {
                "dataset_geid": dataset_geid, # None if it is default template
                "schema_template_geid": template_geid, # None if the 
                "act_geid": get_geid(),
                "operator": username,
                "action": "REMOVE",
                "resource": "Dataset.Schema.Template",
                "detail": {
                    "name": template_name #list of file name
                }
            },
            "queue": "dataset_actlog",
            "routing_key": "",
            "exchange": {
                "name": "DATASET_ACTS",
                "type": "fanout"
            }
        }
        res = requests.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_import_event {}: {}'.format(res.status_code, res.text))
        return res