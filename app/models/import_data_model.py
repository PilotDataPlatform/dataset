from pydantic import BaseModel, Field
import requests

from ..config import ConfigClass
from ..commons.logger_services.logger_factory_service import SrvLoggerFactory

class ImportDataPost(BaseModel):
    '''
    the post request payload for import data from project
    '''
    source_list: list
    operator: str
    project_geid: str


class DatasetFileDelete(BaseModel):
    '''
    the delete request payload for dataset to delete files
    '''
    source_list: list
    operator: str


class DatasetFileMove(BaseModel):
    '''
    the post request payload for dataset to move files
    under the dataset
    '''
    source_list: list
    operator: str
    target_geid: str


class DatasetFileRename(BaseModel):
    '''
    the post request payload for dataset to move files
    under the dataset
    '''
    new_name: str
    operator: str

######################################################################
class SrvDatasetFileMgr():

    logger = SrvLoggerFactory('SrvDatasetFileMgr').get_logger()
    queue_url = ConfigClass.QUEUE_SERVICE + "broker/pub"

    event_action_map = {
        "DATASET_FILE_IMPORT": "ADD",
        "DATASET_FILE_DELETE": "REMOVE",
        "DATASET_FILE_MOVE": "MOVE",
        "DATASET_FILE_RENAME": "UPDATE",
    }


    def on_import_event(self, geid, username, source_list):
        detial = {
            "source_list": source_list #list of file name
        }
        event_type = "DATASET_FILE_IMPORT"
        action = self.event_action_map.get(event_type)
        message_event = event_type+"_SUCCEED"
        res = self._message_send(geid, username, action, message_event, detial)

        return res


    def on_delete_event(self, geid, username, source_list):

        detial = {
            "source_list": source_list #list of file name
        }
        event_type = "DATASET_FILE_DELETE"
        action = self.event_action_map.get(event_type)
        message_event = event_type+"_SUCCEED"
        res = self._message_send(geid, username, action, message_event, detial)

        return res

    
    # this function will be per file/folder since the batch display
    # is not human readable
    def on_move_event(self, geid, username, source, target):

        detial = {
            "from": source,
            "to": target
        }
        event_type = "DATASET_FILE_MOVE"
        action = self.event_action_map.get(event_type)
        message_event = event_type+"_SUCCEED"
        res = self._message_send(geid, username, action, message_event, detial)

        return res


    def on_rename_event(self, geid, username, source, target):

        detial = {
            "from": source,
            "to": target
        }
        event_type = "DATASET_FILE_RENAME"
        action = self.event_action_map.get(event_type)
        message_event = event_type+"_SUCCEED"
        res = self._message_send(geid, username, action, message_event, detial)

        return res


    def _message_send(self, geid:str, operator:str, action:str, event_type:str, 
        detail:dict) -> dict:
        post_json = {
            "event_type": event_type,
            "payload": {
                "dataset_geid": geid,
                "act_geid": get_geid(),
                "operator": operator,
                "action": action,
                "resource": "Dataset",
                "detail": detail
            },
            "queue": "dataset_actlog",
            "routing_key": "",
            "exchange": {
                "name": "DATASET_ACTS",
                "type": "fanout"
            }
        }
        self.logger.info("Sending socket notification: "+str(post_json))
        res = requests.post(self.queue_url, json=post_json)
        if res.status_code != 200:
            raise Exception('on_{}_event {}: {}'.format(event_type, res.status_code, res.text))
        return res.json()



#########################################################################
def get_geid():
    '''
    get geid
    '''
    url = ConfigClass.COMMON_SERVICE + "utility/id"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['result']
    else:
        raise Exception('get_geid {}: {}'.format(response.status_code, url))
