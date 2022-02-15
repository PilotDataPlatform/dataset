from fastapi import APIRouter
from fastapi_utils import cbv
from requests.api import post
from ...commons.logger_services.logger_factory_service import SrvLoggerFactory
from ...resources.error_handler import catch_internal
from ...models.base_models import APIResponse, EAPIResponseCode
from ...models.reqres_dataset import DatasetListForm, DatasetListResponse
from ...config import ConfigClass
import requests
import math

router = APIRouter()

_API_TAG = 'V1 Dataset List API'
_API_NAMESPACE = "api_dataset_list"

@cbv.cbv(router)
class DatasetList:
    '''
    API Dataset List
    '''
    def __init__(self):
        self.__logger = SrvLoggerFactory(_API_NAMESPACE).get_logger()

    @router.post("/v1/users/{username}/datasets", tags=[_API_TAG], response_model=DatasetListResponse,
                summary="list datasets.")
    @catch_internal(_API_NAMESPACE)
    async def list_dataset(self, username, request_payload: DatasetListForm):
        '''
        dataset creation api
        '''
        res = APIResponse()
        # post_dict = request_payload.dict()
        filter = request_payload.filter
        page = request_payload.page
        page_size = request_payload.page_size
        creator = username
        filter['creator'] = creator

        page_kwargs = {
            "order_by": request_payload.order_by,
            "order_type": request_payload.order_type,
            "skip": page * page_size,
            "limit": page_size
        }
        
        relation_payload = {
            **page_kwargs,
            "start_label": "User",
            "end_labels": ["Dataset"],
            "query": {
                "start_params": {"username": creator},
                "end_params": {},
            },
        }

        try:
            response = requests.post(ConfigClass.NEO4J_SERVICE_V2 + "relations/query", json=relation_payload)
            if response.status_code != 200:
                error_msg = response.json()
                res.code = EAPIResponseCode.internal_error
                res.error_msg = f"Neo4j error: {error_msg}"
                return res.json_response()
            nodes = response.json()
        except Exception as e:
            res.code = EAPIResponseCode.internal_error
            res.error_msg = "Neo4j error: " + str(e)
            return res.json_response()

        res.code = EAPIResponseCode.success
        res.total = nodes['total']
        res.page = page
        res.num_of_pages = math.ceil(res.total / page_size)
        res.result = nodes['results']
        return res.json_response()
