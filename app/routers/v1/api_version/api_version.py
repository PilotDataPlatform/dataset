from fastapi import APIRouter, BackgroundTasks, Depends
from fastapi_utils import cbv
from fastapi_sqlalchemy import db

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.models.base_models import APIResponse, EAPIResponseCode
from app.config import ConfigClass
from app.models.version_models import PublishResponse, PublishRequest, VersionResponse, \
        VersionRequest, VersionListRequest
from app.models.version_sql import DatasetVersion
from app.resources.error_handler import APIException
from app.resources.token_manager import generate_token
from .publish_version import PublishVersion, get_dataset_by_geid, parse_minio_location

from redis import Redis
import requests
import time
import json
import re
import math

logger = SrvLoggerFactory("api_version").get_logger()
router = APIRouter()


@cbv.cbv(router)
class VersionAPI:
    @router.post("/v1/dataset/{dataset_geid}/publish", tags=["version"], response_model=PublishResponse, summary="Publish a dataset version")
    async def publish(self, dataset_geid: str, data: PublishRequest, background_tasks: BackgroundTasks):
        api_response = PublishResponse()
        if len(data.notes) > 250:
            api_response.result = "Notes is to large, limit 250 bytes"
            api_response.code = EAPIResponseCode.bad_request
            return api_response.json_response()

        version_format = re.compile("^\d+\.\d+$")
        if not version_format.match(data.version):
            api_response.result = "Incorrect version format"
            api_response.code = EAPIResponseCode.bad_request
            return api_response.json_response()

        # Check if publish is already running
        self.redis_client = Redis(
            host=ConfigClass.REDIS_HOST,
            port=ConfigClass.REDIS_PORT,
            password=ConfigClass.REDIS_PASSWORD,
            db=ConfigClass.REDIS_DB,
        )
        # TODO why here we block the double publish???
        status = self.redis_client.get(dataset_geid)
        if status:
            status = json.loads(status)["status"]
            if status == "inprogress":
                api_response.result = "Dataset is inprogress of publishing"
                api_response.code = EAPIResponseCode.bad_request
                return api_response.json_response()

        # Duplicate check
        try:
            versions = db.session.query(DatasetVersion).filter_by(
                dataset_geid=dataset_geid,
                version=data.version
            ).order_by(DatasetVersion.created_at.desc())
        except Exception as e:
            logger.error("Psql Error: " + str(e))
            api_response.code = EAPIResponseCode.internal_error
            api_response.result = "Psql Error: " + str(e)
            return api_response.json_response()

        if versions.first():
            api_response.code = EAPIResponseCode.conflict
            api_response.result = "Duplicate version found for dataset"
            return api_response.json_response()

        dataset_node = get_dataset_by_geid(dataset_geid)
        client = PublishVersion(
            dataset_node=dataset_node,
            operator=data.operator,
            notes=data.notes,
            status_id=dataset_geid,
            version=data.version
        )
        background_tasks.add_task(client.publish)

        api_response.result = {
            "status_id": dataset_geid 
        }
        return api_response.json_response()

    @router.get("/v1/dataset/{dataset_geid}/publish/status", tags=["version"], response_model=PublishResponse, summary="Publish status")
    async def publish_status(self, dataset_geid: str, status_id: str):
        api_response = APIResponse()

        dataset_node = get_dataset_by_geid(dataset_geid)
        self.redis_client = Redis(
            host=ConfigClass.REDIS_HOST,
            port=ConfigClass.REDIS_PORT,
            password=ConfigClass.REDIS_PASSWORD,
            db=ConfigClass.REDIS_DB,
        )
        status = self.redis_client.get(status_id)
        if not status:
            raise APIException(status_code=404, error_msg="Status not found")
        api_response.result = json.loads(status)
        return api_response.json_response()

    @router.get("/v1/dataset/{dataset_geid}/versions", tags=["version"], response_model=VersionResponse, summary="Get dataset versions")
    async def version(self, dataset_geid: str, data: VersionListRequest = Depends(VersionListRequest)):
        api_response = VersionResponse()
        try:
            versions = db.session.query(DatasetVersion).filter_by(
                dataset_geid=dataset_geid
            ).order_by(DatasetVersion.created_at.desc())
            total = versions.count()
            versions = versions.offset(data.page * data.page_size).limit(data.page_size)
        except Exception as e:
            logger.error("Psql Error: " + str(e))
            api_response.code = EAPIResponseCode.internal_error
            api_response.result = "Psql Error: " + str(e)
            return api_response.json_response()
        results = [v.to_dict() for v in versions]
        api_response.result = results
        api_response.page = data.page
        api_response.total = total 
        api_response.num_of_pages = math.ceil(total / data.page_size)
        return api_response.json_response()

    @router.delete("/v1/dataset/{dataset_geid}/version/{version_id}", tags=["version"], summary="Only used for unit tests, delete a version from psql")
    async def delete_version(self, dataset_geid: str, version_id: str):
        api_response = APIResponse()
        try:
            version = db.session.query(DatasetVersion).get(version_id)
            db.session.delete(version)
            db.session.commit()
        except Exception as e:
            logger.error("Psql Error: " + str(e))
            api_response.code = EAPIResponseCode.internal_error
            api_response.result = "Psql Error: " + str(e)
            return api_response.json_response()
        api_response.result = "success"
        return api_response.json_response()


    @router.get("/v1/dataset/{dataset_geid}/download/pre", tags=["version"], response_model=VersionResponse, summary="Download dataset version")
    async def download_url(self, dataset_geid: str, version: str = ""):
        """
            Get download url for dataset version
        """
        api_response = APIResponse()
        dataset_node = get_dataset_by_geid(dataset_geid)
        try:
            if version:
                query = {
                    "dataset_geid": dataset_geid,
                    "version": version,
                }
            else:
                query = {
                    "dataset_geid": dataset_geid,
                }
            versions = db.session.query(DatasetVersion).filter_by(**query).order_by(DatasetVersion.created_at.desc())
        except Exception as e:
            logger.error("Psql Error: " + str(e))
            api_response.code = EAPIResponseCode.internal_error
            api_response.result = "Psql Error: " + str(e)
            return api_response.json_response()
        dataset_version = versions.first()
        if not dataset_version:
            api_response.code = EAPIResponseCode.not_found
            api_response.error_msg = "No published version found"
            return api_response.json_response()
        token_data = {
            "location": dataset_version.location,
            "expiry": int(time.time()) + ConfigClass.DOWNLOAD_TOKEN_EXPIRE_AT * 60,
        }
        token = generate_token(token_data)
        api_response.result = {"download_hash": token}
        return api_response.json_response()
