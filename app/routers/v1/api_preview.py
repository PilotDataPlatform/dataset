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

import csv
from io import StringIO
from typing import Optional

import httpx
from fastapi import APIRouter
from fastapi import Header
from fastapi.responses import StreamingResponse
from fastapi_utils import cbv

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.commons.service_connection.minio_client import Minio_Client
from app.commons.service_connection.minio_client import Minio_Client_
from app.config import ConfigClass
from app.models.base_models import APIResponse
from app.models.base_models import EAPIResponseCode
from app.models.preview_model import PreviewResponse
from app.resources.error_handler import catch_internal

logger = SrvLoggerFactory('api_preview').get_logger()
router = APIRouter()


@cbv.cbv(router)
class Preview:
    """File Preview."""

    @router.get(
        '/v1/{file_geid}/preview', tags=['preview'], response_model=PreviewResponse, summary='CSV/JSON/TSV File preview'
    )
    @catch_internal('api_preview')
    async def get_preview(
        self, file_geid, Authorization: Optional[str] = Header(None), refresh_token: Optional[str] = Header(None)
    ):

        logger.info('Get preview for: ' + str(file_geid))
        api_response = APIResponse()

        # Get neo4j file node
        file_node = self.get_file_by_geid(file_geid)
        if not file_node:
            api_response.error_msg = 'File not found'
            api_response.code = EAPIResponseCode.not_found
            return api_response.json_response()

        result = {}
        mc = Minio_Client_(Authorization, refresh_token)
        file_data = self.parse_location(file_node['location'])
        file_type = file_node['name'].split('.')[1]

        response = mc.client.get_object(file_data['bucket'], file_data['path'], length=ConfigClass.MAX_PREVIEW_SIZE)
        if file_type in ['csv', 'tsv']:
            result['content'] = self.parse_csv_response(response.data.decode('utf-8-sig'))
        else:
            result['content'] = response.data.decode('utf-8-sig')

        if file_node['file_size'] >= ConfigClass.MAX_PREVIEW_SIZE:
            result['is_concatinated'] = True
        else:
            result['is_concatinated'] = False

        result['type'] = file_type
        api_response.result = result
        return api_response.json_response()

    @router.get('/v1/{file_geid}/preview/stream', tags=['preview'], summary='CSV/JSON/TSV File preview stream')
    def stream(self, file_geid):
        """Get a file preview."""

        logger.info('Get preview for: ' + str(file_geid))
        api_response = APIResponse()
        if not file_geid:
            logger.info('Missing file_geid')
            api_response.set_code(EAPIResponseCode.bad_request)
            api_response.set_result('file_geid is required')
            return api_response.to_dict, api_response.code

        # Get neo4j file node
        file_node = self.get_file_by_geid(file_geid)
        if not file_node:
            api_response.set_error_msg('File not found')
            api_response.set_code(EAPIResponseCode.not_found)
            return api_response.to_dict, api_response.code

        mc = Minio_Client()
        file_data = self.parse_location(file_node['location'])
        file_type = file_node['name'][file_node['name'].rfind('.') :].replace('.', '')

        response = mc.client.get_object(file_data['bucket'], file_data['path'])
        if file_type in ['csv', 'tsv']:
            mimetype = 'text/csv'
        else:
            mimetype = 'application/json'
        return StreamingResponse(response.stream(), media_type=mimetype)

    def parse_csv_response(self, csvdata):
        csv.field_size_limit(ConfigClass.MAX_PREVIEW_SIZE)
        csvfile = StringIO(csvdata)
        csv_out = StringIO()
        # detect csv format
        try:
            dialect = csv.Sniffer().sniff(csvfile.read(1024), [',', '|', ';', '\t'])
        except csv.Error:
            dialect = csv.excel
        csvfile.seek(0)
        reader = csv.reader(csvfile, dialect)
        writer = csv.writer(csv_out, delimiter=',')
        writer.writerows(reader)
        content = csv_out.getvalue()
        if len(content) >= ConfigClass.MAX_PREVIEW_SIZE:
            # Remove last line as it will be incomplete
            content = content[: content[:-1].rfind('\n')]
        return content

    def parse_location(self, path):
        # parse from format minio://<minio_host>/<zone>-<project_code>/<user>/<files>
        protocol = 'https://' if ConfigClass.MINIO_HTTPS else 'http://'
        path = path.replace('minio://', '').replace(protocol, '').split('/')
        bucket = path[1]
        path = '/'.join(path[2:])
        return {'bucket': bucket, 'path': path}

    def get_file_by_geid(self, file_geid):
        payload = {
            'global_entity_id': file_geid,
        }
        with httpx.Client() as client:
            response = client.post(ConfigClass.NEO4J_SERVICE + 'nodes/File/query', json=payload)
        if not response.json():
            return None
        return response.json()[0]
