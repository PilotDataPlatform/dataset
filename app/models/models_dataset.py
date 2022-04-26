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

import json
import os
import time
from uuid import uuid4

import httpx
from common import GEIDClient
from fastapi_sqlalchemy import db
from minio.sseconfig import Rule
from minio.sseconfig import SSEConfig

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.commons.service_connection.dataset_policy_template import (
    create_dataset_policy_template,
)
from app.commons.service_connection.minio_client import Minio_Client
from app.config import ConfigClass
from app.models.schema_sql import DatasetSchema
from app.models.schema_sql import DatasetSchemaTemplate

ESSENTIALS_TPL_NAME = ConfigClass.ESSENTIALS_TPL_NAME
ESSENTIALS_NAME = ConfigClass.ESSENTIALS_NAME


class SrvDatasetMgr:
    """Service for Dataset Entity INFO Manager."""

    logger = SrvLoggerFactory('SrvDatasetMgr').get_logger()
    geid_client = GEIDClient()

    def create(
        self,
        username,
        code,
        title,
        authors,
        dataset_type,
        modality,
        collection_method,
        tags,
        dataset_license,
        description,
    ):
        """Create File Data Entity V2."""
        post_json_form = {
            'source': '',
            'title': title,
            'authors': authors,
            'code': code,
            'type': dataset_type,
            'modality': modality,
            'collection_method': collection_method,
            'license': dataset_license,
            'tags': tags,
            'description': description,
            'size': 0,
            'total_files': 0,
            'name': code,
            'creator': username,
        }
        self.logger.debug('SrvDatasetMgr post_json_form' + str(post_json_form))
        result_create_node = http_post_node(post_json_form)
        if result_create_node.status_code == 200:
            node_created = result_create_node.json()[0]
            global_entity_id = node_created['global_entity_id']
            self.__create_atlas_node(global_entity_id, username)
            self.__create_essentials(
                global_entity_id,
                code,
                title,
                authors,
                dataset_type,
                modality,
                collection_method,
                dataset_license,
                description,
                tags,
                username,
            )
            self.__on_create_event(global_entity_id, username)
            # and also create minio bucket with the dataset code
            try:
                mc = Minio_Client()
                mc.client.make_bucket(code)
                mc.client.set_bucket_encryption(code, SSEConfig(Rule.new_sse_s3_rule()))

                self.logger.info('createing the policy')
                # also use the lazy loading to create the policy in minio
                stream = os.popen('mc admin policy info minio %s' % (username))
                output = stream.read()
                policy_file_name = None
                try:
                    policy = json.loads(output)
                    # if there is a policy then we append the new to the resource
                    policy['Statement'][0]['Resource'].append('arn:aws:s3:::%s/*' % (code))
                    policy_file_name = create_dataset_policy_template(code, json.dumps(policy))
                except json.decoder.JSONDecodeError:
                    # if not found then we just create a new one for user
                    policy_file_name = create_dataset_policy_template(code)

                stream = os.popen('mc admin policy add minio %s %s' % (username, policy_file_name))
                output = stream.read()
                # then remove the policy file until the os is finish
                # otherwise there will be the racing issue
                os.remove(policy_file_name)

            except Exception as e:
                self.logger.error('error when creating minio: ' + str(e))
            return node_created
        else:
            raise Exception(str(result_create_node.text))

    def update(self, current_node, update_json):
        res_update_node = http_update_node('Dataset', current_node['id'], update_json)
        if res_update_node.status_code == 200:
            pass
        else:
            raise Exception(str(res_update_node.text))
        return res_update_node.json()[0]

    def get_bygeid(self, geid):
        payload = {'global_entity_id': geid}
        node_query_url = ConfigClass.NEO4J_SERVICE + 'nodes/Dataset/query'
        with httpx.Client() as client:
            response = client.post(node_query_url, json=payload)
        return response

    def get_bycode(self, code):
        payload = {'code': code}
        node_query_url = ConfigClass.NEO4J_SERVICE + 'nodes/Dataset/query'
        with httpx.Client() as client:
            response = client.post(node_query_url, json=payload)
        return response

    def __create_atlas_node(self, geid, username):
        res = create_atlas_dataset(geid, username)
        if res.status_code != 200:
            raise Exception('__create_atlas_node {}: {}'.format(res.status_code, res.text))
        return res

    def __create_essentials(
        self,
        dataset_geid,
        code,
        title,
        authors,
        dataset_type,
        modality,
        collection_method,
        dataset_license,
        description,
        tags,
        creator,
    ):
        def get_essential_tpl() -> DatasetSchemaTemplate:
            etpl_result = (
                db.session.query(DatasetSchemaTemplate).filter(DatasetSchemaTemplate.name == ESSENTIALS_TPL_NAME).all()
            )
            if not etpl_result:
                raise Exception('{} template not found in database.'.format(ESSENTIALS_TPL_NAME))
            etpl_result = etpl_result[0]
            return etpl_result

        etpl = get_essential_tpl()
        model_data = {
            'geid': self.geid_client.get_GEID(),
            'name': ESSENTIALS_NAME,
            'dataset_geid': dataset_geid,
            'tpl_geid': etpl.geid,
            'standard': etpl.standard,
            'system_defined': etpl.system_defined,
            'is_draft': False,
            'content': {
                'dataset_title': title,
                'dataset_authors': authors,
                'dataset_type': dataset_type,
                'dataset_modality': modality,
                'dataset_collection_method': collection_method,
                'dataset_license': dataset_license,
                'dataset_description': description,
                'dataset_tags': tags,
                'dataset_code': code,
            },
            'creator': creator,
        }
        schema = DatasetSchema(**model_data)
        schema = db_add_operation(schema)
        return schema.to_dict()

    def __on_create_event(self, geid, username):
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
        with httpx.Client() as client:
            res = client.post(url, json=post_json)
        if res.status_code != 200:
            raise Exception('__on_create_event {}: {}'.format(res.status_code, res.text))
        return res


def db_add_operation(schema):
    try:
        db.session.add(schema)
        db.session.commit()
        db.session.refresh(schema)
    except Exception as e:
        db.session.rollback()
        error_msg = f'Psql Error: {str(e)}'
        raise Exception(error_msg)
    return schema


def http_post_node(node_dict: dict, geid=None):
    """will assign the geid automaticly."""
    if not geid:
        node_dict.update({'global_entity_id': GEIDClient().get_GEID()})
    node_creation_url = ConfigClass.NEO4J_SERVICE + 'nodes/Dataset'
    with httpx.Client() as client:
        response = client.post(node_creation_url, json=node_dict)
    return response


def http_update_node(primary_label, neo4j_id, update_json):
    # update neo4j node
    update_url = ConfigClass.NEO4J_SERVICE + 'nodes/{}/node/{}'.format(primary_label, neo4j_id)
    with httpx.Client() as client:
        res = client.put(url=update_url, json=update_json)
    return res


def create_atlas_dataset(geid, operator):
    attrs = {
        'global_entity_id': geid,
        'qualifiedName': geid,
        'name': geid,
        'createTime': time.time(),
        'modifiedTime': 0,
        'replicatedTo': None,
        'userDescription': None,
        'isFile': False,
        'numberOfReplicas': 0,
        'replicatedFrom': None,
        'displayName': None,
        'extendedAttributes': None,
        'nameServiceId': None,
        'posixPermissions': None,
        'clusterName': None,
        'isSymlink': False,
        'group': None,
    }
    atlas_post_form_json = {
        'referredEntities': {},
        'entity': {
            'typeName': 'dataset',
            'attributes': attrs,
            'isIncomplete': False,
            'status': 'ACTIVE',
            'createdBy': operator,
            'version': 0,
            'relationshipAttributes': {'schema': [], 'inputToProcesses': [], 'meanings': [], 'outputFromProcesses': []},
            'customAttributes': {},
            'labels': [],
        },
    }
    url = ConfigClass.CATALOGUING_SERVICE_V1 + 'entity'
    with httpx.Client() as client:
        res = client.post(url, json=atlas_post_form_json)
    return res
