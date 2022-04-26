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
from uuid import uuid4

from app.commons.logger_services.logger_factory_service import SrvLoggerFactory
from app.resources.es_helper import get_one_by_id
from app.resources.es_helper import insert_one_by_id

from .consumer_dynamic import ConsumerDynamic

logger = SrvLoggerFactory('datasetConsumer').get_logger()


def callback(ch, method, properties, body, ctx_context):
    msg = json.loads(body)
    payload = msg['payload']

    # insert activity log to elastic search
    global_entity_id = payload.get('act_geid', str(uuid4()))
    es_body = {
        'global_entity_id': global_entity_id,
        'create_timestamp': int(msg['create_timestamp']),
        'operator': payload['operator'],
        'dataset_geid': payload['dataset_geid'],
        'event_type': msg['event_type'],
        'action': payload['action'],
        'resource': payload['resource'],
        'detail': payload['detail'],
    }

    check_es_res = get_one_by_id('activity-logs', '_doc', global_entity_id)
    if check_es_res['found']:
        logger.info('activity-logs already created', extra=check_es_res)
        return

    create_es_res = insert_one_by_id('_doc', 'activity-logs', es_body, global_entity_id)

    logger.info('create data ES', extra=create_es_res)
    if create_es_res['result'] == 'created':
        logger.info('Publish a “DATASET_ACTLOG_SUCCEED“ event')
        msg['event_type'] = 'DATASET_ACTLOG_SUCCEED'
    else:
        logger.info('publish a “DATASET_ACTLOG_TERMINATED“ message')
        msg['event_type'] = '“DATASET_ACTLOG_TERMINATED“'


def dataset_consumer():
    logger.info('Start background consumer')
    sub_content = {
        'sub_name': 'dataset_activity_logger',
        'queue': 'dataset_actlog',
        'routing_key': '',
        'exchange_name': 'DATASET_ACTS',
        'exchange_type': 'fanout',
    }

    # start consumer
    consumer = ConsumerDynamic(
        'dataset_activity_logger',
        'dataset_actlog',
        routing_key='',
        exchange_name='DATASET_ACTS',
        exchange_type='fanout',
    )
    consumer.set_callback(callback, sub_content)
    consumer.start()
