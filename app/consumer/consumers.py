from .consumer_dynamic import ConsumerDynamic
from .rabbit_operator import RabbitConnection
from ..resources.es_helper import insert_one_by_id, get_one_by_id
import ast
import pika
import json


def publish(queue, routing_key, body,
            exchange_name, exchange_type):
    my_rabbit = RabbitConnection()
    connection_instance = my_rabbit.init_connection()
    channel = connection_instance.channel()
    channel.queue_declare(queue=queue)
    channel.exchange_declare(
        exchange=exchange_name,
        exchange_type=exchange_type)
    channel.queue_bind(
        exchange=exchange_name,
        queue=queue,
        routing_key=routing_key)
    channel.basic_publish(
        exchange=exchange_name,
        routing_key=routing_key,
        body=json.dumps(body),
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )
    )
    channel.confirm_delivery()
    my_rabbit.close_connection()


def callback(ch, method, properties, body, ctx_context):
    msg = json.loads(body)
    payload = msg['payload']

    # insert activity log to elastic search
    es_body = {
        "global_entity_id": payload['act_geid'],
        "create_timestamp": int(msg['create_timestamp']),
        "operator": payload['operator'],
        "dataset_geid": payload['dataset_geid'],
        "event_type": msg['event_type'],
        "action": payload['action'],
        "resource": payload['resource'],
        "detail": payload['detail'],
    }

    check_es_res = get_one_by_id('activity-logs', '_doc', payload['act_geid'])
    if check_es_res['found']:
        return

    create_es_res = insert_one_by_id('_doc', 'activity-logs',
                                     es_body, payload['act_geid'])
    print(create_es_res)
    if create_es_res['result'] == 'created':
        print('Publish a “DATASET_ACTLOG_SUCCEED“ event')
        msg['event_type'] = 'DATASET_ACTLOG_SUCCEED'
    else:
        print('publish a “DATASET_ACTLOG_TERMINATED“ message')
        msg['event_type'] = '“DATASET_ACTLOG_TERMINATED“'
    # publish(ctx_context['queue'], ctx_context['routing_key'], msg,
    #         ctx_context['exchange_name'], ctx_context['exchange_type'])


def dataset_consumer():
    print('Start background consumer')
    sub_content = {
        "sub_name": "dataset_activity_logger",
        "queue": 'dataset_actlog',
        "routing_key": '',
        "exchange_name": 'DATASET_ACTS',
        'exchange_type': 'fanout'
    }

    # start consumer
    consumer = ConsumerDynamic(
        'dataset_activity_logger', 'dataset_actlog',
        routing_key='',
        exchange_name='DATASET_ACTS',
        exchange_type='fanout')
    consumer.set_callback(callback, sub_content)
    consumer.start()
