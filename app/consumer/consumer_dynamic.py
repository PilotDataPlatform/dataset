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
import threading
import time

import httpx
from common import LoggerFactory

from .rabbit_operator import RabbitConnection

logger = LoggerFactory('datasetConsumer').get_logger()


class ConsumerDynamic(threading.Thread):
    def __init__(self, unique_name, queue, routing_key, exchange_name, exchange_type):
        self.unique_name = unique_name
        self.queue = queue
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.exchange_type = exchange_type
        self.__callback = None
        threading.Thread.__init__(self, name=unique_name)

    def set_callback(self, callback, context=None):
        def wrapper(*args):
            try:
                logger.info('Consumer is consuming.', extra={'id': self.unique_name})
                callback(*args, context)
            except Exception:
                logger.exception('Error to consume event')
                # prevent from blocking other thredings
                pass

        self.__callback = wrapper

    def run(self):
        try:
            if not self.__callback:
                raise Exception('[Fatal] callback not set for consumer', extra={'id': self.unique_name})
            queue = self.queue
            exchange_name = self.exchange_name
            exchange_type = self.exchange_type
            routing_key = self.routing_key
            callback = self.__callback
            my_rabbit = RabbitConnection()
            connection_instance = my_rabbit.init_connection()
            channel = connection_instance.channel()
            channel.queue_declare(queue=queue)
            channel.exchange_declare(exchange=exchange_name, exchange_type=exchange_type)
            channel.queue_bind(exchange=exchange_name, queue=queue, routing_key=routing_key)
            channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception:
            # prevent from blocking other thredings
            logger.exception('prevent from blocking other thredings')


# threading consumer example
class Consumer(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self, name=name)

    def run(self):
        for i in range(5):
            logger.info('consuming!', extra={'id': i})
            time.sleep(3)
        logger.info('finished!', extra={'name': self.getName()})


# eventhook consumer callback
def eventhook(ch, method, properties, msg_body, context):
    location = context['location']
    event = json.loads(msg_body)
    event['runtime_ctx'] = context
    with httpx.Client() as client:
        client.post(url=location, json=event)
