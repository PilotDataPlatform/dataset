import threading, time, json
import requests
from .rabbit_operator import RabbitConnection

class ConsumerDynamic(threading.Thread):
    def __init__(self, unique_name,
            queue, routing_key,
            exchange_name, exchange_type):
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
                print("=================Consumer {} is consuming.".format(self.unique_name))
                callback(*args, context)
            except Exception as exce:
                # prevent from blocking other thredings
                pass
        self.__callback = wrapper

    def run(self):
        try:
            if not self.__callback:
                raise Exception(
                    '[Fatal] callback not set for consumer: {}'.format(self.unique_name))
            queue = self.queue
            exchange_name = self.exchange_name
            exchange_type = self.exchange_type
            routing_key = self.routing_key
            callback = self.__callback
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
            channel.basic_consume(
                queue=queue, on_message_callback=callback, auto_ack=True)
            channel.start_consuming()
        except Exception as exce:
            # prevent from blocking other thredings
            print(str(exce))

# threading consumer example
class Consumer(threading.Thread):
    def __init__(self,name):
        threading.Thread.__init__(self, name=name)
    def run(self):
        for i in range(5):
            print("%s is consuming!" % (i))
            time.sleep(3)
        print("%s finished!" % self.getName())

# test event consuming function
def test_callback(ch, method, properties, msg_body):
    from pprint import pprint
    event = json.loads(msg_body) 
    pprint(event)

# eventhook consumer callback
def eventhook(ch, method, properties, msg_body, context):
    location = context["location"]
    event = json.loads(msg_body)
    event["runtime_ctx"] = context
    res = requests.post(url=location, json=event)

