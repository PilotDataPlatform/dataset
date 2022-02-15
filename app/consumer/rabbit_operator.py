import pika
import json
from ..config import ConfigClass


class RabbitConnection:
    # This class used to initiate Queue connection
    def __init__(self):
        pass

    def init_connection(self):
        try:
            credentials = pika.PlainCredentials(
                ConfigClass.gm_username,
                ConfigClass.gm_password)
            self._instance = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=ConfigClass.gm_queue_endpoint,
                    heartbeat=180,
                    credentials=credentials)
            )
            return self._instance
        except Exception as e:
            raise

    def close_connection(self):
        self._instance.close()

    def get_current_connection(self):
        return self._instance
