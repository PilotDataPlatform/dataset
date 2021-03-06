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

from io import BytesIO

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from common import LoggerFactory

from app.config import ConfigClass


class KafkaProducerClient:

    logger = LoggerFactory('KafkaProducerClient').get_logger()
    aioproducer = None

    async def create_kafka_producer(self):
        if not self.aioproducer:
            self.aioproducer = AIOKafkaProducer(bootstrap_servers=[ConfigClass.KAFKA_URL])
            await self.aioproducer.start()

    async def send(self, topic: str, msg: BytesIO):
        try:
            await self.aioproducer.send(topic, msg)
        except KafkaError as ke:
            self.logger.exception('error sending ActivityLog to Kafka: %s', ke)


kafka_client = KafkaProducerClient()


async def get_kafka_client():
    await kafka_client.create_kafka_producer()
    return kafka_client


__all__ = 'get_kafka_client'
