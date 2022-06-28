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

from asyncio import get_event_loop

from aiokafka import AIOKafkaProducer
from common import LoggerFactory

from app.config import SRV_NAMESPACE
from app.config import ConfigClass

logger = LoggerFactory(__name__).get_logger()

loop = get_event_loop()

aioproducer = AIOKafkaProducer(loop=loop, client_id=SRV_NAMESPACE, bootstrap_servers=ConfigClass.KAFKA_URL)
