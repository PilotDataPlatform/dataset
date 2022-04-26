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

import pika

from app.config import ConfigClass


class RabbitConnection:
    # This class used to initiate Queue connection
    def __init__(self):
        pass

    def init_connection(self):
        try:
            credentials = pika.PlainCredentials(ConfigClass.gm_username, ConfigClass.gm_password)
            self._instance = pika.BlockingConnection(
                pika.ConnectionParameters(host=ConfigClass.gm_queue_endpoint, heartbeat=180, credentials=credentials)
            )
            return self._instance
        except Exception:
            raise

    def close_connection(self):
        self._instance.close()

    def get_current_connection(self):
        return self._instance
