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

import httpx
from common import LoggerFactory

from app.config import ConfigClass

__logger = LoggerFactory('es_helper').get_logger()


def insert_one(es_type, es_index, data):
    url = ConfigClass.ELASTIC_SEARCH_SERVICE + '/{}/{}'.format(es_index, es_type)

    with httpx.Client() as client:
        res = client.post(url, json=data)

    return res.json()


def insert_one_by_id(es_type, es_index, data, dataset_id):
    url = ConfigClass.ELASTIC_SEARCH_SERVICE + '/{}/{}/{}'.format(es_index, es_type, dataset_id)

    with httpx.Client() as client:
        res = client.put(url, json=data)

    return res.json()


def get_one_by_id(es_index, es_type, dataset_id):
    url = ConfigClass.ELASTIC_SEARCH_SERVICE + '/{}/{}/{}'.format(es_index, es_type, dataset_id)

    with httpx.Client() as client:
        res = client.get(url)

    return res.json()
