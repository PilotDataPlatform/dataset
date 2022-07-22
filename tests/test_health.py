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

import pytest

from app.core.db import db_engine

pytestmark = pytest.mark.asyncio


async def test_health_should_return_204_with_empty_response(client):
    response = await client.get('/health')
    assert response.status_code == 204
    assert not response.text


@pytest.mark.parametrize('kafka_host,db_host', [('fake', 'fake'), ('fake', None), (None, 'fake')])
async def test_health_should_return_503_when_kafka_or_db_fails(client, monkeypatch, kafka_host, db_host):
    db_engine.instance = None

    from app.config import ConfigClass

    if db_host:
        monkeypatch.setattr(ConfigClass, 'OPS_DB_URI', db_host)
    if kafka_host:
        monkeypatch.setattr(ConfigClass, 'KAFKA_URL', kafka_host)

    response = await client.get('/health')
    assert response.status_code == 503
    assert not response.json()
