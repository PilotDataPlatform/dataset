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

import asyncio
from io import BytesIO
from unittest import mock

import pytest
import pytest_asyncio
from async_asgi_testclient import TestClient
from httpx import Response
from redis import StrictRedis
from sqlalchemy import create_engine
from sqlalchemy import schema
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import close_all_sessions
from starlette.config import environ
from testcontainers.postgres import PostgresContainer
from urllib3 import HTTPResponse

environ['CONFIG_CENTER_ENABLED'] = 'false'

environ['NEO4J_SERVICE'] = 'http://NEO4J_SERVICE'
environ['QUEUE_SERVICE'] = 'http://QUEUE_SERVICE'
environ['DATA_OPS_UTIL'] = 'http://DATA_OPS_UTIL'
environ['CATALOGUING_SERVICE'] = 'http://CATALOGUING_SERVICE'
environ['ENTITYINFO_SERVICE'] = 'http://ENTITYINFO_SERVICE'
environ['ELASTIC_SEARCH_SERVICE'] = 'http://ELASTIC_SEARCH_SERVICE'
environ['SEND_MESSAGE_URL'] = 'http://SEND_MESSAGE_URL'

environ['gm_queue_endpoint'] = 'http://gm_queue_endpoint'
environ['gm_username'] = 'gm_username'
environ['gm_password'] = 'gm_password'

environ['OPEN_TELEMETRY_ENABLED'] = 'true'
environ['CORE_ZONE_LABEL'] = 'Core'
environ['GREEN_ZONE_LABEL'] = 'Greenroom'
environ['MINIO_OPENID_CLIENT'] = 'MINIO_OPENID_CLIENT'
environ['MINIO_ENDPOINT'] = 'MINIO_ENDPOINT'
environ['MINIO_HTTPS'] = 'false'
environ['KEYCLOAK_URL'] = 'KEYCLOAK_URL'

environ['MINIO_ACCESS_KEY'] = 'MINIO_ACCESS_KEY'
environ['MINIO_SECRET_KEY'] = 'MINIO_SECRET_KEY'
environ['KEYCLOAK_MINIO_SECRET'] = 'KEYCLOAK_MINIO_SECRET'
environ['REDIS_PORT'] = '6379'
environ['REDIS_DB'] = '0'
environ['REDIS_PASSWORD'] = ''

environ['ROOT_PATH'] = './tests/'

environ['RDS_SCHEMA_DEFAULT'] = 'INDOC_TEST'
environ['RDS_DB_URI'] = 'postgresql://postgres:postgres@localhost:5432/INDOC_TEST'


@pytest_asyncio.fixture(scope='session')
def db_postgres():
    with PostgresContainer('postgres:9.5') as postgres:
        yield postgres.get_connection_url()


@pytest_asyncio.fixture()
def create_db(db_postgres):
    from app.models.bids_sql import Base as BidsBase
    from app.models.schema_sql import Base as SchemaBase
    from app.models.version_sql import Base as VersionBase

    db_schema = environ.get('RDS_SCHEMA_DEFAULT')
    engine = create_engine(db_postgres, echo=True)
    if not engine.dialect.has_schema(engine, db_schema):
        engine.execute(schema.CreateSchema(db_schema))
    SchemaBase.metadata.create_all(bind=engine)
    VersionBase.metadata.create_all(bind=engine)
    BidsBase.metadata.create_all(bind=engine)
    yield engine
    SchemaBase.metadata.drop_all(bind=engine)
    VersionBase.metadata.drop_all(bind=engine)
    BidsBase.metadata.drop_all(bind=engine)


@pytest_asyncio.fixture()
def db_session(create_db):
    engine = create_db
    Session = sessionmaker(engine)
    yield Session()
    close_all_sessions()


@pytest_asyncio.fixture(scope='session')
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
    asyncio.set_event_loop_policy(None)


@pytest.fixture
def app(db_postgres):
    from app.config import ConfigClass
    from app.main import create_app

    ConfigClass.OPS_DB_URI = db_postgres
    app = create_app()
    yield app


@pytest.fixture
async def client(app):
    return TestClient(app)


@pytest_asyncio.fixture
def mock_minio(monkeypatch):
    from app.commons.service_connection.minio_client import Minio

    http_response = HTTPResponse()
    response = Response(status_code=200)
    response.raw = http_response
    response.raw._fp = BytesIO(b'File like object')

    monkeypatch.setattr(Minio, 'get_object', lambda x, y, z: http_response)
    monkeypatch.setattr(Minio, 'list_buckets', lambda x: [])
    monkeypatch.setattr(Minio, 'fget_object', lambda *x: [])
    monkeypatch.setattr(Minio, 'fput_object', lambda *x: mock.MagicMock())
    monkeypatch.setattr(Minio, 'make_bucket', lambda *x: mock.MagicMock())
    monkeypatch.setattr(Minio, 'set_bucket_encryption', lambda *x: mock.MagicMock())


@pytest_asyncio.fixture(autouse=True)
async def clean_up_redis():
    cache = StrictRedis(host=environ.get('REDIS_HOST'))
    cache.flushall()


@pytest.fixture()
def test_db(db_session):
    yield


@pytest.fixture
def version(db_session):
    from app.models.version_sql import DatasetVersion

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3-1620404058'
    new_version = DatasetVersion(
        dataset_code='dataset_code',
        dataset_geid=dataset_geid,
        version='2.0',
        created_by='admin',
        location='minio_location',
        notes='test',
    )
    db_session.add(new_version)
    db_session.commit()
    yield new_version.to_dict()
    db_session.delete(new_version)
    db_session.commit()
