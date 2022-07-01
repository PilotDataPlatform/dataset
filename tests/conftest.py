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
from urllib.parse import urlparse
from uuid import uuid4

import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from aioredis import StrictRedis
from alembic.command import downgrade
from alembic.command import upgrade
from alembic.config import Config
from async_asgi_testclient import TestClient
from httpx import Response
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from starlette.config import environ
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from urllib3 import HTTPResponse

environ['CONFIG_CENTER_ENABLED'] = 'false'
environ['env'] = 'test'

environ['QUEUE_SERVICE'] = 'http://QUEUE_SERVICE'
environ['DATA_OPS_UTIL'] = 'http://DATA_OPS_UTIL'
environ['CATALOGUING_SERVICE'] = 'http://CATALOGUING_SERVICE'
environ['ENTITYINFO_SERVICE'] = 'http://ENTITYINFO_SERVICE'
environ['ELASTIC_SEARCH_SERVICE'] = 'http://ELASTIC_SEARCH_SERVICE'
environ['SEND_MESSAGE_URL'] = 'http://SEND_MESSAGE_URL'
environ['METADATA_SERVICE'] = 'http://METADATA_SERVICE'
environ['PROJECT_SERVICE'] = 'http://PROJECT_SERVICE'
environ['KEYCLOAK_URL'] = 'http://KEYCLOAK_URL'

environ['gm_queue_endpoint'] = 'http://gm_queue_endpoint'
environ['gm_username'] = 'gm_username'
environ['gm_password'] = 'gm_password'

environ['OPEN_TELEMETRY_ENABLED'] = 'true'
environ['CORE_ZONE_LABEL'] = 'Core'
environ['GREEN_ZONE_LABEL'] = 'Greenroom'
environ['MINIO_OPENID_CLIENT'] = 'MINIO_OPENID_CLIENT'
environ['MINIO_ENDPOINT'] = 'MINIO_ENDPOINT'
environ['MINIO_HTTPS'] = 'false'

environ['MINIO_ACCESS_KEY'] = 'MINIO_ACCESS_KEY'
environ['MINIO_SECRET_KEY'] = 'MINIO_SECRET_KEY'
environ['KEYCLOAK_MINIO_SECRET'] = 'KEYCLOAK_MINIO_SECRET'
environ['REDIS_PORT'] = '6379'
environ['REDIS_DB'] = '0'
environ['REDIS_PASSWORD'] = ''

environ['ROOT_PATH'] = './tests/'

environ['POSTGRES_DB'] = 'dataset'

environ['OPSDB_UTILITY_HOST'] = 'localhost'
environ['OPSDB_UTILITY_PORT'] = '5432'
environ['OPSDB_UTILITY_USERNAME'] = 'postgres'
environ['OPSDB_UTILITY_PASSWORD'] = 'postgres'
environ['KAFKA_URL'] = 'any'


@pytest_asyncio.fixture(scope='session')
def db_postgres():
    with PostgresContainer('postgres:14.1', dbname=environ['POSTGRES_DB']) as postgres:
        db_uri = postgres.get_connection_url()
        yield db_uri.replace(f'{urlparse(db_uri).scheme}://', 'postgresql+asyncpg://', 1)


@pytest_asyncio.fixture(scope='session')
def kafka_url():
    with KafkaContainer() as kafka_container:
        yield kafka_container.get_bootstrap_server()


@pytest_asyncio.fixture(scope='session')
async def kafka_dataset_consumer(kafka_url):
    consumer = AIOKafkaConsumer('datasets-activity-logs', bootstrap_servers=kafka_url)
    await consumer.start()
    yield consumer
    await consumer.stop()


@pytest_asyncio.fixture(autouse=True)
def set_settings(monkeypatch, db_postgres, kafka_url):
    from app.config import ConfigClass

    monkeypatch.setattr(ConfigClass, 'OPS_DB_URI', db_postgres)
    monkeypatch.setattr(ConfigClass, 'KAFKA_URL', kafka_url)


@pytest_asyncio.fixture()
async def create_db(db_postgres):
    engine = create_async_engine(db_postgres)
    config = Config('./alembic.ini')
    upgrade(config, 'head')
    yield engine
    downgrade(config, 'base')
    await engine.dispose()


@pytest_asyncio.fixture()
async def db_session(create_db):
    engine = create_db
    session = AsyncSession(
        engine,
        expire_on_commit=False,
    )
    yield session
    await session.close()


@pytest_asyncio.fixture(scope='session')
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
    asyncio.set_event_loop_policy(None)


@pytest_asyncio.fixture
async def client():
    from app.main import app

    async with TestClient(app) as client:
        yield client


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
    monkeypatch.setattr(Minio, 'copy_object', lambda *x: mock.MagicMock())
    monkeypatch.setattr(Minio, 'make_bucket', lambda *x: mock.MagicMock())
    monkeypatch.setattr(Minio, 'set_bucket_encryption', lambda *x: mock.MagicMock())


@pytest_asyncio.fixture(autouse=True)
async def clean_up_redis():
    cache = StrictRedis(host=environ.get('REDIS_HOST'))
    await cache.flushall()


@pytest_asyncio.fixture()
def test_db(db_session):
    yield db_session


@pytest_asyncio.fixture
async def version(db_session, dataset):
    from app.models.version import DatasetVersion

    new_version = DatasetVersion(
        dataset_code=dataset.code,
        dataset_geid=str(dataset.id),
        version='2.0',
        created_by=dataset.creator,
        location='minio_location',
        notes='test',
    )
    db_session.add(new_version)
    await db_session.commit()
    await db_session.refresh(new_version)
    yield new_version
    await db_session.delete(new_version)
    await db_session.commit()


@pytest_asyncio.fixture
async def dataset(db_session):
    from app.models.dataset import Dataset

    new_dataset = Dataset(
        **{
            'source': '',
            'title': 'fake_dataset',
            'authors': ['test'],
            'code': 'fakedataset',
            'type': 'general',
            'modality': ['any', 'many'],
            'collection_method': ['some'],
            'license': 'mit',
            'tags': ['dataset', 'test'],
            'description': 'fake dataset created by test',
            'size': 0,
            'total_files': 0,
            'creator': 'admin',
            'project_id': str(uuid4()),
        }
    )
    db_session.add(new_dataset)
    await db_session.commit()
    await db_session.refresh(new_dataset)
    yield new_dataset
    await db_session.delete(new_dataset)
    await db_session.commit()


@pytest_asyncio.fixture
async def schema_template(db_session, dataset):
    from app.models.schema import DatasetSchemaTemplate

    new_template = DatasetSchemaTemplate(
        geid=str(uuid4()),
        dataset_geid=str(dataset.id),
        name='test_schema_template',
        standard='default',
        system_defined=True,
        is_draft=True,
        content={},
        creator='admin',
    )
    db_session.add(new_template)
    await db_session.commit()
    await db_session.refresh(new_template)
    yield new_template
    await db_session.delete(new_template)
    await db_session.commit()


@pytest_asyncio.fixture
async def schema(db_session, schema_template, dataset):
    from app.models.schema import DatasetSchema

    schema = DatasetSchema(
        geid=str(uuid4()),
        name='unittestdataset',
        dataset_geid=str(dataset.id),
        tpl_geid=str(schema_template.geid),
        standard='default',
        system_defined=True,
        is_draft=False,
        content={},
        creator='admin',
    )
    db_session.add(schema)
    await db_session.commit()
    await db_session.refresh(schema)
    yield schema
    await db_session.delete(schema)
    await db_session.commit()


@pytest_asyncio.fixture
async def essential_schema(db_session, schema_template, dataset):
    from app.config import ConfigClass
    from app.models.schema import DatasetSchema

    schema = DatasetSchema(
        geid=str(uuid4()),
        name=ConfigClass.ESSENTIALS_NAME,
        dataset_geid=str(dataset.id),
        tpl_geid=str(schema_template.geid),
        standard='default',
        system_defined=True,
        is_draft=False,
        content={},
        creator='admin',
    )
    db_session.add(schema)
    await db_session.commit()
    await db_session.refresh(schema)
    yield schema
    await db_session.delete(schema)
    await db_session.commit()
