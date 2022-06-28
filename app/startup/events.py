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


from common import LoggerFactory
from fastapi import FastAPI
from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor
from opentelemetry.sdk.resources import SERVICE_NAME
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from app.config import SRV_NAMESPACE
from app.config import ConfigClass
from app.consumer.consumers import dataset_consumer
from app.core.db import db_engine
from app.core.kafka.producer import aioproducer

from .exception_handlers import exception_handlers
from .middlewares import middlewares

logger = LoggerFactory(__name__).get_logger()


def _setup_middlewares(app: FastAPI) -> None:
    for middleware in middlewares:
        middleware(app)


def _setup_exception_handlers(app: FastAPI) -> None:
    for exc, handler in exception_handlers:
        app.add_exception_handler(exc, handler)


async def _initialize_instrument_app(app: FastAPI) -> None:
    """Instrument the application with OpenTelemetry tracing."""
    tracer_provider = TracerProvider(resource=Resource.create({SERVICE_NAME: SRV_NAMESPACE}))
    trace.set_tracer_provider(tracer_provider)

    jaeger_exporter = JaegerExporter(
        agent_host_name=ConfigClass.OPEN_TELEMETRY_HOST, agent_port=ConfigClass.OPEN_TELEMETRY_PORT
    )
    tracer_provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

    FastAPIInstrumentor.instrument_app(app)
    HTTPXClientInstrumentor().instrument()

    db = await db_engine()
    SQLAlchemyInstrumentor().instrument(engine=db.sync_engine, service=SRV_NAMESPACE)


async def on_startup_event(app: FastAPI) -> None:
    _setup_middlewares(app)
    _setup_exception_handlers(app)

    if ConfigClass.opentelemetry_enabled:
        await _initialize_instrument_app(app)
    if ConfigClass.env != 'test':
        dataset_consumer()
    await aioproducer.start()


async def on_shutdown_event() -> None:
    await aioproducer.stop()


_all_ = ('on_startup_event', 'on_shutdown_event')
