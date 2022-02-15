import os
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi_sqlalchemy import DBSessionMiddleware
from sqlalchemy import create_engine
from .config import ConfigClass, SRV_NAMESPACE
from .api_registry import api_registry
from .consumer.consumers import dataset_consumer
from app.resources.error_handler import APIException
# from app.models.schema_sql import engine

from opentelemetry import trace
from opentelemetry.exporter.jaeger.thrift import JaegerExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.resources import SERVICE_NAME
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.requests import RequestsInstrumentor
from opentelemetry.instrumentation.sqlalchemy import SQLAlchemyInstrumentor


def create_app():
    '''
    create app function
    '''
    app = FastAPI(
        title="Service Dataset",
        description="Service Dataset",
        docs_url="/v1/api-doc",
        version=ConfigClass.VERSION 
    )

    app.add_middleware(DBSessionMiddleware, db_url=ConfigClass.OPS_DB_URI)
    app.add_middleware(
        CORSMiddleware,
        allow_origins="*",
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # API registry
    # v1
    api_registry(app)
    if os.environ.get("env", "test") != "test":
        print("test")
        dataset_consumer()

    @app.exception_handler(APIException)
    async def http_exception_handler(request: Request, exc: APIException):
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.content,
        )

    if ConfigClass.opentelemetry_enabled:
        instrument_app(app)

    return app


def instrument_app(app) -> None:
    """Instrument the application with OpenTelemetry tracing."""

    tracer_provider = TracerProvider(resource=Resource.create({SERVICE_NAME: SRV_NAMESPACE}))
    trace.set_tracer_provider(tracer_provider)

    jaeger_exporter = JaegerExporter(
        agent_host_name='127.0.0.1', agent_port=6831
    )

    tracer_provider.add_span_processor(BatchSpanProcessor(jaeger_exporter))

    FastAPIInstrumentor.instrument_app(app)
    RequestsInstrumentor().instrument()
    engine = create_engine(ConfigClass.OPS_DB_URI)
    SQLAlchemyInstrumentor().instrument(engine=engine, service=SRV_NAMESPACE)
