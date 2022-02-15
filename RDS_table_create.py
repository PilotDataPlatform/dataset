from sqlalchemy.schema import CreateTable
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects import sqlite 
from sqlalchemy import create_engine

from app.models.schema_sql import DatasetSchemaTemplate, DatasetSchema, Base
from app.config import ConfigClass

engine = create_engine(ConfigClass.OPS_DB_URI, echo = True)

if __name__ == "__main__":
    print(CreateTable(DatasetSchemaTemplate.__table__).compile(dialect=postgresql.dialect()))
    print(CreateTable(DatasetSchema.__table__).compile(dialect=postgresql.dialect()))

    Base.metadata.create_all(bind=engine)