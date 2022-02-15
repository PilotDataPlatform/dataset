# from fastapi_sqlalchemy import db
from sqlalchemy import Column, String, Date, DateTime, Integer, Boolean, ForeignKey
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from app.config import ConfigClass

from datetime import datetime

# # print(ConfigClass.OPS_DB_URI)
# engine = create_engine(ConfigClass.OPS_DB_URI)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# session = SessionLocal()
Base = declarative_base()


class DatasetSchemaTemplate(Base):
    __tablename__ = 'dataset_schema_template'
    __table_args__ = {"schema":ConfigClass.RDS_SCHEMA_DEFAULT}
    geid = Column(String(), unique=True, primary_key=True)
    name = Column(String())
    dataset_geid = Column(String())
    standard = Column(String())
    system_defined = Column(Boolean())
    is_draft = Column(Boolean())
    content = Column(JSONB())
    create_timestamp = Column(DateTime(), default=datetime.utcnow)
    update_timestamp = Column(DateTime(), default=datetime.utcnow)
    creator = Column(String())
    schemas = relationship("DatasetSchema", back_populates="schema_template")

    def __init__(self, geid, name, dataset_geid, standard, system_defined, is_draft, content, creator):
        self.geid = geid 
        self.name = name 
        self.dataset_geid = dataset_geid 
        self.standard = standard 
        self.system_defined = system_defined
        self.is_draft = is_draft 
        self.content = content
        self.creator = creator

    def to_dict(self):
        result = {}
        for field in ["geid", "name", "dataset_geid", "standard", "system_defined", "is_draft", \
                "content", "creator", "create_timestamp", "update_timestamp"]:
            if field in ["create_timestamp", "update_timestamp"]:
                result[field] = str(getattr(self, field).isoformat()[:-3] + 'Z')
            elif field in ["content", "system_defined", "is_draft"]:
                result[field] = getattr(self, field)
            else:
                result[field] = str(getattr(self, field))
        return result


class DatasetSchema(Base):
    __tablename__ = 'dataset_schema'
    __table_args__ = {"schema":ConfigClass.RDS_SCHEMA_DEFAULT}
    geid = Column(String(), unique=True, primary_key=True)
    name = Column(String())
    dataset_geid = Column(String())
    tpl_geid = Column(String(), ForeignKey(DatasetSchemaTemplate.geid))
    standard = Column(String())
    system_defined = Column(Boolean())
    is_draft = Column(Boolean())
    content = Column(JSONB())
    create_timestamp = Column(DateTime(), default=datetime.utcnow)
    update_timestamp = Column(DateTime(), default=datetime.utcnow)
    creator = Column(String())
    schema_template = relationship("DatasetSchemaTemplate", back_populates="schemas")

    def __init__(self, geid, name, dataset_geid, tpl_geid, standard, system_defined, is_draft, content, creator):
        self.geid = geid 
        self.dataset_geid = dataset_geid 
        self.tpl_geid = tpl_geid 
        self.standard = standard 
        self.system_defined = system_defined
        self.is_draft = is_draft 
        self.content = content
        self.creator = creator
        self.name = name 

    def to_dict(self):
        result = {}
        for field in ["geid", "name", "dataset_geid", "tpl_geid", "standard", "system_defined", "is_draft", \
                "content", "creator", "create_timestamp", "update_timestamp"]:
            if field in ["create_timestamp", "update_timestamp"]:
                result[field] = str(getattr(self, field).isoformat()[:-3] + 'Z')
            elif field in ["content", "system_defined", "is_draft"]:
                result[field] = getattr(self, field)
            else:
                result[field] = str(getattr(self, field))
        return result


