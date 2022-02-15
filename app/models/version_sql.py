from fastapi_sqlalchemy import db
from sqlalchemy import Column, String, Date, DateTime, Integer, Boolean, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from app.config import ConfigClass

from datetime import datetime

Base = declarative_base()

class DatasetVersion(Base):
    __tablename__ = 'dataset_version'
    __table_args__ = {"schema":ConfigClass.RDS_SCHEMA_DEFAULT}
    id = Column(Integer, unique=True, primary_key=True)
    dataset_code = Column(String())
    dataset_geid = Column(String())
    version = Column(String())
    created_by = Column(String())
    created_at = Column(DateTime(), default=datetime.utcnow)
    location = Column(String())
    notes = Column(String())

    def __init__(self, dataset_code, dataset_geid, version, created_by, location, notes):
        self.dataset_code = dataset_code
        self.dataset_geid = dataset_geid 
        self.version = version
        self.created_by = created_by 
        self.location = location
        self.notes = notes 

    def to_dict(self):
        result = {}
        for field in ["id", "dataset_code", "dataset_geid", "version", "created_by", "created_at", "location", "notes"]:
            if field == "created_at":
                result[field] = str(getattr(self, field).isoformat()[:-3] + 'Z')
            else:
                result[field] = str(getattr(self, field))
        return result


