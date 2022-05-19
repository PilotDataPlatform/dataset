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

from datetime import datetime

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import relationship

from app.config import ConfigClass
from app.models import DBModel


class DatasetSchemaTemplate(DBModel):
    __tablename__ = 'schema_template'
    __table_args__ = {'schema': ConfigClass.RDS_SCHEMA_DEFAULT}
    geid = Column(String(), primary_key=True)
    name = Column(String())
    dataset_geid = Column(String())
    standard = Column(String())
    system_defined = Column(Boolean())
    is_draft = Column(Boolean())
    content = Column(JSONB())
    create_timestamp = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    update_timestamp = Column(
        TIMESTAMP(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    creator = Column(String())
    schemas = relationship('DatasetSchema', back_populates='schema_template')

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
        for field in [
            'geid',
            'name',
            'dataset_geid',
            'standard',
            'system_defined',
            'is_draft',
            'content',
            'creator',
            'create_timestamp',
            'update_timestamp',
        ]:
            if field in ['create_timestamp', 'update_timestamp']:
                result[field] = str(getattr(self, field).isoformat()[:-3] + 'Z')
            elif field in ['content', 'system_defined', 'is_draft']:
                result[field] = getattr(self, field)
            else:
                result[field] = str(getattr(self, field))
        return result


class DatasetSchema(DBModel):
    __tablename__ = 'schema'
    __table_args__ = {'schema': ConfigClass.RDS_SCHEMA_DEFAULT}
    geid = Column(String(), primary_key=True)
    name = Column(String())
    dataset_geid = Column(String())
    tpl_geid = Column(String(), ForeignKey(DatasetSchemaTemplate.geid))
    standard = Column(String())
    system_defined = Column(Boolean())
    is_draft = Column(Boolean())
    content = Column(JSONB())
    create_timestamp = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    update_timestamp = Column(
        TIMESTAMP(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )
    creator = Column(String())
    schema_template = relationship('DatasetSchemaTemplate', back_populates='schemas')

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
        for field in [
            'geid',
            'name',
            'dataset_geid',
            'tpl_geid',
            'standard',
            'system_defined',
            'is_draft',
            'content',
            'creator',
            'create_timestamp',
            'update_timestamp',
        ]:
            if field in ['create_timestamp', 'update_timestamp']:
                result[field] = str(getattr(self, field).isoformat()[:-3] + 'Z')
            elif field in ['content', 'system_defined', 'is_draft']:
                result[field] = getattr(self, field)
            else:
                result[field] = str(getattr(self, field))
        return result
