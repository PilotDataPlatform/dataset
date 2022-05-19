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
from uuid import uuid4

from sqlalchemy import VARCHAR
from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.dialects.postgresql import INTEGER
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.dialects.postgresql import UUID

from app.models import DBModel


class Dataset(DBModel):
    """Dataset database model."""

    __tablename__ = 'datasets'

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    source = Column(VARCHAR(length=256), nullable=False)
    authors = Column(ARRAY(VARCHAR(256)), default=[], nullable=False)
    code = Column(VARCHAR(length=32), unique=True, index=True, nullable=False)
    type = Column(VARCHAR(length=256), nullable=False)
    modality = Column(ARRAY(VARCHAR(256)), default=[])
    collection_method = Column(ARRAY(VARCHAR(256)), default=[])
    license = Column(VARCHAR(length=256))
    tags = Column(ARRAY(VARCHAR(256)), default=[])
    description = Column(VARCHAR(length=5000), nullable=False)
    size = Column(INTEGER())
    total_files = Column(INTEGER())
    title = Column(VARCHAR(length=256), nullable=False)
    creator = Column(VARCHAR(length=256), nullable=False)
    project_id = Column(UUID(as_uuid=True))
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    updated_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    def to_dict(self):
        result = {}
        fields = [
            'id',
            'source',
            'authors',
            'code',
            'type',
            'modality',
            'collection_method',
            'license',
            'tags',
            'description',
            'size',
            'total_files',
            'title',
            'creator',
            'project_id',
            'created_at',
            'updated_at',
        ]
        for field in fields:
            if field == 'created_at' or field == 'updated_at':
                result[field] = str(getattr(self, field).isoformat()[:-3] + 'Z')
            else:
                result[field] = str(getattr(self, field))
        return result
