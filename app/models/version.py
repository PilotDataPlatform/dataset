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

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import TIMESTAMP

from app.config import ConfigClass
from app.models import DBModel


class DatasetVersion(DBModel):
    __tablename__ = 'version'
    __table_args__ = {'schema': ConfigClass.RDS_SCHEMA_DEFAULT}
    id = Column(Integer, primary_key=True)
    dataset_code = Column(String())
    dataset_geid = Column(String())
    version = Column(String())
    created_by = Column(String())
    created_at = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    location = Column(String())
    notes = Column(String())

    def to_dict(self):
        result = {}
        for field in ['id', 'dataset_code', 'dataset_geid', 'version', 'created_by', 'created_at', 'location', 'notes']:
            if field == 'created_at':
                result[field] = str(getattr(self, field).strftime('%Y-%m-%dT%H:%M:%S'))
            else:
                result[field] = str(getattr(self, field))
        return result
