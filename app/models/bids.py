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

from sqlalchemy import JSON
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.dialects.postgresql import TIMESTAMP

from app.config import ConfigClass
from app.models import DBModel


class BIDSResult(DBModel):
    __tablename__ = 'bids_results'
    __table_args__ = {'schema': ConfigClass.RDS_SCHEMA_DEFAULT}
    id = Column(Integer, primary_key=True)
    dataset_geid = Column(String(), unique=True)
    created_time = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, nullable=False)
    updated_time = Column(TIMESTAMP(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    validate_output = Column(JSON())

    def __init__(self, dataset_geid, validate_output):
        self.dataset_geid = dataset_geid
        self.validate_output = validate_output
        if self.created_time:
            self.created_time = self.created_time

    def to_dict(self):
        result = {}
        for field in ['id', 'dataset_geid', 'created_time', 'updated_time', 'validate_output']:
            if field == 'created_time' or field == 'updated_time':
                result[field] = str(getattr(self, field).strftime('%Y-%m-%dT%H:%M:%S'))
            elif field == 'validate_output':
                result[field] = getattr(self, field)
            else:
                result[field] = str(getattr(self, field))
        return result
