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

from sqlalchemy import create_engine
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from app.config import ConfigClass
from app.models.schema import Base
from app.models.schema import DatasetSchema
from app.models.schema import DatasetSchemaTemplate

engine = create_engine(ConfigClass.OPS_DB_URI, echo=True)

if __name__ == '__main__':
    CreateTable(DatasetSchemaTemplate.__table__).compile(dialect=postgresql.dialect())
    CreateTable(DatasetSchema.__table__).compile(dialect=postgresql.dialect())

    Base.metadata.create_all(bind=engine)
