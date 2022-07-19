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
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from uuid import UUID

from pydantic import BaseModel


class BaseActivityLogSchema(BaseModel):
    activity_time: datetime = datetime.utcnow()
    changes: List[Dict[str, Any]] = []
    activity_type: str
    user: str
    container_code: str


class DatasetActivityLogSchema(BaseActivityLogSchema):
    version: Optional[str]
    target_name: Optional[str] = None


class FileFolderActivityLogSchema(BaseActivityLogSchema):
    item_id: UUID
    item_type: str
    item_name: str
    item_parent_path: str = ''
    container_type: str = 'dataset'
    zone: int = 1
    imported_from: Optional[str] = None
