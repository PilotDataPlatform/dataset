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
from typing import Dict
from typing import Optional

from pydantic import BaseModel


class ActivityLogSchema(BaseModel):
    activity_type: str
    activity_time: str = datetime.utcnow().isoformat()
    container_code: str
    version: str
    target_name: Optional[str] = None
    user: str
    changes: list[Dict[str, str]] = []


class ItemActivityLogSchema(ActivityLogSchema):
    item_id: str
    item_type: str
    item_name: str
    item_parent_path: str
    container_type: str
    zone: int
    imported_from: Optional[str] = None
