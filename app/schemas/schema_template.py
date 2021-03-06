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

from pydantic import BaseModel


class SchemaTemplatePost(BaseModel):
    """the post request payload for import data from project."""

    name: str
    standard: str
    system_defined: bool
    is_draft: bool
    content: dict
    creator: str


class SchemaTemplatePut(BaseModel):
    name: str
    is_draft: bool
    content: dict
    activity: list


class SchemaTemplateList(BaseModel):
    # dataset_geid : Optional[str] = None
    pass
