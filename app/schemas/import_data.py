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


class ImportDataPost(BaseModel):
    """the post request payload for import data from project."""

    source_list: list
    operator: str
    project_geid: str


class DatasetFileDelete(BaseModel):
    """the delete request payload for dataset to delete files."""

    source_list: list
    operator: str


class DatasetFileMove(BaseModel):
    """the post request payload for dataset to move files under the dataset."""

    source_list: list
    operator: str
    target_geid: str


class DatasetFileRename(BaseModel):
    """the post request payload for dataset to move files under the dataset."""

    new_name: str
    operator: str
