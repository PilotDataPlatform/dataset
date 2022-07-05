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
import io
from uuid import UUID

import pytest
from fastavro import schema as avro_schema
from fastavro import schemaless_reader

from app.schemas.activity_log import FileFolderActivityLogSchema
from app.services.activity_log import FileFolderActivityLogService

pytestmark = pytest.mark.asyncio


async def test_send_on_import_event_send_correct_msg(test_db, kafka_file_folder_consumer, dataset):
    item = {
        'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
        'parent_path': 'test_folder_6',
        'type': 'file',
        'zone': 1,
        'name': 'Dateidaten_für_vretest3',
        'owner': 'admin',
        'container_code': 'testdataset202201101',
        'container_type': 'dataset',
    }
    item_list = [item]
    user = 'user'

    project = {'code': 'source_project_code'}
    await FileFolderActivityLogService().send_on_import_event(dataset, project, item_list, user)

    msg = await kafka_file_folder_consumer.getone()

    schema_loaded = avro_schema.load_schema('app/schemas/metadata.items.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = FileFolderActivityLogSchema.parse_obj(activity_log)

    assert activity_log_schema.item_id == UUID(item['id'])
    assert activity_log_schema.item_type == item['type']
    assert activity_log_schema.item_name == item['name']
    assert activity_log_schema.item_parent_path == ''
    assert activity_log_schema.container_type == 'dataset'
    assert activity_log_schema.zone == 1
    assert activity_log_schema.imported_from == project['code']
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == 'user'
    assert activity_log_schema.activity_type == 'import'
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == activity_log['changes']


async def test_send_on_delete_event_send_correct_msg(test_db, kafka_file_folder_consumer, dataset):
    item = {
        'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
        'parent_path': 'test_folder_6',
        'type': 'file',
        'zone': 1,
        'name': 'Dateidaten_für_vretest3',
        'owner': 'admin',
        'container_code': 'testdataset202201101',
        'container_type': 'dataset',
    }
    item_list = [item]
    user = 'user'
    await FileFolderActivityLogService().send_on_delete_event(dataset, item_list, user)

    msg = await kafka_file_folder_consumer.getone()

    schema_loaded = avro_schema.load_schema('app/schemas/metadata.items.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = FileFolderActivityLogSchema.parse_obj(activity_log)

    assert activity_log_schema.item_id == UUID(item['id'])
    assert activity_log_schema.item_type == item['type']
    assert activity_log_schema.item_name == item['name']
    assert activity_log_schema.item_parent_path == item['parent_path']
    assert activity_log_schema.container_type == 'dataset'
    assert activity_log_schema.zone == 1
    assert not activity_log_schema.imported_from
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == 'user'
    assert activity_log_schema.activity_type == 'delete'
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == activity_log['changes']


async def test_send_on_move_event_send_correct_msg(test_db, kafka_file_folder_consumer, dataset):
    item = {
        'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
        'parent_path': 'test_folder_6',
        'type': 'file',
        'zone': 1,
        'name': 'Dateidaten_für_vretest3',
        'owner': 'admin',
        'container_code': 'testdataset202201101',
        'container_type': 'dataset',
    }
    user = 'user'
    await FileFolderActivityLogService().send_on_move_event(dataset, item, user, '', 'folder1')

    msg = await kafka_file_folder_consumer.getone()

    schema_loaded = avro_schema.load_schema('app/schemas/metadata.items.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = FileFolderActivityLogSchema.parse_obj(activity_log)

    assert activity_log_schema.item_id == UUID(item['id'])
    assert activity_log_schema.item_type == item['type']
    assert activity_log_schema.item_name == item['name']
    assert activity_log_schema.item_parent_path == item['parent_path']
    assert activity_log_schema.container_type == 'dataset'
    assert activity_log_schema.zone == 1
    assert not activity_log_schema.imported_from
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == 'user'
    assert activity_log_schema.activity_type == 'update'
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == [{'item_property': 'parent_path', 'old_value': '', 'new_value': 'folder1'}]


async def test_send_on_rename_event_send_correct_msg(test_db, kafka_file_folder_consumer, dataset):
    item = {
        'id': 'ded5bf1e-80f5-4b39-bbfd-f7c74054f41d',
        'parent_path': 'test_folder_6',
        'type': 'file',
        'zone': 1,
        'name': 'Dateidaten_für_vretest3',
        'owner': 'admin',
        'container_code': 'testdataset202201101',
        'container_type': 'dataset',
    }
    item_list = [item]
    user = 'user'
    await FileFolderActivityLogService().send_on_rename_event(dataset, item_list, user, 'file2.txt')

    msg = await kafka_file_folder_consumer.getone()

    schema_loaded = avro_schema.load_schema('app/schemas/metadata.items.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = FileFolderActivityLogSchema.parse_obj(activity_log)

    assert activity_log_schema.item_id == UUID(item['id'])
    assert activity_log_schema.item_type == item['type']
    assert activity_log_schema.item_name == item['name']
    assert activity_log_schema.item_parent_path == item['parent_path']
    assert activity_log_schema.container_type == 'dataset'
    assert activity_log_schema.zone == 1
    assert not activity_log_schema.imported_from
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == 'user'
    assert activity_log_schema.activity_type == 'update'
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == [
        {'item_property': 'name', 'old_value': item['name'], 'new_value': 'file2.txt'}
    ]
