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

import pytest
from fastavro import schema as avro_schema
from fastavro import schemaless_reader

from app.schemas.activity_log import DatasetActivityLogSchema
from app.services.activity_log import DatasetActivityLogService
from app.services.dataset import SrvDatasetMgr

pytestmark = pytest.mark.asyncio


async def test_send_dataset_on_create_event_should_send_correct_msg(dataset, kafka_dataset_consumer):
    dataset_activity_log = DatasetActivityLogService()

    await dataset_activity_log.send_dataset_on_create_event(dataset)

    msg = await kafka_dataset_consumer.getone()

    schema_loaded = avro_schema.load_schema('app/schemas/dataset.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = DatasetActivityLogSchema.parse_obj(activity_log)

    assert not activity_log_schema.version
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == dataset.creator
    assert not activity_log_schema.target_name
    assert activity_log_schema.activity_type == 'create'
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == activity_log['changes']


@pytest.mark.parametrize(
    'method,change,activity',
    [
        (DatasetActivityLogService().send_schema_template_on_delete_event, [], 'template_delete'),
        (DatasetActivityLogService().send_schema_template_on_update_event, [{'field': 'value'}], 'template_update'),
        (DatasetActivityLogService().send_schema_template_on_create_event, [], 'template_create'),
    ],
)
async def test_send_schema_template_events_send_correct_msg(
    test_db, kafka_dataset_consumer, schema_template, method, change, activity
):
    dataset = await SrvDatasetMgr().get_bygeid(test_db, geid=schema_template.dataset_geid)
    if change:
        await method(schema_template, dataset, change)
    else:
        await method(schema_template, dataset)

    msg = await kafka_dataset_consumer.getone()

    schema_loaded = avro_schema.load_schema('app/schemas/dataset.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = DatasetActivityLogSchema.parse_obj(activity_log)

    assert not activity_log_schema.version
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == schema_template.creator
    assert not activity_log_schema.version
    assert activity_log_schema.target_name == schema_template.name
    assert activity_log_schema.activity_type == activity
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == change


async def test_send_publish_version_succeed_should_send_correct_msg(test_db, version, kafka_dataset_consumer):
    dataset_activity_log = DatasetActivityLogService()
    dataset = await SrvDatasetMgr().get_bygeid(test_db, geid=version.dataset_geid)
    await dataset_activity_log.send_publish_version_succeed(version, dataset)

    msg = await kafka_dataset_consumer.getone()

    schema_loaded = avro_schema.load_schema('app/schemas/dataset.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = DatasetActivityLogSchema.parse_obj(activity_log)

    assert activity_log_schema.version == version.version
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == dataset.creator
    assert not activity_log_schema.target_name
    assert activity_log_schema.activity_type == 'release'
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == activity_log['changes']


@pytest.mark.parametrize(
    'method,change,activity,username',
    [
        (DatasetActivityLogService().send_schema_delete_event, [], 'schema_delete', 'user'),
        (DatasetActivityLogService().send_schema_update_event, [{'field': 'value'}], 'schema_update', 'user'),
        (DatasetActivityLogService().send_schema_create_event, [], 'schema_create', 'user'),
    ],
)
async def test_send_schema_events_send_correct_msg(
    test_db, kafka_dataset_consumer, schema, method, change, activity, username
):
    dataset = await SrvDatasetMgr().get_bygeid(test_db, geid=schema.dataset_geid)
    if change:
        await method(schema, dataset, username, change)
    else:
        await method(schema, dataset, username)

    msg = await kafka_dataset_consumer.getone()

    # schema_loaded = schema.load_schema('app/schemas/metadata.items.activity.avsc')
    schema_loaded = avro_schema.load_schema('app/schemas/dataset.activity.avsc')
    activity_log = schemaless_reader(io.BytesIO(msg.value), schema_loaded)

    activity_log_schema = DatasetActivityLogSchema.parse_obj(activity_log)

    assert not activity_log_schema.version
    assert activity_log_schema.container_code == dataset.code
    assert activity_log_schema.user == username
    assert not activity_log_schema.version
    assert activity_log_schema.target_name == schema.name
    assert activity_log_schema.activity_type == activity
    assert activity_log_schema.activity_time == activity_log['activity_time']
    assert activity_log_schema.changes == change
