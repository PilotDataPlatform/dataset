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

import pytest_asyncio


@pytest_asyncio.fixture
async def schema_essential_template(db_session):
    from app.config import ConfigClass
    from app.models.schema import DatasetSchemaTemplate

    schema_template_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'

    new_template = DatasetSchemaTemplate(
        geid=schema_template_geid,
        dataset_geid=None,
        name=ConfigClass.ESSENTIALS_TPL_NAME,
        standard='default',
        system_defined=True,
        is_draft=True,
        content={},
        creator='admin',
    )
    db_session.add(new_template)
    await db_session.commit()
    await db_session.refresh(new_template)
    yield new_template.to_dict()
    await db_session.delete(new_template)


@pytest_asyncio.fixture
async def bids_results(db_session):
    from app.models.bids import BIDSResult

    dataset_geid = '7b5b0948-958b-4eb5-afa9-3780b1c6b718'

    new_bid_result = BIDSResult(dataset_geid=dataset_geid, validate_output={})
    db_session.add(new_bid_result)
    await db_session.commit()
    await db_session.refresh(new_bid_result)
    yield new_bid_result.to_dict()
    await db_session.delete(new_bid_result)
