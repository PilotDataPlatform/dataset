import pytest


@pytest.fixture
def schema_essential_template(db_session):
    from app.config import ConfigClass
    from app.models.schema import DatasetSchemaTemplate

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'
    schema_template_geid = 'ef4eb37d-6d81-46a7-a9d9-db71bf44edc7'

    new_template = DatasetSchemaTemplate(
        geid=schema_template_geid,
        dataset_geid=dataset_geid,
        name=ConfigClass.ESSENTIALS_TPL_NAME,
        standard='default',
        system_defined=True,
        is_draft=True,
        content={},
        creator='admin',
    )
    db_session.add(new_template)
    db_session.commit()
    yield new_template.to_dict()
    db_session.delete(new_template)
    db_session.commit()


@pytest.fixture
def bids_results(db_session):
    from app.models.bids import BIDSResult

    dataset_geid = '5baeb6a1-559b-4483-aadf-ef60519584f3'

    new_bid_result = BIDSResult(dataset_geid=dataset_geid, validate_output={})
    db_session.add(new_bid_result)
    db_session.commit()
    yield new_bid_result.to_dict()
    db_session.delete(new_bid_result)
    db_session.commit()
