"""add system defined schema templates.

Revision ID: 85fc09674dfc
Revises: dfc9500abfb3
Create Date: 2022-07-13 17:26:04.108152
"""
from uuid import uuid4

from alembic import op
from sqlalchemy import orm

from app.models.schema import DatasetSchemaTemplate

# revision identifiers, used by Alembic.
revision = '85fc09674dfc'
down_revision = 'dfc9500abfb3'
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    session = orm.Session(bind=bind)
    templates = []
    base_templates = [
        {
            'name': 'Distribution',
            'standard': 'default',
            'system_defined': True,
            'is_draft': False,
            'content': {
                'ui': {'ui:order': ['dataset_distribution_landing_page', '*']},
                'schema': {
                    'type': 'object',
                    'required': ['dataset_distribution_landing_page'],
                    'properties': {
                        'dataset_distribution_format': {
                            'type': 'array',
                            'items': {'type': 'string'},
                            'title': 'Dataset Distribution Technical Format',
                        },
                        'dataset_distribution_landing_page': {
                            'type': 'string',
                            'title': 'Dataset Distribution Access Landing Page',
                            'format': 'uri',
                        },
                        'dataset_distribution_authorization': {
                            'enum': ['Public', 'Registered', 'Private'],
                            'type': 'string',
                            'title': 'Dataset Distribution Access Authorization',
                        },
                    },
                },
            },
            'creator': 'admin',
        },
        {
            'name': 'Open_minds',
            'standard': 'open_minds',
            'system_defined': True,
            'is_draft': False,
            'content': {'ui': {}, 'schema': {}},
            'creator': 'admin',
        },
        {
            'name': 'Disease',
            'standard': 'default',
            'system_defined': True,
            'is_draft': False,
            'content': {
                'ui': {},
                'schema': {
                    'type': 'object',
                    'required': ['dataset_disease_name'],
                    'properties': {
                        'dataset_disease_name': {'type': 'string', 'title': 'Disease Name'},
                        'daatset_disease_dates': {
                            'type': 'string',
                            'title': 'Disease Diagnosis Date',
                            'format': 'date',
                        },
                        'dataset_disease_status': {'type': 'string', 'title': 'Disease Status'},
                        'dataset_disease_identifier': {'type': 'string', 'title': 'Identifier'},
                        'dataset_disease_identifier_source': {'type': 'string', 'title': 'Identifier Source'},
                    },
                },
            },
            'creator': 'admin',
        },
        {
            'name': 'Contributors',
            'standard': 'default',
            'system_defined': True,
            'is_draft': False,
            'content': {
                'ui': {
                    'dataset_contributors': {
                        'items': {
                            'ui:order': [
                                'dataset_contributor_person_firstname',
                                'dataset_contributor_person_lastname',
                                'dataset_contributor_person_email',
                                'dataset_contributor_organization_name',
                                'dataset_contributor_organization_abbreviation',
                            ]
                        }
                    }
                },
                'schema': {
                    'type': 'object',
                    'properties': {
                        'dataset_contributors': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'anyOf': [
                                    {
                                        'title': 'Person',
                                        'required': [
                                            'dataset_contributor_person_firstname',
                                            'dataset_contributor_person_lastname',
                                            'dataset_contributor_person_email',
                                        ],
                                        'properties': {
                                            'dataset_contributor_person_email': {
                                                'type': 'string',
                                                'title': 'Email',
                                                'format': 'email',
                                            },
                                            'dataset_contributor_person_lastname': {
                                                'type': 'string',
                                                'title': 'Last Name',
                                            },
                                            'dataset_contributor_person_firstname': {
                                                'type': 'string',
                                                'title': 'First Name',
                                            },
                                        },
                                    },
                                    {
                                        'title': 'Organization',
                                        'required': ['dataset_contributor_organization_name'],
                                        'properties': {
                                            'dataset_contributor_organization_name': {
                                                'type': 'string',
                                                'title': 'Full Name',
                                            },
                                            'dataset_contributor_organization_abbreviation': {
                                                'type': 'string',
                                                'title': 'Short Name',
                                            },
                                        },
                                    },
                                ],
                            },
                            'title': 'Contributors Type',
                            'minItems': 1,
                        }
                    },
                },
            },
            'creator': 'admin',
        },
        {
            'name': 'Subjects',
            'standard': 'default',
            'system_defined': True,
            'is_draft': False,
            'content': {
                'ui': {},
                'schema': {
                    'type': 'object',
                    'properties': {
                        'dataset_subjects': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'required': ['subject_id', 'subject_species', 'subject_sex', 'subject_agecategory'],
                                'properties': {
                                    'subject_id': {'type': 'string', 'title': 'Subject ID'},
                                    'subject_sex': {
                                        'enum': ['Female', 'Male', 'Unknown', 'Other'],
                                        'type': 'string',
                                        'title': 'Subject Sex',
                                    },
                                    'subject_species': {
                                        'enum': [
                                            'Homo sapiens',
                                            'Macaca fascicularis',
                                            'Macaca mulatta',
                                            'Mus musculus',
                                            'Mustela putorius',
                                            'Rattus norvegicus',
                                            'Other',
                                        ],
                                        'type': 'string',
                                        'title': 'Subject Species',
                                    },
                                    'subject_agecategory': {
                                        'enum': [
                                            'Neonate',
                                            'Infant',
                                            'Juvenile',
                                            'Young adult',
                                            'Adult',
                                            'Unknown',
                                            'Other',
                                        ],
                                        'type': 'string',
                                        'title': 'Subject Age Category',
                                    },
                                },
                            },
                            'title': 'Subjects',
                            'minItems': 1,
                        }
                    },
                },
            },
            'creator': 'admin',
        },
        {
            'name': 'Grant',
            'standard': 'default',
            'system_defined': True,
            'is_draft': False,
            'content': {
                'ui': {'dataset_grant_funder': {'ui:options': {'orderable': False}}},
                'schema': {
                    'type': 'object',
                    'required': ['dataset_grant_name'],
                    'properties': {
                        'dataset_grant_name': {'type': 'string', 'title': 'Grant Name'},
                        'dataset_grant_funder': {
                            'type': 'array',
                            'items': {
                                'type': 'object',
                                'anyOf': [
                                    {
                                        'title': 'Person',
                                        'required': [
                                            'dataset_contributor_person_firstname',
                                            'dataset_contributor_person_lastname',
                                            'dataset_contributor_person_email',
                                        ],
                                        'properties': {
                                            'dataset_contributor_person_email': {
                                                'type': 'string',
                                                'title': 'Email',
                                                'format': 'email',
                                            },
                                            'dataset_contributor_person_lastname': {
                                                'type': 'string',
                                                'title': 'Last Name',
                                            },
                                            'dataset_contributor_person_firstname': {
                                                'type': 'string',
                                                'title': 'First Name',
                                            },
                                        },
                                    },
                                    {
                                        'title': 'Organization',
                                        'required': ['dataset_contributor_organization_name'],
                                        'properties': {
                                            'dataset_contributor_organization_name': {
                                                'type': 'string',
                                                'title': 'Full Name',
                                            },
                                            'dataset_contributor_organization_abbreviation': {
                                                'type': 'string',
                                                'title': 'Short Name',
                                            },
                                        },
                                    },
                                ],
                            },
                            'title': 'Grant Funder',
                        },
                    },
                },
            },
            'creator': 'admin',
        },
        {
            'name': 'Essential',
            'standard': 'default',
            'system_defined': True,
            'is_draft': False,
            'content': {
                'ui': {
                    'ui:order': [
                        'dataset_title',
                        'dataset_code',
                        'dataset_type',
                        'dataset_authors',
                        'dataset_description',
                        'dataset_modality',
                        'dataset_collection_method',
                        'dataset_license',
                        'dataset_tags',
                        'dataset_subject_number',
                        'dataset_identifier',
                        'dataset_identifier_source',
                        'dataset_derived_from',
                        'parent_dataset_identifier',
                        'parent_dataset_identifier_source',
                        'dataset_publication_title',
                        'dataset_publication_identifier',
                        'dataset_publication_identifier_source',
                        '*',
                    ],
                    'dataset_code': {'ui:readonly': True},
                    'dataset_type': {'ui:options': {'allowClear': False}},
                    'dataset_description': {'ui:widget': 'textarea', 'ui:options': {'rows': 6}},
                },
                'schema': {
                    'type': 'object',
                    'required': ['dataset_title', 'dataset_code', 'dataset_description', 'dataset_authors'],
                    'properties': {
                        'dataset_code': {'type': 'string', 'title': 'Dataset Code', 'maxLength': 32},
                        'dataset_tags': {
                            'type': 'array',
                            'items': {'type': 'string', 'maxLength': 20},
                            'title': 'Tags',
                            'maxItems': 10,
                            'uniqueItems': True,
                        },
                        'dataset_type': {'enum': ['GENERAL', 'BIDS'], 'type': 'string', 'title': 'Type'},
                        'dataset_title': {'type': 'string', 'title': 'Title', 'maxLength': 100},
                        'dataset_authors': {
                            'type': 'array',
                            'items': {'type': 'string', 'maxLength': 50},
                            'title': 'Authors',
                            'maxItems': 10,
                            'minItems': 1,
                            'uniqueItems': True,
                        },
                        'dataset_license': {'type': 'string', 'title': 'License', 'maxLength': 20},
                        'dataset_modality': {
                            'type': 'array',
                            'items': {
                                'enum': [
                                    'anatomical approach',
                                    'behavioral approach',
                                    'cell counting',
                                    'cell morphology',
                                    'cell population characterization',
                                    'cell population imaging',
                                    'computational modeling',
                                    'electrophysiology',
                                    'histological approach',
                                    'microscopy',
                                    'molecular expression approach',
                                    'molecular expression characterization',
                                    'morphological approach',
                                    'multimodal approach',
                                    'neural connectivity',
                                    'neuroimaging',
                                    'physiological approach',
                                ],
                                'type': 'string',
                            },
                            'title': 'Modality',
                            'uniqueItems': True,
                        },
                        'dataset_identifier': {'type': 'string', 'title': 'Dataset Identifier'},
                        'dataset_description': {'type': 'string', 'title': 'Description', 'maxLength': 5000},
                        'dataset_derived_from': {'type': 'string', 'title': 'Derived From'},
                        'dataset_subject_number': {'type': 'integer', 'title': 'Number of Subjects', 'minimum': 0},
                        'dataset_collection_method': {
                            'type': 'array',
                            'items': {'type': 'string', 'maxLength': 20},
                            'title': 'Collection Method',
                            'maxItems': 10,
                            'uniqueItems': True,
                        },
                        'dataset_identifier_source': {'type': 'string', 'title': 'Dataset Identifier Source'},
                        'dataset_publication_title': {'type': 'string', 'title': 'Related Publication Title'},
                        'parent_dataset_identifier': {'type': 'string', 'title': 'Parent Dataset Identifier'},
                        'dataset_publication_identifier': {'type': 'string', 'title': 'Related Publication Identifier'},
                        'parent_dataset_identifier_source': {
                            'type': 'string',
                            'title': 'Parent Dataset Identifier Source',
                        },
                        'dataset_publication_identifier_source': {
                            'type': 'string',
                            'title': 'Related Publication Identifier Source',
                        },
                    },
                },
            },
            'creator': 'admin',
        },
    ]
    for template in base_templates:
        templates.append(
            DatasetSchemaTemplate(
                **{
                    **template,
                    'geid': str(uuid4()),
                }
            )
        )
    session.add_all(templates)
    session.commit()


def downgrade():
    bind = op.get_bind()
    session = orm.Session(bind=bind)
    templates = session.query(DatasetSchemaTemplate).where(DatasetSchemaTemplate.system_defined.is_(True))
    for template in templates:
        session.delete(template)
    session.commit()
