"""
An example test file for the transform script.

It uses pytest fixtures to define the input data and the mock koza transform.
The test_example function then tests the output of the transform script.

See the Koza documentation for more information on testing transforms:
https://koza.monarchinitiative.org/Usage/testing/
"""

import pytest
from koza.utils.testing_utils import mock_koza

# Define the ingest name and transform script path
INGEST_NAME = "alliance_phenotype"
TRANSFORM_SCRIPT = "./src/alliance_phenotype_association_ingest/transform.py"


@pytest.fixture
def map_cache():
    return {"alliance-entity-lookup":
        {
          "RGD:61958": {"category": "biolink:Gene"},
          "MGI:87853": {"category": "biolink:Gene"},
          "MGI:2652709": {"category": "biolink:Genotype"},
          "MGI:1855936": {"category": "biolink:SequenceVariant"}
       }}


@pytest.fixture
def rat_gene(mock_koza, map_cache):
    row = {
        "dateAssigned": "2006-10-25T18:06:17.000-05:00",
        "evidence": {
            "crossReference": {"id": "RGD:1357201", "pages": ["reference"]},
            "publicationId": "PMID:11549339",
        },
        "objectId": "RGD:61958",
        "phenotypeStatement": "cardiac hypertrophy",
        "phenotypeTermIdentifiers": [{"termId": "MP:0001625", "termOrder": 1}],
    }

    return mock_koza(
        INGEST_NAME,
        row,
        TRANSFORM_SCRIPT,
        map_cache=map_cache
    )

def test_rat_gene(rat_gene):
    assert len(rat_gene) == 1
    association = rat_gene[0]
    assert association.category == ['biolink:GeneToPhenotypicFeatureAssociation']
    assert association.subject == 'RGD:61958'
    assert association.predicate == 'biolink:has_phenotype'
    assert association.object == 'MP:0001625'


@pytest.fixture
def mouse_gene(mock_koza, map_cache):
    row = {
        'objectId': 'MGI:87853',
        'phenotypeTermIdentifiers': [{'termId': 'MP:0005118', 'termOrder': 1}],
        'phenotypeStatement': 'decreased circulating pituitary hormone level',
        'evidence': {'publicationId': 'PMID:9877102', 'crossReference': {'id': 'MGI:1328537', 'pages': ['reference']}},
        'primaryGeneticEntityIDs': ['MGI:3802742'],
        'dateAssigned': '2008-08-19T00:00:00-04:00',
    }

    return mock_koza(
        INGEST_NAME,
        row,
        TRANSFORM_SCRIPT,
        map_cache=map_cache
    )

def test_mouse_gene(mouse_gene):
    assert len(mouse_gene) == 1
    association = mouse_gene[0]
    assert association.category == ['biolink:GeneToPhenotypicFeatureAssociation']
    assert association.subject == 'MGI:87853'
    assert association.predicate == 'biolink:has_phenotype'
    assert association.object == 'MP:0005118'
    assert association.publications == ['PMID:9877102']

@pytest.fixture
def mouse_genotype(mock_koza, map_cache):
    row = {'objectId': 'MGI:2652709', 'phenotypeTermIdentifiers': [{'termId': 'MP:0000067', 'termOrder': 1}],
     'phenotypeStatement': 'osteopetrosis',
     'evidence': {'publicationId': 'PMID:8018921', 'crossReference': {'id': 'MGI:67289', 'pages': ['reference']}},
     'dateAssigned': '2010-04-05T00:00:00-04:00'}

    return mock_koza(
        INGEST_NAME,
        row,
        TRANSFORM_SCRIPT,
        map_cache=map_cache
    )

def test_mouse_genotype(mouse_genotype):
    assert len(mouse_genotype) == 1
    association = mouse_genotype[0]
    assert association.category == ['biolink:GenotypeToPhenotypicFeatureAssociation']
    assert association.subject == 'MGI:2652709'
    assert association.predicate == 'biolink:has_phenotype'
    assert association.object == 'MP:0000067'
    assert association.publications == ['PMID:8018921']

@pytest.fixture
def mouse_allele(mock_koza, map_cache):
    row = {'objectId': 'MGI:1855936', 'phenotypeTermIdentifiers': [{'termId': 'MP:0008296', 'termOrder': 1}], 'phenotypeStatement': 'abnormal adrenal gland x-zone morphology', 'evidence': {'publicationId': 'PMID:7976166', 'crossReference': {'id': 'MGI:67485', 'pages': ['reference']}}, 'primaryGeneticEntityIDs': ['MGI:3789003'], 'dateAssigned': '2008-05-22T00:00:00-04:00'}
    return mock_koza(
        INGEST_NAME,
        row,
        TRANSFORM_SCRIPT,
        map_cache=map_cache
    )

def test_mouse_allele(mouse_allele):
    assert len(mouse_allele) == 1
    association = mouse_allele[0]
    assert association.category == ['biolink:VariantToPhenotypicFeatureAssociation']
    assert association.subject == 'MGI:1855936'
    assert association.predicate == 'biolink:has_phenotype'
    assert association.object == 'MP:0008296'
    assert association.publications == ['PMID:7976166']
