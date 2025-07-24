import pytest

from typing import Optional, Dict, Iterable, List

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,Association,
    DiseaseToPhenotypicFeatureAssociation
)
from src.translator_ingest.ingests.hpoa.disease_to_phenotype_transform import transform_record

# List of slots whose values are to be check in a result edge
ASSOCIATION_TEST_SLOTS = [
    "category",
    "subject",
    "predicate",
    "negated",
    "object",
    "publications",
    "has_evidence",
    "sex_qualifier",
    "onset_qualifier",
    "has_percentage",
    "has_quotient",
    "frequency_qualifier"
]

@pytest.mark.parametrize(
    "test_record,result_nodes,result_edge",
    [
        (  # Query 0 - missing data (empty 'hpo_id' field)
            {},
            None,
            None
        ),
        (  # Query 1 - An 'aspect' == 'C' record processed
            {
                "database_id": "OMIM:614856",
                "disease_name": "Osteogenesis imperfecta, type XIII",
                "qualifier": "NOT",
                "hpo_id": "HP:0000343",
                "reference": "OMIM:614856",
                "evidence": "TAS",
                "onset": "HP:0003593",
                "frequency": "1/1",
                "sex": "FEMALE",
                "modifier": "",
                "aspect": "C",  # assert 'Clinical' test record
                "biocuration": "HPO:skoehler[2012-11-16]",
            },
            # This is not a 'P' record, so it should be skipped
            None,
            None
        ),
        (  # Query 2 - An 'aspect' == 'P' record processed
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                "qualifier": "",
                "hpo_id": "HP:0001249",
                "reference": "OMIM:117650",
                "evidence": "TAS",
                "onset": "",
                "frequency": "50%",
                "sex": "",
                "modifier": "",
                "aspect": "P",
                "biocuration": "HPO:probinson[2009-02-17]",
            },
            # Captured node identifiers
            ["OMIM:117650", "HP:0001249"],
            # Captured edge contents
            {
                "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
                "subject": "OMIM:117650",
                "predicate": "biolink:has_phenotype",
                "negated": False,
                "object": "HP:0001249",
                # Although "OMIM:117650" is recorded above as
                # a reference, it is not used as a publication
                "publications": [],
                "has_evidence": ["ECO:0000304"],
                "sex_qualifier": None,
                "onset_qualifier": None,
                "has_percentage": 50.0,
                "has_quotient": None,
                # '50%' above implies HPO term that the phenotype
                # is 'Present in 30% to 79% of the cases'.
                "frequency_qualifier": "HP:0040282"
                # We still need to fix the 'sources' serialization
                # in Pydantic before somehow testing the following
                # "primary_knowledge_source": "infores:hpo-annotations"
                # assert "infores:monarchinitiative" in association.aggregator_knowledge_source
            }
        ),
        (  # Query 3 - Another 'aspect' == 'P' record processed
            {
                "database_id": "OMIM:117650",
                "disease_name": "Cerebrocostomandibular syndrome",
                "qualifier": "",
                "hpo_id": "HP:0001545",
                "reference": "OMIM:117650",
                "evidence": "TAS",
                "onset": "",
                "frequency": "HP:0040283",
                "sex": "",
                "modifier": "",
                "aspect": "P",
                "biocuration": "HPO:skoehler[2017-07-13]",
            },
            ["OMIM:117650", "HP:0001545"],
            {
                "category": ["biolink:DiseaseToPhenotypicFeatureAssociation"],
                "subject": "OMIM:117650",
                "predicate": "biolink:has_phenotype",
                "negated": False,
                "object": "HP:0001545",
                "publications": "OMIM:117650",
                "has_evidence": "ECO:0000304",
                "sex_qualifier": None,
                "onset_qualifier": None,
                "has_percentage": None,
                "has_quotient": None,
                "frequency_qualifier": "HP:0040283",
            #     "primary_knowledge_source": "infores:hpo-annotations"
            #     assert "infores:monarchinitiative" in association.aggregator_knowledge_source

            }
        )
    ]
)
def test_disease_to_phenotype_transform(
        test_record: Dict,
        result_nodes: Optional[List],
        result_edge: Optional[Dict]
):
    nodes: Iterable[NamedThing]
    edges: Iterable[Association]

    # Call the ingest parser function on the mock_record
    nodes, edges = transform_record(test_record)

    # Convert the 'nodes' Iterable content into a List by comprehension
    node: NamedThing
    transformed_nodes = [node.id for node in nodes]

    if result_nodes is None:
        # Check for empty 'transformed_nodes' expectations
        assert not transformed_nodes, \
            f"unexpected non-empty 'nodes' list: {','.join(transformed_nodes)}"
    else:
        # if nodes expected, then are they the expected ones?
        for node_id in transformed_nodes:
            assert node_id in result_nodes

    # Convert the 'edges' Iterable content into a List by comprehension
    edge: Association
    transformed_edges = [dict(edge) for edge in edges]

    if result_edge is None:
        # Check for empty 'transformed_edges' expectations
        assert not transformed_edges, \
            f"Unexpected non-empty result list of edges: '{','.join(str(transformed_edges)[0:20])}'..."
    else:
        # Check contents of edge(s) returned.
        # For HPOA transformation, only one edge is expected to be returned?
        assert len(transformed_edges) == 1

        # then grab it for content assessment
        returned_edge = transformed_edges[0]

        # Check values in expected edge slots of parsed edges
        for slot in ASSOCIATION_TEST_SLOTS:
            if slot in result_edge:
                if isinstance(result_edge[slot], list):
                    # Membership value test.
                    # First, check if both returned and expected lists of
                    # slot values are empty. If so, then the assertion is passed.
                    # Otherwise, check if an expected slot value is seen in the returned list.
                    assert not (result_edge[slot] or returned_edge[slot]) \
                            or result_edge[slot][0] in returned_edge[slot], \
                        f"Value for slot '{slot}' missing in returned edge values '{','.join(returned_edge[slot])}?'"
                else:
                    # Scalar value test
                    assert returned_edge[slot] == result_edge[slot], \
                        f"Value for slot '{slot}' not equal to returned edge value '{returned_edge[slot]}'?"
