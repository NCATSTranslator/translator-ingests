"""
Shared HPOA testing code
"""
import pytest

from typing import Optional, Dict, Iterable, List, Tuple

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


def transform_test_runner(
        result: Tuple[Iterable[NamedThing], Iterable[Association]],
        expected_nodes: Optional[List],
        expected_edge: Optional[Dict]
):
    nodes: Iterable[NamedThing] = result[0]
    edges: Iterable[Association] = result[1]

    # Convert the 'nodes' Iterable content into a List by comprehension
    node: NamedThing
    transformed_nodes = [node.id for node in nodes]

    if expected_nodes is None:
        # Check for empty 'transformed_nodes' expectations
        assert not transformed_nodes, \
            f"unexpected non-empty 'nodes' list: {','.join(transformed_nodes)}"
    else:
        # if nodes expected, then are they the expected ones?
        for node_id in transformed_nodes:
            assert node_id in expected_nodes

    # Convert the 'edges' Iterable content into a List by comprehension
    edge: Association
    transformed_edges = [dict(edge) for edge in edges]

    if expected_edge is None:
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
            if slot in expected_edge:
                if isinstance(expected_edge[slot], list):
                    # Membership value test.
                    # First, check if both returned and expected lists of
                    # slot values are empty. If so, then the assertion is passed.
                    # Otherwise, check if an expected slot value is seen in the returned list.
                    assert not (expected_edge[slot] or returned_edge[slot]) \
                           or expected_edge[slot][0] in returned_edge[slot], \
                        f"Value for slot '{slot}' missing in returned edge values '{','.join(returned_edge[slot])}?'"
                else:
                    # Scalar value test
                    assert returned_edge[slot] == expected_edge[slot], \
                        f"Value for slot '{slot}' not equal to returned edge value '{returned_edge[slot]}'?"
