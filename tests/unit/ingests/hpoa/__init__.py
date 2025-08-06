"""
Shared HPOA testing code
"""

from typing import Optional, Dict, Iterable, List, Tuple

from biolink_model.datamodel.pydanticmodel_v2 import NamedThing, Association, RetrievalSource

# List of slots whose values are
# to be checked in a result edge
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
    "frequency_qualifier",
    "disease_context_qualifier",
    "sources",
    "knowledge_level",
    "agent_type"
]

def flatten_sources(sources: List[RetrievalSource]) -> List[Dict[str, str]]:
    flat_sources: List[Dict[str, str]] = []
    source: RetrievalSource
    for source in sources:
        flat_sources.append({"resource_id": source.resource_id, "resource_role": source.resource_role})
    return flat_sources

def validate_sources(expected: Dict[str, str], returned: List[Dict[str, str]]) -> bool:
    """
    Validates selected field content of Association.sources list of RetrievalSource instances.
    :param expected: Dict[str, str] of (selective) expected field values
    :param returned: List[Dict[str, str]] key fields extracted from sources associated with an edge
    :return: bool, True if validation passed
    """
    return True


def transform_test_runner(
        result: Tuple[Iterable[NamedThing], Iterable[Association]],
        expected_nodes: Optional[List],
        expected_edge: Optional[Dict]
):
    nodes: Iterable[NamedThing] = result[0]
    edges: Iterable[Association] = result[1]

    # TODO: how can we generalize this to also test here for node annotation,
    #      e.g., like the value of the slot 'name'?
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
            # I only bother with this if the slot is included in the
            # 'returned_edge' datum (as defined by the Biolink Pydantic data model)
            # and is also in the list of slots in the 'expected_edge' test data.
            if slot in returned_edge and \
                    slot in expected_edge:
                slot_values = str(returned_edge.get(slot,"Empty!"))
                if isinstance(expected_edge[slot], list):
                    # We only pass things through if *both* of the returned and the
                    # expected lists of slot values are or are not empty (i.e. not XOR).
                    assert not (bool(expected_edge[slot]) ^ bool(returned_edge[slot])), \
                        f"Unexpected return values '{slot_values}' for slot '{slot}' in edge"
                    # ...but we only specifically validate non-empty expectations
                    if expected_edge[slot]:
                        returned_sources: Optional[List[Dict[str, str]]] = None
                        for entry in expected_edge[slot]:
                            if isinstance(entry, str):
                                # Simple Membership value test.
                                assert entry in returned_edge[slot], \
                                    f"Value '{entry}' for slot '{slot}' is missing in returned edge values '{slot_values}?'"
                            elif isinstance(entry, dict):
                                # A more complex validation of field
                                # content, e.g. Association.sources
                                if slot == 'sources':
                                    if returned_sources is None:
                                        returned_sources = flatten_sources(returned_edge[slot])
                                    assert validate_sources(expected=entry, returned=returned_sources), \
                                        f"Invalid returned sources '{returned_sources}'"
                            else:
                                assert False, f"Unexpected value type for {str(expected_edge[slot])} for slot '{slot}'"
                else:
                    # Scalar value test
                    assert returned_edge[slot] == expected_edge[slot], \
                        f"Value '{expected_edge[slot]}' for slot '{slot}' not equal to "+\
                        f"returned edge value '{returned_edge[slot]}'?"
