"""
Shared HPOA testing code
"""

from typing import Optional, Iterable, Any

from biolink_model.datamodel.pydanticmodel_v2 import NamedThing, Association, RetrievalSource

# list of slots whose values are
# to be checked in a result node
NODE_TEST_SLOTS = [
    "id",
    "name",
    "category",
    "provided_by",
    "inheritance"
]

# list of slots whose values are
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

def flatten_sources(sources: list[RetrievalSource]) -> list[dict[str, str]]:
    flat_sources: list[dict[str, str]] = []
    source: RetrievalSource
    for source in sources:
        flat_sources.append({"resource_id": source.resource_id, "resource_role": source.resource_role})
    return flat_sources

def validate_sources(expected: dict[str, str], returned: list[dict[str, str]]) -> bool:
    """
    Validates selected field content of Association.sources list of RetrievalSource instances.
    :param expected: dict[str, str] of (selective) expected field values
    :param returned: list[dict[str, str]] key fields extracted from sources associated with an edge
    :return: bool, True if validation passed
    """
    return any(
        [
            expected["resource_id"] in entry["resource_id"] and
            expected["resource_role"] in entry["resource_role"]
            for entry in returned
        ]
    )

def _compare_slot_values(returned_value, expected_value):
    return (
        returned_value == expected_value
        or (
            isinstance(returned_value, list) and
            isinstance(expected_value, list) and
            set(returned_value) == set(expected_value)
        )
    )

def transform_test_runner(
        result: tuple[Iterable[NamedThing], Iterable[Association]],
        expected_nodes: Optional[list],
        expected_edge: Optional[dict]
):
    """
    A generic method for testing the result of a single
    transform_record() method invocation, against expectations.
    :param result: The output from a single transform_record() method call to be tested
    :param expected_nodes: An optional list of expected nodes
    :param expected_edge: An optional expected edge (as a Python dictionary of field slot names and values)
    :return: None
    :raises: AssertionError if expectations are not met
    """
    nodes: Iterable[NamedThing] = result[0]
    edges: Iterable[Association] = result[1]

    # Convert the 'nodes' Iterable NamedThing content into
    # a list of Python dictionaries by comprehension
    node: NamedThing
    transformed_nodes: list[dict[str,Any]] = [dict(node) for node in nodes]

    if expected_nodes is None:
        # Check for empty 'transformed_nodes' expectations
        assert not transformed_nodes, \
            f"unexpected non-empty set of nodes: {','.join([str(dict(node)) for node in transformed_nodes])}!"
    else:
        assert transformed_nodes, f"Expected a non-empty set of nodes to be returned!"

        # if nodes are returned, then are they the expected ones?
        # for uniformity in checking details, we convert the
        # expected_nodes to a list of node content dictionaries
        # if 'node' is not a string, it needs to be a dictionary otherwise this fails!
        expected_nodes_list: list[dict[str,Any]] = list()
        for node in expected_nodes:
            if isinstance(node, str):
                expected_nodes_list.append({"id": node})
            elif isinstance(node, dict):
                expected_nodes_list.append(node)
            else:
                assert False, f"Unexpected value type in the list of expected nodes: '{str(node)}'"

        for node_property in NODE_TEST_SLOTS:
            for expected_node in expected_nodes_list:
                if node_property not in expected_node:
                    continue
                expected_node_value = expected_node[node_property]
                assert any(
                    [
                        _compare_slot_values(returned_node[node_property], expected_node_value)
                        for returned_node in transformed_nodes if node_property in returned_node
                    ]
                ), (f"Expected node value '{expected_node_value}' for slot '{node_property}'"
                    f" not returned in transformed list of nodes: '{transformed_nodes}' ")

    # Convert the 'edges' Iterable content
    # into a list by comprehension
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

        returned_sources: Optional[list[dict[str, str]]]

        # Check values in expected edge slots of parsed edges
        for association_slot in ASSOCIATION_TEST_SLOTS:
            # I only bother with this if the slot is included in the
            # 'returned_edge' datum (as defined by the Biolink Pydantic data model)
            # and is also in the list of slots in the 'expected_edge' test data.
            if association_slot in returned_edge and \
                    association_slot in expected_edge:
                slot_values = str(returned_edge.get(association_slot,"Empty!"))
                if isinstance(expected_edge[association_slot], list):
                    # We only pass things through if *both* of the returned and the
                    # expected list of slot values are or are not empty (namely not XOR).
                    assert not (bool(expected_edge[association_slot]) ^ bool(returned_edge[association_slot])), \
                        f"Unexpected return values '{slot_values}' for slot '{association_slot}' in edge"
                    # ...but we only specifically validate non-empty expectations
                    if expected_edge[association_slot]:
                        returned_sources = None
                        for entry in expected_edge[association_slot]:
                            if isinstance(entry, str):
                                # Simple Membership value test.
                                assert entry in returned_edge[association_slot], \
                                    f"Value '{entry}' for slot '{association_slot}' is missing in returned edge values '{slot_values}?'"
                            elif isinstance(entry, dict):
                                # A more complex validation of field
                                # content, e.g. Association.sources
                                if association_slot == 'sources':
                                    if returned_sources is None:
                                        returned_sources = flatten_sources(returned_edge[association_slot])
                                    assert validate_sources(expected=entry, returned=returned_sources), \
                                        f"Invalid returned sources '{returned_sources}'"
                            else:
                                assert False, f"Unexpected value type for {str(expected_edge[association_slot])} for slot '{association_slot}'"
                else:
                    # Scalar value test
                    assert returned_edge[association_slot] == expected_edge[association_slot], \
                        f"Value '{expected_edge[association_slot]}' for slot '{association_slot}' not equal to "+\
                        f"returned edge value '{returned_edge[association_slot]}'?"
