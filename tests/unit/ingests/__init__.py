"""
Generic utility code for use in the ingest unit tests.

The main function of interest is validate_transform_result(), which is used to test
the output of a single @koza.transform_record() decorated method invocation, looking for the
expected content in node and edge slots, with test expectations defined by constraints
'expected_nodes', 'expected_edge', 'node_test_slots' and 'association_test_slots'
"""
from multiprocessing.managers import rebuild_as_list

import pytest
from typing import Optional, Iterable, Any, Iterator

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter
from koza.model.graphs import KnowledgeGraph
from koza.transform import Record

from biolink_model.datamodel.pydanticmodel_v2 import NamedThing, Association, RetrievalSource

class MockKozaWriter(KozaWriter):
    """
    Mock "do nothing" implementation of a KozaWriter
    """
    def __init__(self):
        self.items = []

    def write(self, entities: Iterable):
        if isinstance(entities, list):
            self.items.extend(entities)
        else:
            for entity in entities:
                self.items.append(entity)

    def write_nodes(self, nodes: Iterable):
        self.items.extend(nodes)

    def write_edges(self, edges: Iterable):
        self.items.extend(edges)

    def finalize(self):
        pass

class MockKozaTransform(koza.KozaTransform):
    """
    Mock "do nothing" implementation of a KozaTransform
    """
    @property
    def current_reader(self) -> str:
        return ""

    @property
    def data(self) -> Iterator[Record]:
        record: Record = dict()
        yield record

@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)


def flatten_sources(sources: list[RetrievalSource]) -> list[dict[str, str]]:
    flat_sources: list[dict[str, str]] = []
    source: RetrievalSource
    for source in sources:
        flat_sources.append({"resource_id": source.resource_id, "resource_role": source.resource_role})
    return flat_sources


def validate_sources(expected: dict[str, str], returned: list[dict[str, str]]) -> bool:
    """
    Validates selected field content the Association.sources list of RetrievalSource instances.
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


def _match_edge(
        returned_edge:dict,
        expected_edge:dict,
        target_slots: list[str]
) -> Optional[str]:
    returned_sources: Optional[list[dict[str, str]]]
    # We only bother with a comparison if the slot is included in both the
    # 'returned_edge' datum (as defined by the Biolink Pydantic data model)
    # and in the list of slots in the 'expected_edge' test data.
    for association_slot in target_slots:
        if association_slot in returned_edge and association_slot in expected_edge:

            reasv = returned_edge[association_slot]

            # We only pass things through if *both* of the returned and the
            # expected the lists of slot values are or are not empty (namely not XOR).
            if isinstance(expected_edge[association_slot], list):
                if bool(expected_edge[association_slot]) ^ bool(reasv):
                    return f"Unexpected return values '{reasv}' for slot '{association_slot}' in edge"
                # ...but we only specifically validate non-empty expectations
                if expected_edge[association_slot]:
                    returned_sources = None
                    for entry in expected_edge[association_slot]:
                        if isinstance(entry, str):
                            # Simple Membership value test.
                            if entry not in reasv:
                                return f"Value '{entry}' for slot '{association_slot}' " + \
                                f"is missing in returned edge values '{reasv}?'"
                        elif isinstance(entry, dict):
                            # A more complex validation of field
                            # content, e.g., Association.sources
                            if association_slot == 'sources':
                                if returned_sources is None:
                                    returned_sources = flatten_sources(reasv)
                                if not validate_sources(expected=entry, returned=returned_sources):
                                    return f"Invalid returned sources '{returned_sources}'"
                        else:
                            return f"Unexpected value type for "+\
                                f"{str(expected_edge[association_slot])} for slot '{association_slot}'"
            else:
                # Scalar value test
                if reasv != expected_edge[association_slot]:
                    return f"Value '{expected_edge[association_slot]}' "+\
                           f"for slot '{association_slot}' not equal to returned edge value '{reasv}'?"

    # If we got to here, then success!
    # No errors were reported?
    return None

def _found_edge(
        returned_edge:dict,
        expected_edge_list:list[dict],
        target_slots: list[str]
) -> tuple[bool, Optional[list[str]]]:
    error_messages: list[str] = list()
    for expected_edge in expected_edge_list:
        error_msg: Optional[str] = _match_edge(returned_edge, expected_edge, target_slots)
        if error_msg is None:
            # Success! We found at least one match with expectation...
            return True, None

        # We track returned error messages indicating possible sources
        # missed edge expectations, but the caller will need to decide
        # for themselves exactly which specific expectation failed.
        # To guide assessment, the full list of error messages are
        # (only) returned when 'returned_edge' matches no expected edge.
        error_messages.append(error_msg)

    return False, error_messages


def validate_transform_result(
        result: KnowledgeGraph | None,
        expected_nodes: Optional[list],
        expected_edges: Optional[dict] | list[dict],
        expected_no_of_edges: int = 1,
        node_test_slots: list[str] = "id",
        association_test_slots: Optional[list] = None
):
    """
    A generic method for testing the result of a single
    transform_record() method invocation result, against
    test-defined node and edge slot content expectations.

    :param result: The koza.model.graphs.KnowledgeGraph | None from a single invocation of
                   the target **@koza.transform_record**-decorated method to be tested.
    :param expected_nodes: An optional list of expected nodes. The list values can be scalar
                           (node CURIE string identifiers expected) or dictionary of expected node slot values.
    :param expected_edges: An optional argument of either a single expected edge (as a single Python dictionary
                           of field slot names and values) or a list of such edge dictionaries.
                           The expected slot values in the dictionary can be scalars
                           or a list of dictionaries that are edge sources to match.
    :param expected_no_of_edges: The expected number of association edges returned (default: 1).
    :param node_test_slots: String list of node slots to be tested (default: 'id' - only the node 'id' slot is tested)
    :param association_test_slots: String list of edge slots to be tested (default: None - no edge slots are tested)
    :return: None
    :raises: AssertionError condition with a candidate list of possible errors, if result expectations were not met
    """
    if result is None:
        if expected_nodes is None and expected_edges is None:
            return  # we're good! No result was expected.
        else:
            assert False, "Unexpected null result from **`@koza.transform_record`** decorated method call!"

    nodes: Iterable[NamedThing] = result.nodes if result.nodes is not None else []
    edges: Iterable[Association] = result.edges if result.edges is not None else []

    # Convert the 'nodes' Iterable NamedThing content into
    # a list of Python dictionaries by comprehension
    node: NamedThing
    transformed_nodes: list[dict[str, Any]] = [dict(node) for node in nodes]

    if expected_nodes is None:
        # Check for empty 'transformed_nodes' expectations
        assert not transformed_nodes, \
            f"unexpected non-empty set of nodes: {','.join([str(dict(node)) for node in transformed_nodes])}!"
    else:

        assert transformed_nodes, "Expected a non-empty set of nodes to be returned!"

        # if nodes are returned, then are they the expected ones?
        # for uniformity in checking details, we convert the
        # expected_nodes to a list of node content dictionaries
        # if 'node' is not a string, it needs to be a dictionary otherwise this fails!
        expected_nodes_list: list[dict[str, Any]] = list()
        for node in expected_nodes:
            if isinstance(node, str):
                expected_nodes_list.append({"id": node})
            elif isinstance(node, dict):
                expected_nodes_list.append(node)
            else:
                assert False, f"Unexpected value type in the list of expected nodes: '{str(node)}'"

        for node_property in node_test_slots:
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

    if association_test_slots is not None:
        # Convert the 'edges' Iterable content
        # into a list by comprehension
        transformed_edges = [dict(edge) for edge in edges]

        if expected_edges is None:
            # Check for empty 'transformed_edges' expectations
            assert not transformed_edges, \
                "Unexpected non-empty result list of ingest transform edges: "+\
                 f"'{','.join(str(transformed_edges)[0:20])}'..."
        else:
            # Check contents of edge(s) returned.
            # Only 'expected_no_of_edges' are expected to be returned?
            assert len(transformed_edges) == expected_no_of_edges

            expected_edge_list: list[dict] = list()
            if isinstance(expected_edges, list):
                # Blissfully assume that a list of edge slot=value dictionaries was specified
                expected_edge_list.extend(expected_edges)
            else:
                # Blissfully assume just a single edge slot=value dictionary was specified
                expected_edge_list.append(expected_edges)

            for returned_edge in transformed_edges:
                found: bool
                error_messages: Optional[list[str]]
                found, error_messages = _found_edge(returned_edge, expected_edge_list, association_test_slots)
                assert found, '\n'.join(error_messages)
