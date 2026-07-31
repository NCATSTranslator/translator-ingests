"""
Generic utility code for use in the ingest unit tests.

The main function of interest is validate_transform_result(), which is used to test
the output of a single @koza.transform_record() decorated method invocation, looking for the
expected content in node and edge slots, with test expectations defined by constraints
'expected_nodes', 'expected_edge', expected_no_of_edges, 'node_test_slots' and 'association_test_slots'
"""

import pytest
from typing import Optional, Iterable, Any, Union, Iterator

import koza
from koza.transform import Mappings
from koza.io.writer.writer import KozaWriter
from koza.model.graphs import KnowledgeGraph
from koza.transform import Record

from biolink_model.datamodel.pydanticmodel_v2 import NamedThing, Association


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


def _compare_slot_values(returned_value, expected_value):
    return returned_value == expected_value or (
        isinstance(returned_value, list)
        and isinstance(expected_value, list)
        and set(returned_value) == set(expected_value)
    )


def _item_matches_expected(item: Any, expected: dict[str, Any]) -> bool:
    """
    Returns True if *item* (a Pydantic model instance) satisfies all field
    constraints in *expected*.

    For each ``(field, value)`` pair:

    * If *value* is a non-empty list of dicts, each sub-dict must be satisfied
      by at least one element in the item's corresponding sub-collection
      (recursive via :func:`_validate_pydantic_collection`).
    * Otherwise a direct equality check is used.

    :param item: a Pydantic model instance to inspect
    :param expected: mapping of field-name → expected-value
    :return: True if all constraints are satisfied
    """
    for field, value in expected.items():
        if not hasattr(item, field):
            return False
        actual = getattr(item, field)
        if isinstance(value
                , list) and value and isinstance(value[0], dict):
            # Nested Pydantic collection: each expected sub-dict must be
            # satisfied by at least one element in the actual sub-collection.
            if not actual:
                return False
            for sub in value:
                if not _validate_pydantic_collection(sub, actual):
                    return False
        else:
            if actual != value:
                return False
    return True


def _validate_pydantic_collection(
    expected: Union[dict[str, Any],list[dict[str, Any]]],
    returned: list | dict,
) -> bool:
    """
    Returns True if at least one Pydantic model instance in *returned* satisfies
    all field constraints in *expected*.

    Field values in *expected* may be nested: if a value is a non-empty list of
    dicts, each sub-dict is matched recursively against the corresponding
    sub-collection on the candidate item, enabling deep validation such as
    ``Association.has_supporting_studies.has_study_results``.

    Works for both list collections (e.g. ``has_affinity: list[AffinityMeasurement]``)
    and dict collections (e.g. ``has_supporting_studies: dict[str, Study]``),
    iterating ``dict.values()`` for the latter.

    :param expected: mapping of field-name → expected-value pairs to match
    :param returned: list of Pydantic model instances, or a dict whose values
                     are Pydantic model instances
    :return: True if at least one instance satisfies all expected field values
    """
    # check if two dictionary collections are being matched...
    if isinstance(expected, dict) and isinstance(returned, dict):
        # better iterate, if more than one expected item
        found: list[bool] = []
        for key, value in expected.items():
            if key not in returned.keys():
                # All expected keys must be somewhere
                # in the returned dictionary, which essentially
                # tests if expected.key == returned.key ...
                return False
            returned_item = returned[key]
            expected_item = expected[key]

            # ...then one-on-one match attempted of the body of the items
            found.append(_item_matches_expected(returned_item, expected_item))

        # since I'm matching all expected against
        # returned, then I need to match all of them?
        return all(found)

    elif isinstance(returned, list):

        items = returned
        if isinstance(expected, list):
            # matching a list of expected instances?
            found: list[bool] = []
            for entry in expected:
                found.append(any(_item_matches_expected(item, entry) for item in items))

            # since I'm matching all expected against
            # returned, then I need to match all of them?
            return all(found)

        else:
            # perhaps just one expected (dictionary) entry
            # to match against a simple list of return values?
            for item in items:
                if _item_matches_expected(item, expected):
                    return True
            return False
    else:
        # just comparing a simple returned item against simple expected value
        return _item_matches_expected(returned, expected)


def _match_edge(
        returned_edge: dict,
        expected_edge: dict,
        target_slots: tuple[str,...]
) -> Optional[str]:
    # We only bother with a comparison if the slot is included in both the
    # 'returned_edge' datum (as defined by the Biolink Pydantic data model)
    # and in the list of slots in the 'expected_edge' test data.
    for association_slot in target_slots:
        if association_slot in returned_edge and association_slot in expected_edge:

            reasv = returned_edge[association_slot]

            # We only pass things through if *both* of the returned and the
            # expected the lists of slot values are or are not empty (namely not XOR).
            expected_slot_value = expected_edge[association_slot]
            if isinstance(expected_slot_value, list):
                if bool(expected_slot_value) ^ bool(reasv):
                    return f"Unexpected return values '{reasv!r}' for slot '{association_slot}' in edge"
                # ...but we only specifically validate non-empty expectations
                if expected_slot_value:
                    for entry in expected_slot_value:
                        if isinstance(entry, str):
                            # Simple Membership value test.
                            if entry not in reasv:
                                return (
                                    f"Value '{entry}' for slot '{association_slot}' "
                                    + f"is missing in returned edge values '{reasv!r}?'"
                                )
                        elif isinstance(entry, dict):
                            # Validate that at least one Pydantic model instance
                            # in the collection matches all expected field values.
                            if not _validate_pydantic_collection(entry, reasv):
                                return (
                                    f"Expected fields {entry!r} not found in any returned "
                                    f"'{association_slot}' entry in '{reasv!r}'"
                                )
                        else:
                            return (
                                "Unexpected value type for "
                                + f"{expected_slot_value!r} for slot '{association_slot}'"
                            )
            elif isinstance(expected_slot_value, dict):
                if not _validate_pydantic_collection(expected_slot_value, reasv):
                    return (
                        f"Expected fields {expected_slot_value!r} not found in any returned "
                        f"'{association_slot}' entry in '{reasv!r}'"
                    )
            else:
                # Scalar value test
                if reasv != expected_slot_value:
                    return (
                        f"Value '{expected_slot_value!r}' "
                        + f"for slot '{association_slot}' not equal to returned edge value '{reasv!r}'?"
                    )

    # If we got to here, then success!
    # No errors were reported?
    return None


def _found_edge(
    returned_edge: dict,
        expected_edge_list: list[dict],
        target_slots: tuple[str,...]
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
        # To guide assessment, the full list of error messages is
        # (only) returned when 'returned_edge' matches no expected edge.
        error_messages.append(error_msg)

    return False, error_messages


def validate_transform_result(
    result: KnowledgeGraph | None,
    expected_nodes: Optional[list],
    expected_edges: Optional[dict] | list[dict],
    expected_no_of_edges: int = 1,
    node_test_slots: Optional[tuple[str,...]] = ("id",),
    edge_test_slots: Optional[tuple[str,...]] = None,
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
    :param node_test_slots: String list of node slots to be tested (default: 'id' - only
                            the node 'id' slot is tested; set to 'None' to skip node slot testing)
    :param edge_test_slots: String list of edge slots to be tested (default: None - no edge slots are tested)
    :return: None
    :raises: AssertionError condition with a candidate list of possible errors, if result expectations were not met
    """
    if result is None:
        if expected_nodes is None and expected_edges is None:
            return  # we're good! No result was expected.
        else:
            assert False, "Unexpected null result from the **`@koza.transform_record`** decorated method call!"
    else:
        # but one or the other of nodes and edges could still be empty, but the test would go on
        nodes: Iterable[NamedThing] = result.nodes if result.nodes is not None else []
        assert (nodes and expected_nodes is not None) or (not nodes and expected_nodes is None), \
            "Unexpected number of nodes returned by record transformation!"
        edges: Iterable[Association] = result.edges if result.edges is not None else []
        assert (edges and expected_edges is not None) or (not edges and expected_edges is None), \
            "Unexpected number of edges returned by record transformation!"

    # if we get this far, we're only interested in testing a non-empty list of nodes
    if nodes and expected_nodes is not None and node_test_slots is not None:

        # Convert the 'nodes' Iterable NamedThing content into
        # a list of Python dictionaries by comprehension
        node: NamedThing
        transformed_nodes: list[dict[str, Any]] = [dict(node) for node in nodes]

        # if nodes are returned, then are they the expected ones?
        # for uniformity in checking details, we convert the
        # expected_nodes to a list of node content dictionaries
        # if 'node' is not a string, it needs to be a dictionary otherwise this fails!
        expected_nodes_list: list[dict[str, Any]] = list()
        for node in expected_nodes:
            if isinstance(node, str):
                # might alone be checking for the node identifiers (simplest check)
                expected_nodes_list.append({"id": node})
            elif isinstance(node, dict):
                # otherwise we're expecting a dictionary
                # of node property=value pairs to match
                expected_nodes_list.append(node)
            else:
                assert False, f"Unexpected value type in the list of expected nodes: '{str(node)}'"

        for node_property in node_test_slots:
            for expected_node in expected_nodes_list:
                if node_property not in expected_node:
                    # We decided not to check this node
                    # property, even if it is returned
                    continue
                expected_node_value = expected_node[node_property]
                # Here we are happy simply to find at least one transformed node matching
                # at least one entry in the expected nodes. This kind of logic allows us
                # to do a lightweight sampling of results, to call the transform successful.
                assert any(
                    [
                        _compare_slot_values(returned_node[node_property], expected_node_value)
                        for returned_node in transformed_nodes
                        if node_property in returned_node
                    ]
                ), (
                    f"Expected node value '{expected_node_value}' for slot '{node_property}'"
                    f" not returned in transformed list of nodes: '{transformed_nodes}' "
                )

    # if we get this far, we're only interested in testing a non-empty list of edges
    if edges and expected_edges is not None and edge_test_slots is not None:

        # Convert the 'edges' Iterable Association content
        # into a list by comprehension
        transformed_edges = [dict(edge) for edge in edges]

        # Check contents of edge(s) returned.
        # Only 'expected_no_of_edges' is expected to be returned?
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
            found, error_messages = _found_edge(returned_edge, expected_edge_list, edge_test_slots)
            assert found, "\n".join(list(error_messages)) if error_messages else "No edges matched expected values?"
