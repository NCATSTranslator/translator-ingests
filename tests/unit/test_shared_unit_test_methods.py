"""
These unit tests vet a few test fringe cases of the tests/unit/ingests/__init__.py methods
"""
from typing import Optional
import pytest

from koza.model.graphs import KnowledgeGraph
from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum
)

from tests.unit.ingests import validate_transform_result

TEST_ENTITY_ID_1= "foo:bar"
TEST_NODE_1 = NamedThing(id=TEST_ENTITY_ID_1)
TEST_EDGE_1 = Association(
    id=TEST_ENTITY_ID_1,
    subject=TEST_ENTITY_ID_1,
    predicate="biolink:related_to",
    object=TEST_ENTITY_ID_1,
    knowledge_level=KnowledgeLevelEnum.not_provided,
    agent_type=AgentTypeEnum.not_provided
)

TEST_ENTITY_ID_2= "tweedle:dumb"
TEST_NODE_2 = NamedThing(id=TEST_ENTITY_ID_2)
TEST_EDGE_2 = Association(
    id=TEST_ENTITY_ID_2,
    subject=TEST_ENTITY_ID_2,
    predicate="biolink:related_to",
    object=TEST_ENTITY_ID_2,
    knowledge_level=KnowledgeLevelEnum.not_provided,
    agent_type=AgentTypeEnum.not_provided
)

TEST_EDGE_3 = Association(
    id=TEST_ENTITY_ID_1,
    subject=TEST_ENTITY_ID_1,
    predicate="biolink:related_to",
    object=TEST_ENTITY_ID_2,
    knowledge_level=KnowledgeLevelEnum.not_provided,
    agent_type=AgentTypeEnum.not_provided
)

TEST_SLOTS = ("id",)


def test_validate_transform_results():
    # This regular result should pass,
    # given all other arguments set with default values
    validate_transform_result(
        result=KnowledgeGraph(
            nodes=[TEST_NODE_1, TEST_NODE_2],
            edges=[TEST_EDGE_3],
        ),
        expected_nodes=[TEST_NODE_1.model_dump(),TEST_NODE_2.model_dump()],
        expected_edges=TEST_EDGE_3.model_dump(exclude_none=True),
        edge_test_slots=TEST_SLOTS
    )


def test_incorrect_number_of_validate_transform_results():
    # The following will fail simply because the number of edges
    # returned is not equal to the expected number of edges
    with pytest.raises(AssertionError):
        validate_transform_result(
            result=KnowledgeGraph(
                nodes=[TEST_NODE_1, TEST_NODE_2],
                edges=[TEST_EDGE_3],
            ),
            expected_nodes=[TEST_NODE_1.model_dump(), TEST_NODE_2.model_dump()],
            expected_no_of_edges=2,
            expected_edges=TEST_EDGE_3.model_dump(exclude_none=True),
            edge_test_slots=TEST_SLOTS
        )


@pytest.mark.parametrize(
    "query_result,expected_nodes,expected_edges,node_test_slots,edge_test_slots",
    [
        (   # Query 0 - Returns a null result,
            #           but we were expecting a node,
            #           thus raising an exception
            None,
            [TEST_NODE_1.model_dump()], None,
            None, None
        ),
        (   # Query 1 - Returns a null result,
            #           but we were expecting an edge,
            #           thus raising an exception
            None,
            None, TEST_EDGE_1.model_dump(exclude_none=True),
            None, None
        ),
        (   # Query 2 - Returns empty nodes in KnowledgeGraph result
            #           but expected a node, thus raising an exception
            KnowledgeGraph(),
            [TEST_NODE_1.model_dump()], None,
            None, None
        ),
        (   # Query 3 - Returns empty edges in KnowledgeGraph result
            #           but expected an edge, thus raising an exception
            KnowledgeGraph(),
            None, TEST_EDGE_1.model_dump(exclude_none=True),
            None, None
        ),
        (   # Query 4 - With node_test_slots provided,
            #           given a knowledge graph result with a node,
            #           but we don't expect a node, thus raising an exception
            KnowledgeGraph(
                nodes=[TEST_NODE_1],
                edges=None
            ),
            None, None,
            TEST_SLOTS, None
        ),
        (   # Query 5 - With edge_test_slots provided,
            #           given a knowledge graph result with an edge,
            #           but we don't expect an edge, thus raising an exception
            KnowledgeGraph(
                nodes=None,
                edges=[TEST_EDGE_1],
            ),
            None, None,
            None, TEST_SLOTS
        ),
        (   # Query 6 - With node_test_slots provided,
            #           given a knowledge graph result with a node,
            #           the node returned doesn't match the expected node, thus raising an exception
            KnowledgeGraph(
                nodes=[TEST_NODE_2],
                edges=None
            ),
            [TEST_NODE_1.model_dump()], None,
            TEST_SLOTS, None
        ),
        (   # Query 7 - With edge_test_slots provided,
            #           given a knowledge graph result with an edge,
            #           the edge returned doesn't match the expected edge, thus raising an exception
            KnowledgeGraph(
                nodes=None,
                edges=[TEST_EDGE_2],
            ),
            None,[TEST_NODE_1.model_dump()],
            None, TEST_SLOTS
        )
    ],
)
def test_validate_transform_results_exceptions(
    query_result: KnowledgeGraph | None,
    expected_nodes: Optional[list],
    expected_edges: Optional[dict] | list[dict],
    node_test_slots: Optional[tuple[str,...]],
    edge_test_slots: Optional[tuple[str,...]],
):
    with pytest.raises(AssertionError):
        validate_transform_result(
            result=query_result,
            expected_nodes=expected_nodes,
            expected_edges=expected_edges,
            node_test_slots=node_test_slots,
            edge_test_slots=edge_test_slots,
        )
