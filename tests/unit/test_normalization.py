# Unit tests for normalizations of nodes and edges, based on Pydantic models
from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import (
    KnowledgeLevelEnum,
    AgentTypeEnum,
    NamedThing,
    Association,
    Gene
)

from src.translator_ingest.util.normalize import (
    convert_to_preferred,
    normalize_edge,
    normalize_node
)

############################################
# TODO: Caveat that these unit tests are
#       currently using CURIE resolution
#       based on Node normalization data
#       extant as of July 1, 2025, for the
#       test CURIE for WRN (a human disease
############################################

# def convert_to_preferred(curie, allowed_list):
def test_convert_to_preferred():
    result: str = convert_to_preferred("HGNC:12791", "NCBIGene")
    assert result == "NCBIGene:7486"


# normalize_node(node: NamedThing) -> Optional[NamedThing]
def test_normalize_missing_node():
    node = NamedThing(id="foo:bar", category=["biolink:NamedThing"],**{})
    result: Optional[NamedThing] = normalize_node(node)
    assert result is None


def test_normalize_node():
    node = NamedThing(id="HGNC:12791", category=["biolink:NamedThing"],**{})
    result: Optional[NamedThing] = normalize_node(node)
    assert result.id == "NCBIGene:7486"
    # TODO: a more thorough testing of normalization method side effects is required


def test_normalize_node_already_canonical():
    node = NamedThing(id="NCBIGene:7486", category=["biolink:NamedThing"],**{})
    result: Optional[NamedThing] = normalize_node(node)
    assert result.id == "NCBIGene:7486"


def test_normalize_non_named_thing_node():
    node = Gene(id="HGNC:12791", category=["biolink:Gene"],**{})
    result: Optional[NamedThing] = normalize_node(node)
    assert result.id == "NCBIGene:7486"


def test_normalize_edge():
    edge = Association(
        id="foo:bar",
        subject="HGNC:12791",
        predicate="causes",
        object="DOID:5688",
        knowledge_level=KnowledgeLevelEnum.not_provided,
        agent_type=AgentTypeEnum.not_provided,
        **{})
    result: Optional[Association] = normalize_edge(edge)
    assert result.subject == "NCBIGene:7486"
    assert result.object == "MONDO:0010196"


