# Unit tests for normalizations of nodes and edges, based on Pydantic models
from typing import Optional
import pytest

from biolink_model.datamodel.pydanticmodel_v2 import NamedThing, Association
from src.translator_ingest.util.normalize import (
    convert_to_preferred,
    normalize_edge,
    normalize_node
)


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


def test_normalize_edge():
    pass
