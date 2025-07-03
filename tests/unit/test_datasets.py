# Unit tests against the DataSet (translator_ingests.util.dataset) module

from typing import Optional, Dict

import pytest

from src.translator_ingest.util.storage import DataSetId, NodeId, EdgeId, DataSet


def test_dataset_creation():
    dataset = DataSet()
    dataset.get_id()
    assert dataset.get_curie().startswith("urn:uuid:")


def test_dataset_with_dataset_id():
    dataset_id = DataSetId()
    dataset = DataSet(dataset_id=dataset_id)
    assert dataset.get_id() == dataset_id
    assert dataset.get_curie() == dataset_id.urn


def test_node_id():
    node_id = NodeId("test-id")
    assert node_id == "test-id"


def test_edge_id():
    dataset_id = EdgeId()
    assert dataset_id.urn.startswith("urn:uuid:")

@pytest.mark.parametrize(
    "data,outcome",
    [
        ({}, False),
        ({}, True)
    ]
)
def test_validate_node(data: Dict, outcome: bool):
    dataset = DataSet()
    assert dataset.validate_node(data=data) == outcome


@pytest.mark.parametrize(
    "data,outcome",
    [
        ({}, False),
        ({}, True)
    ]
)
def test_validate_edge(data: Dict, outcome: bool):
    dataset = DataSet()
    assert dataset.validate_edge(data=data) == outcome


@pytest.mark.parametrize(
    "data,outcome",
    [
        ({}, None),
        ({}, "node-id")
    ]
)
def test_publish_node(data: Dict, outcome: Optional[NodeId]):
    dataset = DataSet()
    node_id: Optional[NodeId] = dataset.publish_node(data={})
    assert node_id == outcome


@pytest.mark.parametrize(
    "data,outcome",
    [
        ({}, None),
        ({}, "edge-id")
    ]
)
def test_publish_edge(data: Dict, outcome: Optional[EdgeId]):
    dataset = DataSet()
    edge_id: Optional[EdgeId] = dataset.publish_edge(data={})
    assert edge_id == outcome
