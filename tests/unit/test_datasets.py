# Unit tests against the DataSet (translator_ingests.util.dataset) module

import pytest

from src.translator_ingest.util.storage import DataSetId, DataSet


def test_dataset_creation():
    dataset = DataSet()
    dataset.get_id()
    assert dataset.get_curie().startswith("urn:uuid:")


def test_dataset_with_dataset_id():
    dataset_id = DataSetId()
    dataset = DataSet(dataset_id=dataset_id)
    assert dataset.get_id() == dataset_id
    assert dataset.get_curie() == dataset_id.urn
