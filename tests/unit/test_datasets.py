# Unit tests against the DataSet (translator_ingests.util.dataset) module
import pytest
from uuid import UUID,uuid4
from src.translator_ingest.util.storage import DataSetId, DataSet
from translator_ingest.util.storage.dataset import DataSource


def test_basic_datasource():
    anonymous_datasource = DataSource()
    assert anonymous_datasource.get_infores() is None
    assert anonymous_datasource.get_name() is None
    set_datasource = DataSource(infores="infores:translator")
    assert set_datasource.get_infores() == "infores:translator"
    with pytest.raises(AssertionError):
        DataSource(infores="not-an-infores")


def test_basic_dataset_id():
    unset_dataset = DataSetId()
    assert isinstance(unset_dataset.get_uuid(), UUID)
    test_id = uuid4()
    test_id_urn = test_id.urn
    set_dataset = DataSetId(urn=test_id_urn)
    assert set_dataset.get_uuid().urn, test_id_urn
    with_datasource = DataSetId(urn=test_id_urn)


def test_dataset_creation():
    dataset = DataSet()
    dataset.get_id()
    assert dataset.get_id().get_urn().startswith("urn:uuid:")


def test_dataset_with_dataset_id():
    dataset_id = DataSetId()
    dataset = DataSet(dataset_id=dataset_id)
    assert dataset.get_id() == dataset_id
    assert isinstance(dataset.get_id(), DataSetId)
    assert dataset.get_id().get_urn() == dataset_id.get_urn()
