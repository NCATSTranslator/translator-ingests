# Unit tests against the DataSet (translator_ingests.util.dataset) module
from uuid import UUID,uuid4
from translator_ingest.util.data import DataSetId
from translator_ingest.util.data.core import BasicDataSource
from translator_ingest.util.data.core import TextDataSet


def test_basic_dataset_id():
    # Without DataSource

    ## Ab initial identifier
    unset_dataset = DataSetId()
    assert isinstance(unset_dataset.get_uuid(), UUID)

    ## Preset identifier
    test_id = uuid4()
    test_id_urn = test_id.urn
    set_dataset = DataSetId(urn=test_id_urn)
    assert set_dataset.get_urn(), test_id_urn

    # With DataSource
    datasource = BasicDataSource()
    with_datasource_no_create = DataSetId(data_source=datasource, urn=test_id_urn)


def test_dataset_creation():
    dataset = TextDataSet()
    dataset.get_id()
    assert dataset.get_id().get_urn().startswith("urn:uuid:")


def test_dataset_with_dataset_id():
    dataset_id = DataSetId()
    dataset = TextDataSet(dataset_id=dataset_id)
    assert dataset.get_id() == dataset_id
    assert isinstance(dataset.get_id(), DataSetId)
    assert dataset.get_id().get_urn() == dataset_id.get_urn()
