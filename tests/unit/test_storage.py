import pytest

from src.translator_ingest.util.storage import (
    StorageType,
    Storage,
    DataSetId,
    DataSet,
    FileStorage,
    Database,
    CloudStorage
)

@pytest.mark.parametrize(
    "storage_type,storage_class",
    [
        (StorageType.FILE, FileStorage),
        (StorageType.DATABASE, Database),
        (StorageType.CLOUD, CloudStorage)
    ]
)
def test_storage_get_handle(storage_type: StorageType, storage_class):
    storage_handle = Storage.get_handle(storage_type)
    assert isinstance(storage_handle, storage_class)


def test_file_storage_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.FILE, **config)
    dataset_id: DataSetId = "fake_id"
    dataset = DataSet(dataset_id)
    storage_handle.store(dataset)
    data = storage_handle.store(dataset_id)
    assert data is not None, "FILE Stored dataset was not returned"


def test_database_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.DATABASE, **config)
    dataset_id: DataSetId = "fake_id"
    dataset = DataSet(dataset_id)
    storage_handle.store(dataset)
    data = storage_handle.store(dataset_id)
    assert data is not None, "DATABASE Stored dataset was not returned"


def test_cloud_storage_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.CLOUD, **config)
    dataset_id: DataSetId = "fake_id"
    dataset = DataSet(dataset_id)
    storage_handle.store(dataset)
    data = storage_handle.store(dataset_id)
    assert data is not None, "CLOUD Stored dataset was not returned"
