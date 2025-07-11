import pytest

from src.translator_ingest.util.storage import (
    StorageType,
    Storage,
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


@pytest.mark.skip("Underlying code not yet implemented")
def test_file_storage_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.FILE, **config)
    # new empty ab initio dataset created
    dataset = DataSet()
    storage_handle.store(dataset)
    data = storage_handle.retrieve(dataset.get_id())
    assert data is not None, "FILE Stored dataset was not returned"

@pytest.mark.skip("Underlying code not yet implemented")
def test_database_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.DATABASE, **config)
    # new empty ab initio dataset created
    dataset = DataSet()
    storage_handle.store(dataset)
    data = storage_handle.retrieve(dataset.get_id())
    assert data is not None, "DATABASE Stored dataset was not returned"


@pytest.mark.skip("Underlying code not yet implemented")
def test_cloud_storage_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.CLOUD, **config)
    # new empty ab initio dataset created
    dataset = DataSet()
    storage_handle.store(dataset)
    data = storage_handle.retrieve(dataset.get_id())
    assert data is not None, "CLOUD Stored dataset was not returned"
