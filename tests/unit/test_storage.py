import pytest

from src.translator_ingest.util.data.core import TextDataSet
from src.translator_ingest.util.storage import (
    StorageType,
    Storage,
    MemoryStorage,
    FileStorage,
    CloudStorage
)


@pytest.mark.parametrize(
    "storage_type,storage_class",
    [
        (StorageType.IN_MEMORY, MemoryStorage),
        (StorageType.FILE, FileStorage),
        (StorageType.CLOUD, CloudStorage)
    ]
)
def test_storage_get_handle(storage_type: StorageType, storage_class):
    storage_handle = Storage.get_handle(storage_type)
    assert isinstance(storage_handle, storage_class)


@pytest.mark.skip("Underlying code not yet implemented")
def test_database_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.IN_MEMORY, **config)
    # new empty ab initio dataset created
    dataset = TextDataSet()
    storage_handle.store(dataset)
    data = storage_handle.retrieve(dataset.get_id())
    assert data is not None, "IN_MEMORY Stored dataset was not returned"

@pytest.mark.skip("Underlying code not yet implemented")
def test_file_storage_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.FILE, **config)
    # new empty ab initio dataset created
    dataset = TextDataSet()
    storage_handle.store(dataset)
    data = storage_handle.retrieve(dataset.get_id())
    assert data is not None, "FILE Stored dataset was not returned"


@pytest.mark.skip("Underlying code not yet implemented")
def test_cloud_storage_access():
    config = dict()
    storage_handle = Storage.get_handle(StorageType.CLOUD, **config)
    # new empty ab initio dataset created
    dataset = TextDataSet()
    storage_handle.store(dataset)
    data = storage_handle.retrieve(dataset.get_id())
    assert data is not None, "CLOUD Stored dataset was not returned"
