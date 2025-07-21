# The Ingest Pipeline stores diverse intermediate
# and final datasets in a storage repository.
#
# This module abstracts out the use of pipeline data storage,
# from its implementation (allowing for alternate back end
# choices: in memory, local file, remote cloud, etc.)
#
from typing import Optional
from enum import Enum
from abc import ABC, abstractmethod  # see https://docs.python.org/3/library/abc.html

from .dataset import DataSetId, DataSet

from .memory_storage import MemoryStorage
from .file_storage import FileStorage
from .cloud_storage import CloudStorage

class StorageType(Enum):
    """
    Available categories of data storage,
    currently in memory, local file and remote cloud storage.
    """
    IN_MEMORY = 1,
    FILE = 2,
    CLOUD = 3


class Storage(ABC):

    def __init__(self):
        pass

    @classmethod
    def get_handle(cls, storage_type: StorageType, **kwargs):
        """
        Generic factory wrapper for the ingest pipeline data storage.
        :param storage_type: StorageType modality of storage being used
        :param kwargs: storage configuration parameters
        """

        if storage_type == StorageType.IN_MEMORY:
            return MemoryStorage(**kwargs)
        elif storage_type == StorageType.FILE:
            return FileStorage(**kwargs)
        elif storage_type == StorageType.CLOUD:
            return CloudStorage(**kwargs)
        else:
            raise f"Unknown storage type {storage_type}"

    @abstractmethod
    def store(self, data: DataSet):
        pass

    @abstractmethod
    def retrieve(self, dataset_id: DataSetId)-> Optional[DataSet]:
        pass


Storage.register(MemoryStorage)
Storage.register(FileStorage)
Storage.register(CloudStorage)
