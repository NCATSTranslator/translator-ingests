# The Ingest Pipeline stores intermediate and
# final datasets in a storage repository.
#
# The interfaces in this module encapsulate the use of storage,
# from its implementation (allowing for alternate back end
# design choices: local file, database, cloud, etc.)
#
from typing import Optional
from enum import Enum
from abc import ABC, abstractmethod  # see https://docs.python.org/3/library/abc.html

from .dataset import DataSetId, DataSet

from .file_storage import FileStorage
from .database import Database
from .cloud_storage import CloudStorage

class StorageType(Enum):
    """
    Available categories of data storage,
    currently local FILE, local DATABASE and CLOUD.
    """
    FILE = 1
    DATABASE = 2
    CLOUD = 3


class Storage(ABC):

    def __init__(self):
        pass

    @classmethod
    def get_handle(cls, storage_type: StorageType, **kwargs):
        """
        Generic factory wrapper for ingest pipeline data storage.
        :param storage_type: StorageType modality of storage being used
        :param kwargs: storage configuration parameters
        """

        if storage_type == StorageType.FILE:
            return FileStorage(**kwargs)
        elif storage_type == StorageType.DATABASE:
            return Database(**kwargs)
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


Storage.register(FileStorage)
Storage.register(Database)
Storage.register(CloudStorage)
