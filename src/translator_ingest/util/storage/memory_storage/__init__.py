from typing import Optional
from .. import DataSetId, DataSet

class MemoryStorage:
    def __init__(self, **kwargs):
        """
        Configure an in-memory storage access instance.
        :param kwargs: storage configuration parameters
        """
        pass

    def store(self, data: DataSet):
        pass

    def retrieve(self, dataset_id: DataSetId) -> Optional[DataSet]:
        pass
