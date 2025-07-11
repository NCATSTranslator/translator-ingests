# Abstract encapsulation of Translator Ingest process datasets
# Probably will be a collection of KGX jsonlines + metadata files
# but for now, we specify the concept in an agnostic fashion here
from typing import Optional
from os import urandom
from uuid import UUID, uuid4
from ..kgx import KGX

class DataSetId(UUID):
    def __init__(self):
        # emulates uuid4() via the UUID constructor
        # TODO: can I make a variant with UUID input argument?
        UUID.__init__(self, bytes=urandom(16), version=4)


# TODO: this class is just a stub proxy for some real data, so very incomplete!
class DataSet:

    def __init__(self, dataset_id: Optional[DataSetId] = None):
        """
        Constructor for initialization of an empty dataset. Issues a new DataSetId for the DataSet
        if an existing DataSetId is not provided in as an argument to the construction.
        """
        # A dataset_id may be provided if the dataset already exists somewhere...
        self._dataset_id = dataset_id if dataset_id is not None else DataSetId()

        # ... however, we defer initialization of the KGX dataset to a later time
        self._kgx: Optional[KGX] = None

    def set_kgx(self, kgx:KGX):
        self._kgx = kgx

    def get_kgx(self):
        return self._kgx

    def get_id(self) -> DataSetId:
        """
        :return: Returns the direct internal Python representation of the dataset identifier
        """
        return self._dataset_id

    def get_curie(self) -> str:
        """
        Gets the CURIE string format of the DataSet identifier for data documentation purposes.
        however, most of the software within the project  (e.g. storage access methods) will generally want to use
        the get_id() method to use the direct internal Python representation of the dataset identifier.
        :return: CURIE string representation of the DataSet identifier
        """
        return self._dataset_id.urn
