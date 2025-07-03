# Abstract encapsulation of Translator Ingest process datasets
# For now, we specify the concept in an agnostic fashion here
from enum import Enum
from typing import Optional, Dict
from os import urandom
from uuid import UUID

class DataSetType(Enum):
    PKS = "Primary Knowledge Source Knowledge Graph capture format"
    KGX = "Knowledge Graph eXchange ('KGX') format"

class DataSetId(UUID):
    def __init__(self):
        # emulates uuid4() via the UUID constructor
        # TODO: can I make a variant with UUID input argument?
        UUID.__init__(self, bytes=urandom(16), version=4)

class NodeId(str):
    pass

class EdgeId(UUID):
    def __init__(self):
        # emulates uuid4() via the UUID constructor
        # TODO: can I make a variant with UUID input argument?
        UUID.__init__(self, bytes=urandom(16), version=4)

# TODO: this class is currently just a stub proxy
#  for some real data, thus very incomplete!
class DataSet:

    def __init__(
            self,
            dataset_id: Optional[DataSetId] = None,
            dataset_type: DataSetType = DataSetType.PKS
    ):
        """
        Constructor for initialization of an empty dataset. Issues a new DataSetId for the DataSet
        if an existing DataSetId is not provided in as an argument to the construction.
        """
        # A dataset_id may be provided if the dataset already exists somewhere...
        self._dataset_id = dataset_id if dataset_id is not None else DataSetId()
        self.dataset_type = dataset_type

        # TODO: design the internal KG data representation:
        #       directly or by subclass inheritance?

    def get_id(self) -> DataSetId:
        """
        :return: Returns the direct internal Python representation of the dataset identifier
        """
        return self._dataset_id

    def get_curie(self) -> str:
        """
        Gets the CURIE string format of the DataSet identifier for data documentation purposes.
        However, most of the software within the project (e.g., storage access methods) will generally want to use
        the get_id() method to use the direct internal Python representation of the dataset identifier.
        :return: CURIE string representation of the DataSet identifier
        """
        return self._dataset_id.urn

    def publish_node(self, data: Dict) -> Optional[NodeId]:
        """
        The contents of a node are defined internally as a
        multi-level Python dictionary representing a JSON object
        with properties defined by the type of dataset.
        :param data:
        :return: NodeId of the newly created node in the DataSet
        """
        if self.validate_node(data):
            # TODO: process node here
            return None  # NodeId()
        else:
            # The input data was not validated,
            # thus the node could not be published
            # TODO: how do we communicate the source
            #       of the node validation failure:
            #       by error log or by exception?
            return None

    def publish_edge(self, data: Dict) -> Optional[EdgeId]:
        """
        The contents of an edge are defined internally as a
        multi-level Python dictionary representing a JSON object
        with properties defined by the type of dataset.
        :param data:
        :return: EdgeId of the newly created edge in the DataSet
        """
        if self.validate_edge(data):
            # TODO: process edge here
            return None  # EdgeId()
        else:
            # The input was not validated,
            # thus the node could not be published
            # TODO: how do we communicate the source
            #       of the edge validation failure:
            #       by error log or by exception?
            return None

    @staticmethod
    def validate_node(data: Dict) -> bool:
        # TODO: DatasetType-specific validation of a node dictionary data structure
        return False

    @staticmethod
    def validate_edge(data: Dict) -> bool:
        # TODO: DatasetType-specific validation of an edge dictionary data structure
        return False