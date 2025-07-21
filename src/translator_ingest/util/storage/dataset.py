# Abstract encapsulation of Translator Ingest process datasets
# The wrapper is agnostic about the specific content of the dataset
# which rather can (or should) be specified in a child class override.
# For example, a 'KgxDataSet 'would precisely define the specific
# file components and formats of the KGX data.
from typing import Optional

from uuid import UUID, uuid4

class DataSource:
    def __init__(
            self, infores: Optional[str] = None,
            name: Optional[str] = None
    ):
        """
        Constructor of a reference object for a DataSource.
        :param infores: (optional) infores of external provider of the DataSet. If provided, it must be well-formatted.
        :param name:  (optional) name of the DataSet.
        """
        # basic QA sanity check (doesn't prove infores existence, though...)
        assert infores is None or infores.startswith("infores:")
        self._infores = infores
        self._name = name

    def get_infores(self) -> Optional[str]:
        return self._infores

    def get_name(self) -> Optional[str]:
        return self._name


class DataSetId:
    """
    Abstraction wrapper for specific dataset identification.
    """
    def __init__(
            self,
            data_source: Optional[DataSource] = None,
            urn: Optional[str] = None,
            version: Optional[str] = None
    ):
        """

        :param data_source: (Optional) DataSource instance describing the concrete external provider of the DataSet.
        :param urn: (Optional) UUID urn string (of an existing dataset)
        :param version: (Optional) versioning of a dataset, specific to a given DataSource
        """
        self._identifier = UUID(urn) if urn is not None else uuid4()
        self._data_source = data_source
        self._version = version


    def get_data_source(self) -> Optional[DataSource]:
        """
        :return: DataSource instance describing the concrete external provider of the DataSet.
        """
        return self._data_source

    def get_uuid(self) -> UUID:
        """
        :return: UUID-formatted object of the Dataset identifier
        """
        return self._identifier

    def get_urn(self) -> str:
        """
        :return: UUID URN string representation of the DataSet identifier
        """
        return self._identifier.urn

    def get_version(self) -> Optional[str]:
        """
        The version string return is idiosyncratic to the DataSource,
        i.e., it could be a timestamp, date string or a Semantic Version, Git version/branch/tag, etc.
        :return: (Optional) string specifying the version of a dataset (identifier)
        """
        return self._version


# TODO: this class is just a stub proxy for some real data, so very incomplete,
#       and may be subclassed for specific major file formats (e.g. KGX file set)
class DataSet:

    def __init__(self, dataset_id: Optional[DataSetId] = None):
        """
        Constructor for initialization of an empty dataset.
        Issues a new anonymous DataSetId for the DataSet if an existing
        DataSetId is not provided in as an argument to the construction.
        For most formal existing datasets, it is preferable to construct and input
        a valid DataSetId for the existing dataset, before binding it to the DataSet.
        """
        # A dataset_id may be provided if the dataset already exists somewhere...
        self._dataset_id = dataset_id if dataset_id is not None else DataSetId()

    def get_id(self) -> DataSetId:
        """
        :return: Returns the direct internal Python representation of the dataset identifier
        """
        return self._dataset_id
