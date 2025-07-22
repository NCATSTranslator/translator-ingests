# Abstract encapsulation of Translator Ingest process datasets
# The wrapper is agnostic about the specific content of the dataset
# which rather can (or should) be specified in a child class override.
# For example, a 'KgxDataSet 'would precisely define the specific
# file components and formats of the KGX data.
from typing import Optional
from abc import ABC, abstractmethod  # see https://docs.python.org/3/library/abc.html
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
        """
        :return: infores of the DataSource (if set, may be None if anonymous)
        """
        return self._infores

    def get_name(self) -> Optional[str]:
        """
        :return: name of the DataSource (if set, may be None if anonymous)
        """
        return self._name

    def get_dataset(
            self,
            urn: Optional[str] = None,
            version: Optional[str] = None,
            create: bool = False
    ) -> Optional[UUID]:
        pass

    def get_version(self, dataset_id: Optional[UUID]) -> Optional[str]:
        """
        In some instances, the DataSource may already know how to retrieve
        the version specification of a given DataSet identified by its identifier.
        The trivial case of a None input identifier, returns None.
        The case where the DataSource cannot identify a version by its identifier, also returns None.
        Otherwise, a suitable version string is returned in a DataSource idiosyncratic format (e.g., date, SemVer, etc.)
        
        :param dataset_id: Optional[UUID], identifier of DataSet whose version is being retrieved.
        :return: Optional[str], DataSource resolved version of an identified DataSet
        """
        pass


class DataSetId:
    """
    Abstraction wrapper for specific dataset identification.
    """
    def __init__(
            self,
            data_source: Optional[DataSource] = None,
            urn: Optional[str] = None,
            version: Optional[str] = None,
            create: bool = False
    ):
        """
        Constructor of a DataSet Identifier.

        May also trigger the registration of a new DataSet if the DataSource is not None
        and a DataSet urn and/or version is specified, with the 'create' flag set to True.
        Creation and loading of the DataSet itself is deferred until it is accessed.

        :param data_source: (Optional) DataSource instance describing the concrete external provider of the DataSet.
        :param urn: (Optional) UUID urn string (of an existing dataset)
        :param version: (Optional) versioning of a dataset, specific to a given DataSource
        :param create: bool, creates a DataSet if it does not exist (ignored if DataSet already exists; default: False)
        """
        self._data_source = data_source
        self._version: Optional[str] = version
        if data_source is not None:
            if urn is not None:
                # Get an existing dataset by dataset UUID urn
                self._identifier = data_source.get_dataset(urn=urn, version=version, create=create)
            else:
                # Otherwise, try to identify or create a dataset by version or,
                # if the version is None, identify a default dataset of a DataSource
                self._identifier: Optional[UUID] = data_source.get_dataset(version=version, create=create)

            # The version may be reset here if it is known,
            # i.e., as determined by the DataSource by the DataSet identifier
            # (May still be None)
            self._version = data_source.get_version(self._identifier)
        else:  # anonymous DataSource
            if urn is not None:
                # The user specified UUID urn is directly used as the DataSet identifier
                self._identifier = UUID(urn)
            else:
                # Otherwise, just create a valid new identifier (but without registering the DataSet anywhere)
                # (The version is already set above to whatever one the user gave above (possibly None)
                self._identifier = uuid4()

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
class DataSet(ABC):

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
