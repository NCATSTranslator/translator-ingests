from abc import ABC, abstractmethod
from typing import Optional
from uuid import UUID, uuid4

class DataSourceException(Exception):
    pass

class DataSource(ABC):
    def __init__(
            self, infores: Optional[str] = None,
            name: Optional[str] = None
    ):
        """
        Constructor of a reference object for a DataSource.
        :param infores: Optional[str] infores for an external provider of
                        the DataSet. If provided, it must be well-formatted.
        :param name: Optional[str] name of the DataSet.
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

    @abstractmethod
    def get_dataset(
            self,
            urn: Optional[str] = None,
            version: Optional[str] = None,
            create: bool = False
    ) -> UUID:
        """
        Access or possibly create ("register") a DataSource-hosted DataSet.

        :param urn: Optional[str] uniform resource name of the DataSet to be accessed.
        :param version: Optional[str] of a DataSet version (of specific DataSource-specified format)
        :param create: bool, flag triggering creation of a new DataSet (default: False)
        :return: UUID identifier of the requested DataSet.
        :raises DataSourceException if the DataSet cannot be identified.

        The procedure of DataSet access (or creation) is guided by user-specified arguments as follows:

        1. Identify or create a DataSet by 'urn' which is not None.

        1.1 If 'create' is False, then raise a DataSourceException
            if a DataSet with the given urn does not exist.

        1.2 If 'create' is True, then create a new DataSet identified by specified urn.
            If the version is not None, set DataSet 'version' accordingly;
            If the version is None, set the 'version' in accordance with the DataSource versioning policy.

        2. Identify DataSet by 'version' only (the 'urn' is None).

        2.1 If 'create' is False, and a non-empty 'version' is given, attempt to access that 'version'.
            If the 'version' is set to None, attempt to return a suitable default current release of the data.
            Raise a DataSourceException in either case if a DataSet with indicative 'version' does not exist.

        2.2 If 'create' is True, create new DataSet, assigning it a new ad hoc UUID identifier.
            If the specified 'version' value is not None, set the 'version' of the new DataSet to that value.
            Otherwise, generate a 'version' label following the 'version' format policy of the given DataSource.
        """
        pass

    @abstractmethod
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

        May also trigger the registration of a new DataSet in the associated DataSource
        if the latter is provided and a DataSet urn and/or version is specified
        when the 'create' flag is set to True;
        Otherwise, a DataSourceException may be raised, if the DataSet doesn't already exist
        Creation and loading of the DataSet itself is deferred until it is accessed.

        :param data_source: (Optional) DataSource instance describing the concrete external provider of the DataSet.
        :param urn: (Optional) UUID4-compliant urn string (of an existing dataset)
        :param version: (Optional) versioning of a dataset, specific to a given DataSource
        :param create: bool, creates a DataSet if it does not exist (ignored if DataSet already exists; default: False)
        """
        self._data_source = data_source
        self._identifier: UUID
        self._version: Optional[str] = version
        if data_source is not None:
            self._identifier = data_source.get_dataset(urn=urn, version=version, create=create)

            # The version may be reset here if it is known,
            # i.e., as determined by the DataSource by the DataSet identifier
            # (May still be None)
            self._version = data_source.get_version(self._identifier)
        else:  # anonymous DataSource
            if urn is not None:
                # The user specified UUID urn is directly used as the DataSet identifier
                self._identifier = UUID(urn)
            else:
                # Otherwise, create an 'ad hoc' valid new identifier (but without DataSource registration)
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

    @abstractmethod
    def add_file(self, file, label: Optional[str] = None):
        pass

    @abstractmethod
    def add_metadata(self, tag: str, value: str):
        pass
