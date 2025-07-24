"""
Abstract encapsulation of Translator Ingest process DataSource
"""
from typing import Optional
from uuid import UUID, uuid4

from translator_ingest.util.data import DataSource, DataSet


class BasicDataSource(DataSource):
    """
    Basic DataSource Implementation
    """

    def get_dataset(
            self,
            urn: Optional[str] = None,
            version: Optional[str] = None,
            create: bool = False
    ) -> UUID:
        """
        Access or possibly create ("register") a DataSet hosted by a generic BasicDataSource

        :param urn: Optional[str] uniform resource name of the DataSet to be accessed.
        :param version: Optional[str] of a DataSet version (of specific DataSource-specified format)
        :param create: bool, flag triggering creation of a new DataSet (default: False)
        :return: UUID identifier of the requested DataSet.
        :raises DataSourceException if the DataSet cannot be identified.
        """
        identifier: UUID = uuid4()  # stub implementation
        if urn is not None:
            # 1. Identify or create a DataSet by 'urn' which is not None.
            if not create:
                # 1.1 If 'create' is False, then raise a DataSourceException
                #     if a DataSet with the given urn does not exist.
                pass

            else:
                # 1.2 If 'create' is True, then create a new DataSet identified by specified urn.
                #     If the version is not None, set DataSet 'version' accordingly;
                #     If the version is None, set the 'version' in accordance with the DataSource versioning policy.
                pass
        else:
            # 2. Identify DataSet by 'version' only (the 'urn' is None).
            if not create:
                # 2.1 If 'create' is False, and a non-empty 'version' is given, attempt to access that 'version'.
                #     If the 'version' is set to None, attempt to return a suitable default current release of the data.
                #     Raise a DataSourceException in either case if a DataSet with indicative 'version' does not exist.
                pass
            else:
                # 2.2 If 'create' is True, create new DataSet, assigning it a new ad hoc UUID identifier.
                #     If the specified 'version' value is not None, set the 'version' of the new DataSet to that value.
                #     Otherwise, generate a 'version' label following the 'version' format policy of the given DataSource.
                pass

        return identifier

    def get_version(self, dataset_id: Optional[UUID]) -> Optional[str]:
        # Stub implementation: don't know here how to discern the version of a Basic DataSource
        return None

DataSet.register(BasicDataSource)


class TextDataSet(DataSet):
    def add_file(self, file, label: Optional[str] = None):
        pass

    def add_metadata(self, tag: str, value: str):
        pass

DataSet.register(TextDataSet)