"""
Abstract encapsulation of Translator Ingest process DataSource
"""
from typing import Optional
from uuid import UUID

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
    ) -> Optional[UUID]:
        raise NotImplementedError()

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