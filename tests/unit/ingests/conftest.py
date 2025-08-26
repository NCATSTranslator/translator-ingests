"""
This conftest module contains basic (NOP) MockKoza* fixtures
that can be used across all 'ingest' unit tests.
"""

from typing import Iterator, Iterable

import pytest
import koza
from koza.transform import Record, Mappings
from koza.io.writer.writer import KozaWriter

class MockKozaWriter(KozaWriter):
    """
    Mock "do nothing" implementation of a KozaWriter
    """
    def write(self, entities: Iterable):
        pass

    def finalize(self):
        pass

    def write_edges(self, edges: Iterable):
        pass

    def write_nodes(self, nodes: Iterable):
        pass


class MockKozaTransform(koza.KozaTransform):
    """
    Mock "do nothing" implementation of a KozaTransform
    """
    @property
    def current_reader(self) -> str:
        return ""

    @property
    def data(self) -> Iterator[Record]:
        record: Record = dict()
        yield record


@pytest.fixture(scope="package")
def mock_koza_transform() -> koza.KozaTransform:
    writer: KozaWriter = MockKozaWriter()
    mappings: Mappings = dict()
    return MockKozaTransform(extra_fields=dict(), writer=writer, mappings=mappings)
