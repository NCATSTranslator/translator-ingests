import pytest

from translator_ingest.util.data.core import BasicDataSource


def test_basic_datasource():
    anonymous_datasource = BasicDataSource()
    assert anonymous_datasource.get_infores() is None
    assert anonymous_datasource.get_name() is None
    set_datasource = BasicDataSource(infores="infores:translator")
    assert set_datasource.get_infores() == "infores:translator"
    with pytest.raises(AssertionError):
        BasicDataSource(infores="not-an-infores")
