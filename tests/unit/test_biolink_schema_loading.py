"""
Tests for biolink schema loading functionality in validate_biolink_kgx.py
"""
import pytest
from importlib.resources import files

from linkml_runtime.utils.schemaview import SchemaView
from bmt import Toolkit

from translator_ingest.util.biolink import (
    get_biolink_schema,
    get_current_biolink_version,
    get_biolink_model_toolkit
)


@pytest.fixture(autouse=True)
def clear_lru_cache():
    """Clear the LRU cache before each test."""
    get_biolink_schema.cache_clear()
    yield
    get_biolink_schema.cache_clear()

def test_can_import_biolink_model():
    """Test that we can access biolink_model resources with importlib."""
    schema_file = files("biolink_model.schema").joinpath("biolink_model.yaml")
    assert schema_file.exists()


def test_can_load_schema_from_local_biolink_model():
    """Test that we can load biolink schema from a local biolink_model import."""
    schema_file = files("biolink_model.schema").joinpath("biolink_model.yaml")
    schema_view = SchemaView(str(schema_file))
    assert schema_view is not None
    assert hasattr(schema_view, 'schema')
    assert schema_view.schema is not None

    # Verify this is actually a biolink schema
    assert schema_view.get_class("named thing") is not None
    assert schema_view.get_slot("related to") is not None

def test_can_load_schema_from_url():
    """Test that we can load biolink schema from the official URL."""
    schema_view = SchemaView("https://w3id.org/biolink/biolink-model.yaml")
    assert schema_view is not None
    assert hasattr(schema_view, 'schema')

def test_get_biolink_model_toolkit():
    """Test that we can get a Biolink Model Toolkit
       configured with the expected project Biolink Model schema."""
    bmt: Toolkit = get_biolink_model_toolkit()
    assert bmt.get_model_version() == get_current_biolink_version()
