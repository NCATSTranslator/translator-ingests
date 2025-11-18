"""
Tests for biolink schema loading functionality in validate_biolink_kgx.py
"""

import pytest
import importlib.resources
from linkml_runtime.utils.schemaview import SchemaView

from src.translator_ingest.util.validate_biolink_kgx import get_biolink_schema


@pytest.fixture(autouse=True)
def clear_lru_cache():
    """Clear the LRU cache before each test."""
    get_biolink_schema.cache_clear()
    yield
    get_biolink_schema.cache_clear()


def test_can_import_biolink_model():
    """Test that we can access biolink_model resources with importlib."""
    with importlib.resources.path("biolink_model.schema", "biolink_model.yaml") as schema_path:
        assert schema_path.exists()


def test_can_load_schema_from_local_biolink_model():
    """Test that we can load biolink schema from local biolink_model import."""
    with importlib.resources.path("biolink_model.schema", "biolink_model.yaml") as schema_path:
        schema_view = SchemaView(str(schema_path))
        assert schema_view is not None
        assert hasattr(schema_view, "schema")
        assert schema_view.schema is not None

        # Verify this is actually a biolink schema
        assert schema_view.get_class("named thing") is not None
        assert schema_view.get_slot("related to") is not None


def test_can_load_schema_from_url():
    """Test that we can load biolink schema from the official URL."""
    schema_view = SchemaView("https://w3id.org/biolink/biolink-model.yaml")
    assert schema_view is not None
    assert hasattr(schema_view, "schema")
