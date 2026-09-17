"""
Tests for biolink schema loading functionality in validate_biolink_kgx.py
"""
import os
import pytest
from importlib.metadata import version
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


def test_bmt_default_schema_matches_installed_biolink_model():
    """
    bmt's default schema must agree with the pinned biolink_model package.

    bmt hardcodes a biolink-model version as its default schema URL, independently of the
    biolink-model distribution this project pins. When the two drift, the failure is
    silent and misleading rather than loud: the pydantic classes accept a slot while bmt
    reports that the class does not permit it, so validation and generated mappings are
    computed against a different model than the one being emitted.

    If this fails, align the ``bmt`` and ``biolink-model`` pins in pyproject.toml. Code
    that needs a Toolkit should prefer ``get_biolink_model_toolkit()``, which builds from
    the installed schema and is immune to the default.
    """
    installed = version("biolink-model")
    bmt_default = Toolkit().view.schema.version
    assert bmt_default == installed, (
        f"bmt defaults to the biolink-model {bmt_default} schema but this project pins "
        f"biolink-model {installed}; align the pins in pyproject.toml"
    )


def test_repo_toolkit_uses_the_installed_schema():
    """get_biolink_model_toolkit() must track the installed biolink_model, not bmt's default."""
    assert get_biolink_model_toolkit().view.schema.version == version("biolink-model")


def test_biolink_model_distribution_version_matches_schema_version():
    """The BL_VERSION set in translator_ingest/__init__ comes from the installed biolink-model
       distribution, because reading it is cheap. Ensure that stays equivalent to the schema
       version recorded in ingest metadata, so for example ORION merges on the version we claim to use."""
    assert version("biolink-model") == get_current_biolink_version()


def test_bl_version_env_var_is_set_for_orion():
    """ORION reads BL_VERSION at import time to pick the Biolink Model version."""
    assert os.environ["BL_VERSION"] == get_current_biolink_version()
