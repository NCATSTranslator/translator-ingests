"""Biolink Model support for Translator Ingests"""
import importlib.resources
from functools import lru_cache
from linkml_runtime.utils.schemaview import SchemaView
from loguru import logger

# knowledge source InfoRes curies
INFORES_MONARCHINITIATIVE = "infores:monarchinitiative"
INFORES_OMIM = "infores:omim"
INFORES_ORPHANET = "infores:orphanet"
INFORES_MEDGEN = "infores:medgen"
INFORES_DECIFER = "infores:decifer"
INFORES_HPOA = "infores:hpo-annotations"
INFORES_CTD = "infores:ctd"
INFORES_GOA = "infores:goa"
INFORES_SEMMEDDB = "infores:semmeddb"
INFORES_BIOLINK = "infores:biolink"
INFORES_TTD = "infores:ttd"

# TODO: could this be moved into bmt.pydantic?
@lru_cache(maxsize=1)
def get_biolink_schema() -> SchemaView:
    """Get cached Biolink schema, loading it if not already cached."""

    # Try to load from local biolink model first (same version as ingests)
    try:
        with importlib.resources.path("biolink_model.schema", "biolink_model.yaml") as schema_path:
            schema_view = SchemaView(str(schema_path))
            logger.debug("Successfully loaded Biolink schema from local file")
            return schema_view
    except Exception as e:
        logger.warning(f"Failed to load local Biolink schema: {e}")
        # Fallback to loading from official URL
        schema_view = SchemaView("https://w3id.org/biolink/biolink-model.yaml")
        logger.debug("Successfully loaded Biolink schema from URL")
        return schema_view


def get_current_biolink_version() -> str:
    return get_biolink_schema().schema.version
