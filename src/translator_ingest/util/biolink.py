"""Biolink Model support for Translator Ingests"""
from typing import Optional
from functools import lru_cache
from importlib.resources import files
from linkml_runtime.utils.schemaview import SchemaView

from loguru import logger
from biolink_model.datamodel.pydanticmodel_v2 import RetrievalSource
from bmt.toolkit import Toolkit

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
INFORES_INTACT = "infores:intact"

#
# A different version of bmt.pydantic.build_association_knowledge_sources,
# but which takes in a list of dictionaries which are TRAPI-like 'sources' values,
# for conversion into a list of Pydantic RetrieveSources
#
#
# {
#   "sources": [
#     {
#       "resource_id": "infores:columbia-cdw-ehr-data",
#       "resource_role": "supporting_data_source"
#     },
#     {
#       "resource_id": "infores:cohd",
#       "resource_role": "primary_knowledge_source",
#       "upstream_resource_ids": [
#         "infores:columbia-cdw-ehr-data"
#       ]
#     }
#   ]
# }
def knowledge_sources_from_trapi(source_list: Optional[list[dict]] ) -> Optional[list[RetrievalSource]]:
    sources: Optional[list[RetrievalSource]] = None
    if source_list:
        source: dict
        for source in source_list:
            rs = RetrievalSource(
                id=source["resource_id"],
                resource_role=source["resource_role"],
                upstream_resource_ids=source.get("upstream_resource_ids", None)
            )
            sources.append(rs)

    return sources


@lru_cache(maxsize=1)
def get_biolink_schema() -> SchemaView:
    """Get cached Biolink schema, loading it if not already cached."""

    # Try to load from the local Biolink Model package
    # from the locally installed distribution
    try:
        with files("biolink_model.schema").joinpath("biolink_model.yaml") as schema_path:
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

@lru_cache(maxsize=1)
def get_biolink_model_toolkit() -> Toolkit:
    return Toolkit(schema=get_biolink_schema().schema)
