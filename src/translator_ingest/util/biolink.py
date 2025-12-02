"""Biolink Model support for Translator Ingests"""
from functools import lru_cache
from importlib.resources import files
import logging
from linkml_runtime.utils.schemaview import SchemaView

from typing import Optional
from uuid import uuid4
from loguru import logger

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    DiseaseOrPhenotypicFeature,
    Disease,
    SmallMolecule,
    Drug,
    MolecularMixture,
    ChemicalEntity,
    RetrievalSource,
    ResourceRoleEnum
)
logger = logging.getLogger(__name__)

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


def entity_id() -> str:
    """
    Generate a unique identifier for a Biolink Model entity.
    :return: str, unique identifier
    """
    return uuid4().urn


def _infores(identifier: str) -> str:
    """
    Coerce the specified identifier to the Biolink Model infores namespace.
    :param identifier:
    :return: identifier properly coerced to infores namespace
    """
    # Limitation: no attempt is made to validate
    # them against the public infores inventory at
    # https://github.com/biolink/information-resource-registry)
    return identifier if identifier.startswith("infores:") else f"infores:{identifier}"


_BIOLINK_CLASS_MAPPING: dict[str, type[NamedThing]] = {
    "biolink:NamedThing": NamedThing,
    "biolink:DiseaseOrPhenotypicFeature": DiseaseOrPhenotypicFeature,
    "biolink:Disease": Disease,
    "biolink:SmallMolecule": SmallMolecule,
    "biolink:Drug": Drug,
    "biolink:MolecularMixture": MolecularMixture,
    "biolink:ChemicalEntity": ChemicalEntity,
}

def get_node_class(node_id: str, categories: list[str]) -> type[NamedThing]:
    if len(categories) != 1:
        logger.warning(f"ICEES record with id {node_id} has empty or multiple categories: '{str(categories)}'")
        # TODO: Need to figure out how to return the most specific class for this... Check BMT?
        category = "biolink:NamedThing"
    else:
        category = categories[0]
    return _BIOLINK_CLASS_MAPPING.get(category, NamedThing)


def build_association_knowledge_sources(
        primary: str,
        supporting: Optional[list[str]] = None,
        aggregating: Optional[dict[str, list[str]]] = None
) -> list[RetrievalSource]:
    """
    This function attempts to build a list of well-formed Biolink Model RetrievalSource
    of Association 'sources' annotation from the specified knowledge source parameters.
    This method is lenient in that it allows for strings that are not explicitly infores identifiers:
    it converts these to infores identifiers by prefixing with the 'infores:' namespace (but doesn't validate
    them against the public infores inventory at https://github.com/biolink/information-resource-registry).

    :param primary: String infores identifier for the primary knowledge source of an Association
    :param supporting: Optional[list[str]], Infores identifiers of the supporting data sources (default: None)
    :param aggregating: Optional[dict[str, list[str]]] With infores identifiers of the aggregating knowledge sources
                        as keys, and list[str] of upstream knowledge source infores identifiers (default: None)
    :return: list[RetrievalSource] not guaranteed in any given order, except that
                                   the first entry may be the primary knowledge source
    """
    #
    # RetrievalSource fields of interest
    #     resource_id: Union[str, URIorCURIE] = None
    #     resource_role: Union[str, "ResourceRoleEnum"] = None
    #     upstream_resource_ids: Optional[Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]]] = empty_list()
    #     Limitation: the current use case doesn't use source_record_urls, but...
    #     source_record_urls: Optional[Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]]] = empty_list()
    #
    sources: list[RetrievalSource] = list()
    primary_knowledge_source: Optional[RetrievalSource] = None
    if primary:
        primary_knowledge_source = RetrievalSource(
            id=entity_id(), resource_id=_infores(primary), resource_role=ResourceRoleEnum.primary_knowledge_source, **{}
        )
        sources.append(primary_knowledge_source)

    if supporting:
        for source_id in supporting:
            supporting_knowledge_source = RetrievalSource(
                id=entity_id(),
                resource_id=_infores(source_id),
                resource_role=ResourceRoleEnum.supporting_data_source,
                **{},
            )
            sources.append(supporting_knowledge_source)
            if primary_knowledge_source:
                if primary_knowledge_source.upstream_resource_ids is None:
                    primary_knowledge_source.upstream_resource_ids = list()
                primary_knowledge_source.upstream_resource_ids.append(_infores(source_id))
    if aggregating:
        for source_id, upstream_ids in aggregating.items():
            aggregating_knowledge_source = RetrievalSource(
                id=entity_id(),
                resource_id=_infores(source_id),
                resource_role=ResourceRoleEnum.aggregator_knowledge_source,
                **{},
            )
            aggregating_knowledge_source.upstream_resource_ids = [_infores(upstream) for upstream in upstream_ids]
            sources.append(aggregating_knowledge_source)

    return sources


#
# A different version of the above function, but which takes in
# a list of dictionaries which are TRAPI-like 'sources' values,
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

INFORES_TTD = "infores:ttd"
INFORES_INTACT = "infores:intact"

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

