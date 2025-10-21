"""
Some simple utilities for COHD ingest
"""
from loguru import logger
from typing import Optional

from biolink_model.datamodel.pydanticmodel_v2 import (
    NamedThing,
    DiseaseOrPhenotypicFeature,
    Disease,
    SmallMolecule,
    Drug,
    MolecularMixture,
    ChemicalEntity,
    RetrievalSource
)

_COHD_CLASS_MAPPING: dict[str, type[NamedThing]] = {
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
        logger.warning(f"COHD record with id {node_id} has empty or multiple categories: '{str(categories)}'")
        # TODO: Need to figure out how to return the most specific class for this... Check BMT?
        category = "biolink:NamedThing"
    else:
        category = categories[0]
    return _COHD_CLASS_MAPPING.get(category, NamedThing)

#
# COHD 'sources' values are TRAPI-like JSON
# which need to be wrapped as Pydantic RetrieveSources
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
def build_sources(source_list: Optional[list[dict]] ) -> Optional[list[RetrievalSource]]:
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

