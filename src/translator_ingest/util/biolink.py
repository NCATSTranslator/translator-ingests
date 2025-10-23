"""Biolink Model support for Translator Ingests"""

from typing import Optional
from uuid import uuid4
from functools import lru_cache
from loguru import logger
import biolink_model.datamodel.pydanticmodel_v2 as pyd

from bmt import Toolkit
# default toolkit, unless specified otherwise
toolkit = Toolkit()

# knowledge source InfoRes curies
INFORES_MONARCHINITIATIVE = "infores:monarchinitiative"
INFORES_OMIM = "infores:omim"
INFORES_ORPHANET = "infores:orphanet"
INFORES_MEDGEN = "infores:medgen"
INFORES_DECIFER = "infores:decifer"
INFORES_HPOA = "infores:hpo-annotations"
INFORES_CTD = "infores:ctd"
INFORES_GOA = "infores:goa"
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


def _depth_function(bmt: Toolkit = toolkit):
    # Get depth of each category in the hierarchy
    @lru_cache()
    def get_category_depth(category):
        depth = 0
        while True:
            parent = bmt.get_parent(category)
            if not parent:
                break
            category = parent
            depth += 1
        return depth
    return get_category_depth


def get_most_specific_category(category_list, bmt: Toolkit = toolkit):
    # Rank categories by depth
    # This works because depth in the hierarchy correlates with
    # specificity—leaf nodes or deeply nested classes are more specific
    ranked = sorted(category_list, key=_depth_function(bmt), reverse=True)
    most_specific = ranked[0]
    print(f"Most specific category: {most_specific}")
    return most_specific


def get_node_class(node_id: str, categories: list[str], bmt: Toolkit = toolkit) -> type[pyd.NamedThing] | None:
    if not categories:
        logger.warning(f"Node with id {node_id} has empty categories")
        return None
    category = get_most_specific_category(categories, bmt=bmt)
    try:
        category = category.replace("biolink:", "")
        return getattr(pyd, category)
    except AttributeError:
        logger.warning(f"No Biolink Model class found for category '{category}', for node with id {node_id}")
        return None


def build_association_knowledge_sources(
    primary: str, supporting: Optional[list[str]] = None, aggregating: Optional[dict[str, list[str]]] = None
) -> list[pyd.RetrievalSource]:
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
    sources: list[pyd.RetrievalSource] = list()
    primary_knowledge_source: Optional[pyd.RetrievalSource] = None
    if primary:
        primary_knowledge_source = pyd.RetrievalSource(
            id=entity_id(), resource_id=_infores(primary), resource_role=pyd.ResourceRoleEnum.primary_knowledge_source, **{}
        )
        sources.append(primary_knowledge_source)

    if supporting:
        for source_id in supporting:
            supporting_knowledge_source = pyd.RetrievalSource(
                id=entity_id(),
                resource_id=_infores(source_id),
                resource_role=pyd.ResourceRoleEnum.supporting_data_source,
                **{},
            )
            sources.append(supporting_knowledge_source)
            if primary_knowledge_source:
                if primary_knowledge_source.upstream_resource_ids is None:
                    primary_knowledge_source.upstream_resource_ids = list()
                primary_knowledge_source.upstream_resource_ids.append(_infores(source_id))
    if aggregating:
        for source_id, upstream_ids in aggregating.items():
            aggregating_knowledge_source = pyd.RetrievalSource(
                id=entity_id(),
                resource_id=_infores(source_id),
                resource_role=pyd.ResourceRoleEnum.aggregator_knowledge_source,
                **{},
            )
            aggregating_knowledge_source.upstream_resource_ids = [_infores(upstream) for upstream in upstream_ids]
            sources.append(aggregating_knowledge_source)

    return sources
