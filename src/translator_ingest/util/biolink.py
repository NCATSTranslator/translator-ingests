""" Biolink Model support for Translator Ingests """
from typing import Optional, List, Dict
from uuid import uuid4

from biolink_model.datamodel.pydanticmodel_v2 import RetrievalSource, ResourceRoleEnum

# knowledge source InfoRes curies
INFORES_MONARCHINITIATIVE = "infores:monarchinitiative"
INFORES_OMIM = "infores:omim"
INFORES_ORPHANET = "infores:orphanet"
INFORES_MEDGEN = "infores:medgen"
INFORES_DECIFER = "infores:decifer"
INFORES_HPOA = "infores:hpo-annotations"

# hard-coded predicates
BIOLINK_AFFECTS = "biolink:affects"
BIOLINK_CAUSES = "biolink:causes"
BIOLINK_CONTRIBUTES_TO = "biolink:contributes_to"
BIOLINK_ASSOCIATED_WITH = "biolink:associated_with"
BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"


def entity_id() -> str:
    return uuid4().urn

def build_association_knowledge_sources(
            primary: str,
            supporting: Optional[List[str]] = None,
            aggregating: Optional[Dict[str, List[str]]] = None
        ) -> List[RetrievalSource]:
    """
    This function attempts to build a List of well-formed Biolink Model RetrievalSource
    of Association 'sources' annotation from the specified knowledge source parameters.
    :param primary: str, infores identifier of the primary knowledge source of an Association
    :param supporting: Optional[List[str]], infores identifiers of the supporting data sources (default: None)
    :param aggregating: Optional[Dict[str, List[str]]] with infores identifiers of the aggregating knowledge sources
                        as keys, and List[str] of upstream knowledge source infores identifiers (default: None)
    :return: List[RetrievalSource] not guaranteed in any given order, except that
                                   the first entry may be the primary knowledge source
    """
    #
    # RetrievalSource fields of interest
    #     resource_id: Union[str, URIorCURIE] = None
    #     resource_role: Union[str, "ResourceRoleEnum"] = None
    #     upstream_resource_ids: Optional[Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]]] = empty_list()
    #     TODO: current use case doesn't use source_record_urls, but...
    #     source_record_urls: Optional[Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]]] = empty_list()
    #
    sources: List[RetrievalSource] = list()
    primary_knowledge_source: Optional[RetrievalSource] = None
    if primary:
        primary_knowledge_source = RetrievalSource(
            id=entity_id(),
            resource_id=primary,
            resource_role=ResourceRoleEnum.primary_knowledge_source,
            **{}
        )
        sources.append(primary_knowledge_source)

    if supporting:
        for source_id in supporting:
            supporting_knowledge_source = RetrievalSource(
                id=entity_id(),
                resource_id=source_id,
                resource_role=ResourceRoleEnum.supporting_data_source,
                **{}
            )
            sources.append(supporting_knowledge_source)
            if primary_knowledge_source:
                if primary_knowledge_source.upstream_resource_ids is None:
                    primary_knowledge_source.upstream_resource_ids = list()
                primary_knowledge_source.upstream_resource_ids.append(source_id)
    if aggregating:
        for source_id,upstream_ids in aggregating.items():
            aggregating_knowledge_source = RetrievalSource(
                id=entity_id(),
                resource_id=source_id,
                resource_role=ResourceRoleEnum.aggregator_knowledge_source,
                **{}
            )
            aggregating_knowledge_source.upstream_resource_ids = upstream_ids
            sources.append(aggregating_knowledge_source)

    return sources
