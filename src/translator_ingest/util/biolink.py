""" Biolink Model support for Translator Ingests """
from typing import Optional, List, Dict
from biolink_model.datamodel.model import RetrievalSource

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
BIOLINK_GENE_ASSOCIATED_WITH_CONDITION = "biolink:gene_associated_with_condition"
BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"

def build_association_knowledge_sources(
            primary: str,
            supporting: Optional[List[str]] = None,
            aggregating: Optional[Dict[str, List[str]]] = None
        ) -> List[RetrievalSource]:
    """
    This function attempts to build a List of well-formed Biolink Model RetrievalSource
    of Association 'sources' annotation from the specified knowledge source parameters.
    :param primary: str, infores identifier of the primary knowledge source of an Association
    :param supporting: List[str], infores identifiers of the primary knowledge sources (default: None)
    :param aggregating: Dict[str, List[str]] with infores identifiers of the aggregating knowledge sources as keys,
                        and List[str] of upstream knowledge source infores identifiers (default: None)
    :return:
    """
    #
    # RetrievalSource fields of interest
    #     id: Union[str, RetrievalSourceId] = None
    #     category: Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]] = None
    #     resource_id: Union[str, URIorCURIE] = None
    #     resource_role: Union[str, "ResourceRoleEnum"] = None
    #     upstream_resource_ids: Optional[Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]]] = empty_list()
    #     source_record_urls: Optional[Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]]] = empty_list()
    #     xref: Optional[Union[Union[str, URIorCURIE], list[Union[str, URIorCURIE]]]] = empty_list()
    #
    sources: List[RetrievalSource] = list()
    if primary:
        sources.append(
            RetrievalSource(
                primary
            )
        )
    if supporting:
        for source_id in supporting:
            sources.append(
                RetrievalSource(
                    source_id
                )
            )
    if aggregating:
        # TODO: how should upstream ids etc. be handled?
        for source_id,upstream_ids in aggregating.items():
            sources.append(
                RetrievalSource(
                    source_id
                )
            )
    return sources
