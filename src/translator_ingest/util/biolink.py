"""Biolink Model support for Translator Ingests"""
from typing import Optional, Union
from functools import lru_cache
from importlib.resources import files

from linkml_runtime.utils.schemaview import SchemaView

from biolink_model.datamodel.pydanticmodel_v2 import RetrievalSource, ResourceRoleEnum

from bmt import Toolkit

from translator_ingest.util.logging_utils import get_logger
logger = get_logger(__name__)

# knowledge source InfoRes curies
INFORES_MONARCHINITIATIVE = "infores:monarchinitiative"
INFORES_OMIM = "infores:omim"
INFORES_ORPHANET = "infores:orphanet"
INFORES_MEDGEN = "infores:medgen"
INFORES_DECIFER = "infores:decifer"
INFORES_HPOA = "infores:hpo-annotations"
INFORES_CTD = "infores:ctd"
INFORES_GOA = "infores:goa"
INFORES_PATHBANK = "infores:pathbank"
INFORES_SEMMEDDB = "infores:semmeddb"
INFORES_BIOLINK = "infores:biolink"
INFORES_SIGNOR = "infores:signor"
INFORES_TTD = "infores:ttd"
INFORES_BGEE = "infores:bgee"
INFORES_TEXT_MINING_KP = "infores:text-mining-provider-targeted"
INFORES_INTACT = "infores:intact"
INFORES_DGIDB = "infores:dgidb"
INFORES_DISEASES = "infores:diseases"
INFORES_MEDLINEPLUS = "infores:medlineplus"
INFORES_AMYCO = "infores:amyco"
INFORES_EBI_G2P = "infores:gene2phenotype"
INFORES_DRUGCENTRAL = "infores:drugcentral"
INFORES_DRUGMATRIX = "infores:drugmatrix"
INFORES_PDSP_KI = "infores:pdsp-ki"
INFORES_WOMBAT_PK = "infores:wombat-pk"
## from dgidb ingest, can move above if others use it
INFORES_CGI = "infores:cgi"
INFORES_CIVIC = "infores:civic"
INFORES_CKB_CORE = "infores:ckb-core"
INFORES_COSMIC = "infores:cosmic"
INFORES_CANCERCOMMONS = "infores:cancercommons"
INFORES_CHEMBL = "infores:chembl"
INFORES_CLEARITY_BIOMARKERS = "infores:clearity-biomarkers"
INFORES_CLEARITY_CLINICAL = "infores:clearity-clinical-trial"
INFORES_DTC = "infores:dtc"
INFORES_DOCM = "infores:docm"
INFORES_FDA_PGX = "infores:fda-pgx"
INFORES_GTOPDB = "infores:gtopdb"
INFORES_MYCANCERGENOME = "infores:mycancergenome"
INFORES_MYCANCERGENOME_TRIALS = "infores:mycancergenome-trials"
INFORES_NCIT = "infores:ncit"
INFORES_ONCOKB = "infores:oncokb"
INFORES_PHARMGKB = "infores:pharmgkb"

@lru_cache(maxsize=1)
def get_biolink_schema() -> SchemaView:
    """Get cached Biolink schema, loading it if not already cached."""

    # Try to load from the local Biolink Model package
    # from the locally installed distribution
    try:
        schema_path = files("biolink_model.schema").joinpath("biolink_model.yaml")
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
    """Get a Biolink Model Toolkit configured with the expected project Biolink Model schema."""
    return Toolkit(schema=get_biolink_schema().schema)


def parse_attributes(attributes: Optional[dict]) -> Optional[dict]:
    return (
        attributes
        if attributes is not None and len(attributes) > 0
        else None
    )

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
    """
    Mapping TRAPI-style sources onto the Pydantic data model
    is relatively straightforward since the TRAPI model itself
    was mapped onto the Biolink Model RetrievalSources class.
    """
    if not source_list:
        return None
    else:
        sources: list[RetrievalSource] = []
        source: dict
        for source in source_list:
            rs = RetrievalSource(
                id=source["resource_id"],
                resource_id=source["resource_id"],
                resource_role=source["resource_role"],
                upstream_resource_ids=source.get("upstream_resource_ids", None)
            )
            sources.append(rs)
        return sources


def _build_retrieval_source(
        source_spec: Union[str,tuple[str, list[str]]],
        resource_role: Optional[ResourceRoleEnum]
) -> RetrievalSource:
    if isinstance(source_spec, tuple):
        assert len(source_spec) == 2, f"Invalid supporting data source tuple: {source_spec}"
        resource_id = str(source_spec[0])
        source_record_urls = source_spec[1] if len(source_spec[1]) > 0 else None
    else:
        resource_id = source_spec
        source_record_urls = None
    return RetrievalSource(
        id=resource_id,
        resource_id=resource_id,
        resource_role=resource_role,
        source_record_urls=source_record_urls,
        **{},
    )

def build_association_knowledge_sources(
        primary: Union[str,tuple[str, list[str]]],
        supporting: Optional[list[Union[str,tuple[str, list[str]]]]] = None,
        aggregating: Optional[Union[str,tuple[str, list[str]]]] = None
) -> list[RetrievalSource]:
    """
    This function attempts to build a list of a well-formed RetrievalSource list
    for an Association **sources** slot, using given knowledge source parameters
    for primary, supporting and aggregating knowledge sources.

    The use case for 'aggregating knowledge source' represents the limited use case where
    only one primary knowledge source is specified, as a single upstream knowledge source.
    This is, of course, not the general case for all aggregating knowledge sources;
    however, the use case of aggregating knowledge sources with multiple upstream ('primary')
    knowledge sources is not yet supported.

    This method is lenient in that it allows for strings that are not explicitly encoded
    as infores identifiers, converting these to infores identifiers by prefixing with
    the 'infores:' namespace (but the method doesn't validate these coerced infores identifiers
    against the public infores inventory at https://github.com/biolink/information-resource-registry).

    There are optional 'extended form' provisions for the addition of associated **source_record_urls**
    to the instances of **resource_id** provided for primary, aggregating and supporting knowledge sources.

    Parameters
    ----------
    primary:
        **Simple form:** Infores 'resource_id' for the primary knowledge source of an Association.
        **Extended form:** 2-tuple of (resource_id, list[source_record_urls])
        for the primary knowledge source of an Association.

    supporting:
        **Simple form:** List of supporting datasource infores 'resource_id' instances. Supporting
        data sources are automatically assumed to be upstream of the primary knowledge source and
        mapped accordingly.
        **Extended form:** List of 2-tuples with form (resource_id, list[source_record_urls]).

    aggregating:
        **Simple form:** With the infores 'resource_id' of the aggregating knowledge source.
        The primary knowledge source given to the method is automatically assumed to be upstream
        of the aggregating knowledge source and mapped accordingly.
        **Extended form:** 2-tuple of (resource_id, list[source_record_urls])
        for the aggregating knowledge source of an Association.

    Returns
    -------
    list[RetrievalSource]:
        List of RetrievalSource entries that are not guaranteed in any given order,
        except that the first entry will usually be the primary knowledge source.

    """
    primary_knowledge_source: Optional[RetrievalSource] = \
        _build_retrieval_source(
            primary,
            ResourceRoleEnum.primary_knowledge_source
        )
    sources: list[RetrievalSource] =[primary_knowledge_source]

    if supporting:
        for supporting_source_id in supporting:
            supporting_knowledge_source = \
                _build_retrieval_source(
                    supporting_source_id,
                    ResourceRoleEnum.supporting_data_source
            )
            sources.append(supporting_knowledge_source)
            if primary_knowledge_source.upstream_resource_ids is None:
                primary_knowledge_source.upstream_resource_ids = []
            primary_knowledge_source.upstream_resource_ids.append(
                supporting_knowledge_source.resource_id
            )

    if aggregating:
        aggregating_knowledge_source = \
            _build_retrieval_source(
                aggregating,
                ResourceRoleEnum.aggregator_knowledge_source
            )

        # The use case for 'aggregating knowledge source' represents the
        # limited use case where only one primary knowledge source is specified
        aggregating_knowledge_source.upstream_resource_ids = [
            primary_knowledge_source.resource_id
        ]

        sources.append(aggregating_knowledge_source)

    return sources
