import uuid
import logging
from pathlib import Path
import koza
from typing import Iterable, Any

import koza
import requests

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    Protein,
    GeneToGoTermAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    # OntologyClass,
    # OntologyClass, while OntologyClass would be semantically more appropriate, Koza's KGX converter only supports NamedThing and Association entities for proper serialization.
    NamedThing,
    Association,
    BiologicalProcess,
    MolecularActivity,
    CellularComponent,
    MacromolecularComplex,
    RNAProduct
)
from translator_ingest.util.biolink import (
    INFORES_GOA,
    INFORES_BIOLINK,
    entity_id,
    build_association_knowledge_sources
)

# Constants
INFORES_GOA = "infores:goa"
INFORES_BIOLINK = "infores:biolink"
GOA_RELEASE_METADATA_URL = "https://current.geneontology.org/metadata/release-date.json"
GOA_DATA_DIR = Path("data/goa")


logger = logging.getLogger(__name__)


def _parse_header_date(gaf_path: Path) -> str | None:
    try:
        with gaf_path.open("r") as handle:
            for line in handle:
                if not line.startswith("!"):
                    break
                if line.lower().startswith("!date-generated"):
                    return line.split(":", 1)[1].strip()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RuntimeError(f"Failed to read GOA GAF header from {gaf_path}") from exc
    return None


def _fallback_version_from_gaf() -> str:
    candidate_files = sorted(
        GOA_DATA_DIR.glob("*.gaf"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidate_files:
        raise RuntimeError("No GOA GAF files found for fallback version lookup")

    for gaf_path in candidate_files:
        version = _parse_header_date(gaf_path)
        if version:
            return version

    raise RuntimeError(
        "Unable to determine GOA version: metadata endpoint failed and no '!date-generated' header found"
    )


def get_latest_version() -> str:
    """Fetch the current GOA release version from the metadata endpoint with a local fallback."""
    try:
        response = requests.get(GOA_RELEASE_METADATA_URL, timeout=10)
        response.raise_for_status()
        metadata = response.json()
    except requests.RequestException as exc:
        logger.warning("GOA metadata request failed (%s). Using local fallback header.", exc)
        return _fallback_version_from_gaf()

    version = metadata.get("date")
    if not version:
        logger.warning("GOA metadata response missing 'date'. Using local fallback header.")
        return _fallback_version_from_gaf()

    return version

# Dynamic category assignments from Biolink pydantic models
# This ensures the categories are always in sync with the Biolink model
GENE_CATEGORY = Gene.model_fields['category'].default
PROTEIN_CATEGORY = Protein.model_fields['category'].default
NAMED_THING_CATEGORY = NamedThing.model_fields['category'].default
GENE_TO_GO_TERM_ASSOCIATION_CATEGORY = GeneToGoTermAssociation.model_fields['category'].default
MACROMOLECULAR_COMPLEX_CATEGORY = MacromolecularComplex.model_fields['category'].default
RNA_PRODUCT_CATEGORY = RNAProduct.model_fields['category'].default

# GO aspect to biolink class mapping for proper categorization
GO_ASPECT_TO_BIOLINK_CLASS = {
    "P": BiologicalProcess,  # Biological Process
    "F": MolecularActivity,  # Molecular Function  
    "C": CellularComponent,  # Cellular Component
}

# Database to biolink class mapping
# This allows dynamic selection of Gene vs Protein based on the data source
# UniProtKB contains proteins, while other sources like MGI contain genes
DB_TO_BIOLINK_CLASS = {
    "UniProtKB": Protein,           # UniProtKB contains protein sequences
    "MGI": Gene,                    # MGI contains gene information
    "SGD": Gene,                    # SGD contains gene information
    "RGD": Gene,                    # RGD contains gene information
    "ZFIN": Gene,                   # ZFIN contains gene information
    "FB": Gene,                     # FlyBase contains gene information
    "WB": Gene,                     # WormBase contains gene information
    "TAIR": Gene,                   # TAIR contains gene information
    "ComplexPortal": MacromolecularComplex,  # ComplexPortal contains protein complexes
    "RNAcentral": RNAProduct,       # RNAcentral contains RNA products
}

# Mappings for GO qualifiers to Biolink predicates
# The qualifier field in GAF contains the actual relationship type
# This mapping converts GO qualifiers to appropriate biolink predicates
QUALIFIER_TO_PREDICATE = {
    # Standard qualifiers
    "enables": "biolink:enables",
    "located_in": "biolink:located_in", 
    "part_of": "biolink:part_of",
    "involved_in": "biolink:participates_in",
    "contributes_to": "biolink:contributes_to",
    "colocalizes_with": "biolink:colocalizes_with",
    "is_active_in": "biolink:is_active_in",
    
    # Upstream qualifiers
    "acts_upstream_of": "biolink:acts_upstream_of",
    "acts_upstream_of_or_within": "biolink:acts_upstream_of_or_within",
    "acts_upstream_of_positive_effect": "biolink:acts_upstream_of_positive_effect",
    "acts_upstream_of_negative_effect": "biolink:acts_upstream_of_negative_effect",
    "acts_upstream_of_or_within_positive_effect": "biolink:acts_upstream_of_or_within_positive_effect",
    "acts_upstream_of_or_within_negative_effect": "biolink:acts_upstream_of_or_within_negative_effect",
}

# Fallback mapping for aspect-based predicates (used when the qualifier is not recognized)
ASPECT_TO_PREDICATE = {
    "P": "biolink:participates_in",  # Biological Process
    "F": "biolink:enables",          # Molecular Function  
    "C": "biolink:located_in",       # Cellular Component
}

# GO evidence codes mapped to biolink knowledge levels and agent types
# Note: Using hardcoded mapping instead of JSON config for simplicity and performance
# The biolink model provides KnowledgeLevelEnum and AgentTypeEnum for validation
# This mapping follows GO evidence code standards and maps them to appropriate Biolink knowledge levels
EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE = {
    # Experimental evidence codes (high confidence)
    "EXP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IDA": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IPI": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IMP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IGI": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IEP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HTP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HDA": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HMP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HGI": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "HEP": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    
    # Phylogenetic evidence codes (prediction level)
    "IBA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IBD": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IKR": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "IRD": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    
    # Computational analysis evidence codes (prediction level)
    "ISS": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ISO": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ISA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "ISM": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IGC": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "RCA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    
    # Author statement evidence codes
    "TAS": (KnowledgeLevelEnum.knowledge_assertion, AgentTypeEnum.manual_agent),
    "NAS": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    "IC": (KnowledgeLevelEnum.prediction, AgentTypeEnum.manual_agent),
    
    # Special cases
    "ND": (KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided),  # No biological data available
    "IEA": (KnowledgeLevelEnum.prediction, AgentTypeEnum.automated_agent),  # Inferred from Electronic Annotation
}

@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> Iterable[Any]:
    """
    Transform a single GAF record into Biolink nodes and edges.
    
    Uses @koza.transform_record() decorator for record-by-record processing,
    following the pattern established in the ingest template and CTD implementation.
    This approach is chosen over @koza.transform() for better memory efficiency
    and clearer error handling per record.
    
    This function leverages the biolink pydantic model for validation and structure,
    keeping the code simple and biolink-centered while maintaining readability.
    """
    # Parse GAF record fields
    # GAF format: tab-delimited with 17 columns as defined in the YAML configuration
    db_object_id = record["DB_Object_ID"]  # UniProtKB identifier
    go_id = record["GO_ID"]               # GO term identifier
    aspect = record["Aspect"]             # P (Process), F (Function), C (Component)
    db_object_symbol = record["DB_Object_Symbol"]  # Gene symbol
    qualifier = record["Qualifier"]       # Can contain "NOT" for negative associations
    # Pipe-separated publication references
    # Only retain valid PMIDs per RIG guidance (exclude GO_REF, ISBN, Reactome, etc.)
    db_references_raw = record.get("DB_Reference", "")
    publications = db_references_raw.split("|") if db_references_raw else []
    evidence_code = record["Evidence_Code"]  # GO evidence code (EXP, IEA, etc.)
    taxon = record["Taxon"]              # NCBI taxonomy identifier
    db_object_name = record["DB_Object_Name"]  # Full gene name/description

    # Determine Biolink class based on the database source
    # The DB field contains the database name (e.g., "UniProtKB"), not the DB_Object_ID
    db_source = record["DB"]
    biolink_class = DB_TO_BIOLINK_CLASS.get(db_source)

    if not biolink_class:
        koza.log(f"Database source '{db_source}' not recognized for record: {record}", level="WARNING")
        return [], []

    # Create entity node using appropriate Biolink class
    # Biolink pydantic model centric: Uses appropriate class from biolink model for automatic validation of required fields,
    # proper type checking, and biolink-compliant structure
    
    # Handle entity ID creation - some databases already include the prefix in DB_Object_ID
    if db_object_id.startswith(f"{db_source}:"):
        # DB_Object_ID already includes the database prefix (e.g., "MGI:101757")
        node_id = db_object_id
    else:
        # DB_Object_ID doesn't include prefix, so we add it (e.g., "A0A024RBG1" -> "UniProtKB:A0A024RBG1")
        node_id = f"{db_source}:{db_object_id}"
    
    entity = biolink_class(
        id=node_id,
        name=db_object_symbol,
        category=biolink_class.model_fields['category'].default,  # Dynamic category from Biolink model
        in_taxon=[taxon.replace("taxon:", "NCBITaxon:")],  # Convert GO taxon format to Biolink NCBI format
        description=db_object_name if db_object_name else None,  # Include full entity name as description
    )

    # Create GO term node using the appropriate biolink class based on aspect
    # GO aspects map to specific biolink classes for proper semantic categorization:
    # P (Process) -> BiologicalProcess, F (Function) -> MolecularActivity, C (Component) -> CellularComponent
    go_biolink_class = GO_ASPECT_TO_BIOLINK_CLASS.get(aspect, NamedThing)
    go_term = go_biolink_class(
        id=go_id,
        category=go_biolink_class.model_fields['category'].default  # Dynamic category from Biolink model
    )

    # Get predicate from qualifier mapping (primary) or aspect mapping (fallback)
    # The qualifier field contains the actual relationship type, which is more specific than the aspect
    # Handle NOT qualifiers by extracting the base qualifier
    base_qualifier = qualifier.replace("NOT|", "") if qualifier.startswith("NOT|") else qualifier
    
    # Try to get predicate from qualifier first
    predicate = QUALIFIER_TO_PREDICATE.get(base_qualifier)
    
    # Fallback to aspect-based predicate if qualifier not recognized
    if not predicate:
        predicate = ASPECT_TO_PREDICATE.get(aspect)
        if not predicate:
            koza.log(msg=f"Unknown qualifier '{qualifier}' and aspect '{aspect}' for record: {record}", level="WARNING")
            return [], []
        else:
            koza.log(
                msg="Using fallback predicate for " +
                    f"qualifier '{qualifier}' -> aspect '{aspect}' -> '{predicate}'", level="INFO"
            )

    # Get knowledge level and agent type from evidence code mapping
    # Biolink-centric: Uses biolink KnowledgeLevelEnum and AgentTypeEnum for type safety
    # and automatic validation of biolink-compliant knowledge metadata
    # This mapping ensures that the confidence level of GO annotations is properly
    # represented in the knowledge graph according to Biolink standards
    knowledge_level, agent_type = EVIDENCE_CODE_TO_KNOWLEDGE_LEVEL_AND_AGENT_TYPE.get(
        evidence_code, 
        (KnowledgeLevelEnum.not_provided, AgentTypeEnum.not_provided)
    )

    # Format publications as CURIEs using biolink conventions
    # Biolink pydantic model centric: Formats publication IDs as proper CURIEs following biolink model conventions
    # for consistent identifier representation across the knowledge graph
    publications_list = []
    for ref in publications:
        if not ref:
            continue
        token = ref.strip()
        if not token:
            continue
        # Accept only PMIDs of the form PMID:12345678 or bare numeric which we normalize to PMID:12345678
        if token.startswith("PMID:"):
            pmid_part = token[len("PMID:") :].strip()
            if pmid_part.isdigit():
                publications_list.append(f"PMID:{pmid_part}")
        elif token.isdigit():
            publications_list.append(f"PMID:{token}")

    # Create association dynamically based on the biolink class
    # Biolink pydantic model centric: Uses appropriate association class based on the entity type
    # For Gene entities, use GeneToGoTermAssociation; for other entities, use generic Association
    # since there are no specific associations for Protein, MacromolecularComplex, or RNAProduct in the biolink model
    if biolink_class == Gene:
        # Use GeneToGoTermAssociation for gene entities
        association = GeneToGoTermAssociation(
            id=entity_id(),
            subject=entity.id,
            predicate=predicate,
            object=go_term.id,
            negated="NOT" in qualifier,  # Handle negative associations from the GAF qualifier field
            has_evidence=[f"ECO:{evidence_code}"],  # Biolink pydantic model centric: Formats evidence as ECO CURIE
            publications=publications_list,
            sources=build_association_knowledge_sources(
                primary=INFORES_GOA, # GOA as the primary source
                aggregating={INFORES_BIOLINK:[INFORES_GOA]} # This repository as the aggregator
            ),
            knowledge_level=knowledge_level,
            agent_type=agent_type,
        )
    else:
        # Use generic Association for protein, complex, and RNA entities since there are no specific associations
        association = Association(
            id=entity_id(),
            subject=entity.id,
            predicate=predicate,
            object=go_term.id,
            negated="NOT" in qualifier,  # Handle negative associations from the GAF qualifier field
            has_evidence=[f"ECO:{evidence_code}"],  # Biolink pydantic model centric: Formats evidence as ECO CURIE
            publications=publications_list,
            sources=build_association_knowledge_sources(
                primary=INFORES_GOA,  # GOA as the primary source
                aggregating={INFORES_BIOLINK: [INFORES_GOA]}  # This repository as the aggregator
            ),
            knowledge_level=knowledge_level,
            agent_type=agent_type,
        )

    return [entity, go_term, association]