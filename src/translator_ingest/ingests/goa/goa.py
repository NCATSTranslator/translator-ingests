import uuid
import koza
from typing import Iterable, Any

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

# Constants
INFORES_GOA = "infores:goa"
INFORES_BIOLINK = "infores:biolink"

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

# Mappings for GO aspects to Biolink predicates
# Note: Biolink pydantic model doesn't expose predicate constants programmatically from the YAML slots section,
# so we use hardcoded mappings. This could be enhanced if biolink-model adds predicate registry in future versions.
# The YAML file contains predicate definitions like "participates in:", "enables:", "located in:" in the slots section,
# but these are not exposed as constants in the generated biolink pydantic model.
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
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> (Iterable[NamedThing], Iterable[Association]):
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
    publications = record["DB_Reference"].split("|")  # Pipe-separated publication references
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
        entity_id = db_object_id
    else:
        # DB_Object_ID doesn't include prefix, so we add it (e.g., "A0A024RBG1" -> "UniProtKB:A0A024RBG1")
        entity_id = f"{db_source}:{db_object_id}"
    
    entity = biolink_class(
        id=entity_id,
        name=db_object_symbol,
        category=biolink_class.model_fields['category'].default,  # Dynamic category from Biolink model
        in_taxon=[taxon.replace("taxon:", "NCBITaxon:")],  # Convert GO taxon format to Biolink NCBI format
        description=db_object_name if db_object_name else None,  # Include full entity name as description
    )

    # Create GO term node using appropriate biolink class based on aspect
    # GO aspects map to specific biolink classes for proper semantic categorization:
    # P (Process) -> BiologicalProcess, F (Function) -> MolecularActivity, C (Component) -> CellularComponent
    go_biolink_class = GO_ASPECT_TO_BIOLINK_CLASS.get(aspect, NamedThing)
    go_term = go_biolink_class(
        id=go_id,
        category=go_biolink_class.model_fields['category'].default  # Dynamic category from Biolink model
    )

    # Get predicate from aspect mapping
    # Biolink pydantic model centric: Uses biolink predicate IRIs from hardcoded mapping since biolink model 
    # doesn't expose predicate constants from YAML slots section
    predicate = ASPECT_TO_PREDICATE.get(aspect)
    if not predicate:
        koza.log(f"Unknown aspect '{aspect}' for record: {record}", level="WARNING")
        return [], []

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
    for p in publications:
        if p and p.strip():
            if p.startswith("PMID:"):
                publications_list.append(p)
            else:
                publications_list.append(f"PMID:{p}")

    # Create association dynamically based on the biolink class
    # Biolink pydantic model centric: Uses appropriate association class based on the entity type
    # For Gene entities, use GeneToGoTermAssociation; for other entities, use generic Association
    # since there are no specific associations for Protein, MacromolecularComplex, or RNAProduct in the biolink model
    if biolink_class == Gene:
        # Use GeneToGoTermAssociation for gene entities
        association = GeneToGoTermAssociation(
            id=str(uuid.uuid4()),
            subject=entity.id,
            predicate=predicate,
            object=go_term.id,
            negated="NOT" in qualifier,  # Handle negative associations from GAF qualifier field
            has_evidence=[f"ECO:{evidence_code}"],  # Biolink pydantic model centric: Formats evidence as ECO CURIE
            publications=publications_list,
            primary_knowledge_source=INFORES_GOA,  # GOA as the primary source
            aggregator_knowledge_source=[INFORES_BIOLINK],  # This repository as aggregator
            knowledge_level=knowledge_level,
            agent_type=agent_type,
        )
    else:
        # Use generic Association for protein, complex, and RNA entities since there are no specific associations
        association = Association(
            id=str(uuid.uuid4()),
            subject=entity.id,
            predicate=predicate,
            object=go_term.id,
            negated="NOT" in qualifier,  # Handle negative associations from GAF qualifier field
            has_evidence=[f"ECO:{evidence_code}"],  # Biolink pydantic model centric: Formats evidence as ECO CURIE
            publications=publications_list,
            primary_knowledge_source=INFORES_GOA,  # GOA as the primary source
            aggregator_knowledge_source=[INFORES_BIOLINK],  # This repository as aggregator
            knowledge_level=knowledge_level,
            agent_type=agent_type,
        )

    return [entity, go_term], [association]