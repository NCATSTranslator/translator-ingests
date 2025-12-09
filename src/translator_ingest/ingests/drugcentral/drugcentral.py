"""DrugCentral ingest for Translator.

Transforms DrugCentral data into Biolink-compliant KGX format.
"""

import uuid
import koza
from typing import List, Iterable, Any
from loguru import logger
import subprocess
from pathlib import Path

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    DiseaseOrPhenotypicFeature,
    Protein,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    ChemicalAffectsGeneAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)

# Constants
DRUGCENTRAL_INFORES = "infores:drugcentral"

# Predicate mappings
OMOP_RELATION_MAP = {
    'off-label use': 'biolink:applied_to_treat',
    'reduce risk': 'biolink:preventative_for_condition',
    'contraindication': 'biolink:contraindicated_in',
    'symptomatic treatment': 'biolink:treats',
    'indication': 'biolink:treats',
    'diagnosis': 'biolink:diagnoses'
}

# Knowledge source mappings
ACT_SOURCE_MAP = {
    'IUPHAR': 'infores:gtopdb',
    'KEGG DRUG': 'infores:kegg',
    'PDSP': 'infores:pdsp',
    'CHEMBL': 'infores:chembl',
    'DRUGBANK': 'infores:drugbank'
}

# Action type to predicate mappings (from DGIDB mappings)
ACTION_TYPE_MAP = {
    'ANTAGONIST': 'biolink:decreases_activity_of',
    'AGONIST': 'biolink:increases_activity_of',
    'POSITIVE MODULATOR': 'biolink:increases_activity_of',
    'GATING INHIBITOR': 'biolink:decreases_activity_of',
    'BLOCKER': 'biolink:decreases_activity_of',
    'NEGATIVE MODULATOR': 'biolink:decreases_activity_of',
    'ACTIVATOR': 'biolink:increases_activity_of',
    'BINDING AGENT': 'biolink:directly_physically_interacts_with',
    'ANTISENSE INHIBITOR': 'biolink:decreases_activity_of',
    'POSITIVE ALLOSTERIC MODULATOR': 'biolink:increases_activity_of',
    'INVERSE AGONIST': 'biolink:increases_activity_of',
    'PHARMACOLOGICAL CHAPERONE': 'biolink:directly_physically_interacts_with',
    'PARTIAL AGONIST': 'biolink:increases_activity_of',
    'NEGATIVE ALLOSTERIC MODULATOR': 'biolink:decreases_activity_of',
    'ANTIBODY BINDING': 'biolink:directly_physically_interacts_with',
    'ALLOSTERIC ANTAGONIST': 'biolink:decreases_activity_of',
    'INHIBITOR': 'biolink:decreases_activity_of',
    'OPENER': 'biolink:increases_activity_of',
    'SUBSTRATE': 'biolink:is_substrate_of',
    'MODULATOR': 'biolink:affects',
    'ALLOSTERIC MODULATOR': 'biolink:affects',
    'RELEASING AGENT': 'biolink:directly_physically_interacts_with'
}

# Act type to predicate mappings
ACT_TYPE_MAP = {
    'IC50': 'biolink:decreases_activity_of',
    'Kd': 'biolink:directly_physically_interacts_with',
    'AC50': 'biolink:increases_activity_of',
    'Ki': 'biolink:decreases_activity_of',
    'EC50': 'biolink:increases_activity_of'
}


def get_latest_version() -> str:
    """Get the latest DrugCentral version.

    Currently returns a hardcoded version as DrugCentral doesn't have
    a versioning API.
    """
    return "2023_11_01"


@koza.prepare_data()
def prepare_drugcentral_data(koza_transform: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Prepare DrugCentral data by running preprocessing script to generate TSV files."""
    logger.info("Preparing DrugCentral data: running preprocessing to generate TSV files...")
    
    # Get the data directory where TSV files should be written
    data_dir = Path(koza_transform.input_files_dir)
    
    # Check if TSV files already exist
    required_files = ['structures.tsv', 'chemical_phenotype.tsv', 'adverse_events.tsv', 'bioactivity.tsv']
    files_exist = all((data_dir / f).exists() for f in required_files)
    
    if files_exist:
        logger.info("TSV files already exist, skipping preprocessing")
    else:
        logger.info(f"Running preprocessing script to generate TSV files in {data_dir}")
        
        # Run the preprocessing script
        preprocess_script = Path(__file__).parent / 'preprocess.py'
        
        try:
            result = subprocess.run(
                ['python', str(preprocess_script), '--output-dir', str(data_dir)],
                capture_output=True,
                text=True,
                check=True
            )
            logger.info("Preprocessing completed successfully")
            logger.debug(f"Preprocessing output: {result.stdout}")
            
            # Verify files were created
            missing_files = [f for f in required_files if not (data_dir / f).exists()]
            if missing_files:
                raise RuntimeError(f"Preprocessing failed to create files: {missing_files}")
                
        except subprocess.CalledProcessError as e:
            logger.error(f"Preprocessing failed: {e.stderr}")
            raise RuntimeError(f"Failed to run preprocessing script: {e}")
    
    # Return empty iterator as we're just preparing files
    return iter([])


@koza.transform_record(tag="structures")
def transform_structure_node(koza_transform: koza.KozaTransform, row: dict) -> List:
    """Transform chemical structure data to ChemicalEntity nodes."""
    struct_id = f"DRUGCENTRAL:{row['struct_id']}"

    # Create chemical entity node
    chemical = ChemicalEntity(
        id=struct_id,
        category=["biolink:ChemicalEntity"],
        name=None,  # Could be added from structures table if available
    )

    # Add properties if available
    if row.get('inchikey'):
        chemical.has_attribute = chemical.has_attribute or []
        chemical.has_attribute.append({
            "attribute_type_id": "biolink:inchikey",
            "value": row['inchikey']
        })

    if row.get('smiles'):
        chemical.has_attribute = chemical.has_attribute or []
        chemical.has_attribute.append({
            "attribute_type_id": "biolink:smiles",
            "value": row['smiles']
        })

    if row.get('formula'):
        chemical.has_attribute = chemical.has_attribute or []
        chemical.has_attribute.append({
            "attribute_type_id": "biolink:chemical_formula",
            "value": row['formula']
        })

    if row.get('molecular_weight'):
        chemical.has_attribute = chemical.has_attribute or []
        chemical.has_attribute.append({
            "attribute_type_id": "biolink:molecular_weight",
            "value": float(row['molecular_weight'])
        })

    return [chemical]


@koza.transform_record(tag="chemical_phenotype")
def transform_chemical_phenotype(koza_transform: koza.KozaTransform, row: dict) -> List:
    """Transform chemical-phenotype relationships."""
    entities = []
    associations = []

    struct_id = f"DRUGCENTRAL:{row['struct_id']}"

    # Determine phenotype ID - prefer UMLS, fallback to SNOMED
    if row.get('umls_cui'):
        phenotype_id = f"UMLS:{row['umls_cui']}"
    elif row.get('snomed_conceptid'):
        phenotype_id = f"SNOMEDCT:{row['snomed_conceptid']}"
    else:
        logger.warning(f"No phenotype ID for struct_id {struct_id}")
        return []

    # Get predicate from relationship name
    relationship = row.get('relationship_name', '')
    predicate = OMOP_RELATION_MAP.get(relationship, 'biolink:related_to')

    if predicate == 'biolink:related_to':
        logger.warning(f"Unknown relationship type: {relationship}")
        return []

    # Create minimal nodes
    chemical = ChemicalEntity(
        id=struct_id,
        category=["biolink:ChemicalEntity"]
    )
    entities.append(chemical)

    phenotype = DiseaseOrPhenotypicFeature(
        id=phenotype_id,
        category=["biolink:DiseaseOrPhenotypicFeature"],
        name=row.get('snomed_full_name')
    )
    entities.append(phenotype)

    # Create association
    association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        id="uuid:" + str(uuid.uuid4()),
        category=["biolink:ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation"],
        subject=struct_id,
        predicate=predicate,
        object=phenotype_id,
        primary_knowledge_source=DRUGCENTRAL_INFORES,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )
    associations.append(association)

    return entities + associations


@koza.transform_record(tag="adverse_events")
def transform_adverse_events(koza_transform: koza.KozaTransform, row: dict) -> List:
    """Transform adverse event data from FAERS."""
    entities = []
    associations = []

    struct_id = f"DRUGCENTRAL:{row['struct_id']}"
    meddra_id = f"MEDDRA:{row['meddra_code']}"

    # Create minimal nodes
    chemical = ChemicalEntity(
        id=struct_id,
        category=["biolink:ChemicalEntity"]
    )
    entities.append(chemical)

    phenotype = DiseaseOrPhenotypicFeature(
        id=meddra_id,
        category=["biolink:DiseaseOrPhenotypicFeature"]
    )
    entities.append(phenotype)

    # Create association
    association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        id="uuid:" + str(uuid.uuid4()),
        category=["biolink:ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation"],
        subject=struct_id,
        predicate="biolink:has_adverse_event",
        object=meddra_id,
        primary_knowledge_source="infores:faers",
        aggregator_knowledge_source=[DRUGCENTRAL_INFORES],
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        has_attribute=[{
            "attribute_type_id": "FAERS_llr",
            "value": float(row['llr'])
        }]
    )
    associations.append(association)

    return entities + associations


@koza.transform_record(tag="bioactivity")
def transform_bioactivity(koza_transform: koza.KozaTransform, row: dict) -> List:
    """Transform bioactivity data."""
    entities = []
    associations = []

    struct_id = f"DRUGCENTRAL:{row['struct_id']}"
    protein_id = f"UniProtKB:{row['accession']}"

    # Determine predicate from action type or act type
    action_type = row.get('action_type', '')
    act_type = row.get('act_type', '')

    # Try action type first, then act type
    if action_type and action_type.upper() in ACTION_TYPE_MAP:
        predicate = ACTION_TYPE_MAP[action_type.upper()]
    elif act_type and act_type in ACT_TYPE_MAP:
        predicate = ACT_TYPE_MAP[act_type]
    else:
        predicate = 'biolink:directly_physically_interacts_with'

    # Create minimal nodes
    chemical = ChemicalEntity(
        id=struct_id,
        category=["biolink:ChemicalEntity"]
    )
    entities.append(chemical)

    protein = Protein(
        id=protein_id,
        category=["biolink:Protein"]
    )
    entities.append(protein)

    # Determine knowledge sources
    act_source = row.get('act_source', '')
    primary_source = ACT_SOURCE_MAP.get(act_source, DRUGCENTRAL_INFORES)

    sources = []
    if primary_source == DRUGCENTRAL_INFORES:
        sources.append(RetrievalSource(
            id=DRUGCENTRAL_INFORES,
            resource_id=DRUGCENTRAL_INFORES,
            resource_role=ResourceRoleEnum.primary_knowledge_source
        ))
    else:
        sources.append(RetrievalSource(
            id=primary_source,
            resource_id=primary_source,
            resource_role=ResourceRoleEnum.primary_knowledge_source
        ))
        sources.append(RetrievalSource(
            id=DRUGCENTRAL_INFORES,
            resource_id=DRUGCENTRAL_INFORES,
            resource_role=ResourceRoleEnum.aggregator_knowledge_source
        ))

    # Create association
    association = ChemicalAffectsGeneAssociation(
        id="uuid:" + str(uuid.uuid4()),
        category=["biolink:ChemicalAffectsGeneAssociation"],
        subject=struct_id,
        predicate=predicate,
        object=protein_id,
        sources=sources,
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )

    # Add affinity information if available
    if row.get('act_value') and row.get('act_type'):
        association.has_attribute = association.has_attribute or []
        association.has_attribute.extend([
            {
                "attribute_type_id": "biolink:affinity",
                "value": float(row['act_value'])
            },
            {
                "attribute_type_id": "biolink:affinity_parameter",
                "value": f"p{row['act_type']}"
            }
        ])

    # Add publication if from scientific literature
    if act_source == 'SCIENTIFIC LITERATURE' and row.get('act_source_url'):
        url = row['act_source_url']
        if url.startswith('http://www.ncbi.nlm.nih.gov/pubmed'):
            pmid = f"PMID:{url.split('/')[-1]}"
            association.publications = [pmid]

    associations.append(association)

    return entities + associations
