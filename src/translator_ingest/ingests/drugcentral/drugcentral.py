"""DrugCentral ingest for Translator.

Transforms DrugCentral data into Biolink-compliant KGX format.
"""

import os
import uuid
import koza
from typing import List, Iterable, Any, Dict
from loguru import logger
import psycopg2
import psycopg2.extras

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
from koza.model.graphs import KnowledgeGraph

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

# Filter out semantic types that don't represent diseases/conditions
EXCLUDED_STYS = [
    'T002', 'T007', 'T034', 'T040', 'T042', 'T058', 'T059', 'T060', 'T061',
    'T109', 'T121', 'T130', 'T131', 'T167'
]


def get_latest_version() -> str:
    """Get the latest DrugCentral version.
    
    Currently returns a hardcoded version as DrugCentral doesn't have
    a versioning API.
    """
    return "2023_11_01"


def connect_to_db() -> psycopg2.extensions.connection:
    """Connect to DrugCentral database."""
    # Try environment variables first
    db_host = os.environ.get('DRUGCENTRAL_DB_HOST', 'unmtid-dbs.net')
    db_user = os.environ.get('DRUGCENTRAL_DB_USER', 'drugman')
    db_password = os.environ.get('DRUGCENTRAL_DB_PASSWORD', 'dosage')
    db_name = os.environ.get('DRUGCENTRAL_DB_NAME', 'drugcentral')
    db_port = os.environ.get('DRUGCENTRAL_DB_PORT', '5433')
    
    logger.info(f"Connecting to DrugCentral database at {db_host}:{db_port}")
    
    connection = psycopg2.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        port=db_port
    )
    
    logger.info("Successfully connected to DrugCentral database")
    return connection


@koza.prepare_data(tag="drugcentral")
def prepare_drugcentral_data(koza_transform: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]]:
    """Prepare DrugCentral data by querying database and yielding records."""
    logger.info("Preparing DrugCentral data from database...")
    
    try:
        # Connect to database
        connection = connect_to_db()
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # First, get structure data for node properties
        structures = {}
        logger.info("Fetching structure data...")
        query = '''
            SELECT id, inchikey, smiles, cd_formula, cd_molweight
            FROM public.structures
        '''
        cursor.execute(query)
        for row in cursor:
            structures[row['id']] = {
                'struct_id': row['id'],
                'inchikey': row['inchikey'],
                'smiles': row['smiles'],
                'formula': row['cd_formula'],
                'molecular_weight': row['cd_molweight']
            }
        logger.info(f"Loaded {len(structures)} structures")
        
        # Store structures in transform metadata for later use
        koza_transform.transform_metadata['structures'] = structures
        
        # Query chemical-phenotype relationships
        logger.info("Fetching chemical-phenotype relationships...")
        excluded_stys_sql = ', '.join(f"'{sty}'" for sty in EXCLUDED_STYS)
        query = f'''
            SELECT struct_id, relationship_name, umls_cui, cui_semantic_type, 
                   snomed_conceptid, snomed_full_name
            FROM public.omop_relationship
            WHERE umls_cui IS NOT NULL
            AND (cui_semantic_type IS NULL OR cui_semantic_type NOT IN ({excluded_stys_sql}))
        '''
        cursor.execute(query)
        for row in cursor:
            yield {
                'record_type': 'chemical_phenotype',
                'struct_id': row['struct_id'],
                'relationship_name': row['relationship_name'],
                'umls_cui': row['umls_cui'],
                'cui_semantic_type': row['cui_semantic_type'],
                'snomed_conceptid': row['snomed_conceptid'],
                'snomed_full_name': row['snomed_full_name']
            }
        
        # Query adverse events
        logger.info("Fetching adverse events...")
        query = '''
            SELECT struct_id, meddra_code, llr 
            FROM public.faers 
            WHERE llr > llr_threshold 
            AND drug_ae > 25
        '''
        cursor.execute(query)
        for row in cursor:
            yield {
                'record_type': 'adverse_event',
                'struct_id': row['struct_id'],
                'meddra_code': row['meddra_code'],
                'llr': row['llr']
            }
        
        # Query bioactivity data
        logger.info("Fetching bioactivity data...")
        query = '''
            SELECT a.struct_id, a.act_value, a.act_unit, 
                   a.act_type, a.act_source, a.act_source_url, 
                   a.action_type, dc.component_id, c.accession
            FROM public.act_table_full a
            JOIN public.td2tc dc ON a.target_id = dc.target_id
            JOIN public.target_component c ON dc.component_id = c.id
        '''
        cursor.execute(query)
        for row in cursor:
            yield {
                'record_type': 'bioactivity',
                'struct_id': row['struct_id'],
                'act_value': row['act_value'],
                'act_unit': row['act_unit'],
                'act_type': row['act_type'],
                'act_source': row['act_source'],
                'act_source_url': row['act_source_url'],
                'action_type': row['action_type'],
                'component_id': row['component_id'],
                'accession': row['accession']
            }
        
        # Close database connection
        cursor.close()
        connection.close()
        logger.info("Database connection closed")
        
    except Exception as e:
        logger.error(f"Error querying DrugCentral database: {e}")
        raise


@koza.transform_record(tag="drugcentral")
def transform(koza_transform: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """Transform DrugCentral records to KnowledgeGraph."""
    record_type = record.get('record_type')
    
    if record_type == 'chemical_phenotype':
        return transform_chemical_phenotype(koza_transform, record)
    elif record_type == 'adverse_event':
        return transform_adverse_event(koza_transform, record)
    elif record_type == 'bioactivity':
        return transform_bioactivity(koza_transform, record)
    else:
        logger.warning(f"Unknown record type: {record_type}")
        return None


def transform_chemical_phenotype(koza_transform: koza.KozaTransform, row: dict) -> KnowledgeGraph | None:
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
        return None
    
    # Get predicate from relationship name
    relationship = row.get('relationship_name', '')
    predicate = OMOP_RELATION_MAP.get(relationship, 'biolink:related_to')
    
    if predicate == 'biolink:related_to':
        logger.warning(f"Unknown relationship type: {relationship}")
        return None
    
    # Create chemical node
    chemical = ChemicalEntity(
        id=struct_id,
        category=["biolink:ChemicalEntity"]
    )
    
    entities.append(chemical)
    
    # Create phenotype node
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
    
    return KnowledgeGraph(nodes=entities, edges=associations)


def transform_adverse_event(koza_transform: koza.KozaTransform, row: dict) -> KnowledgeGraph | None:
    """Transform adverse event data from FAERS."""
    entities = []
    associations = []
    
    struct_id = f"DRUGCENTRAL:{row['struct_id']}"
    meddra_id = f"MEDDRA:{row['meddra_code']}"
    
    # Create chemical node
    chemical = ChemicalEntity(
        id=struct_id,
        category=["biolink:ChemicalEntity"]
    )
    
    entities.append(chemical)
    
    # Create phenotype node
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
    )
    associations.append(association)
    
    return KnowledgeGraph(nodes=entities, edges=associations)


def transform_bioactivity(koza_transform: koza.KozaTransform, row: dict) -> KnowledgeGraph | None:
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
    
    # Create chemical node
    chemical = ChemicalEntity(
        id=struct_id,
        category=["biolink:ChemicalEntity"]
    )
    
    entities.append(chemical)
    
    # Create protein node
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
    
    # TODO: Add affinity information as attributes when attribute format is clarified
    
    # Add publication if from scientific literature
    if act_source == 'SCIENTIFIC LITERATURE' and row.get('act_source_url'):
        url = row['act_source_url']
        if url.startswith('http://www.ncbi.nlm.nih.gov/pubmed'):
            pmid = f"PMID:{url.split('/')[-1]}"
            association.publications = [pmid]
    
    associations.append(association)
    
    return KnowledgeGraph(nodes=entities, edges=associations)