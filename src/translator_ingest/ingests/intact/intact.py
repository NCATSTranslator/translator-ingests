from typing import Any
import re
import requests
import koza

from biolink_model.datamodel.pydanticmodel_v2 import (
    Protein,
    Gene,
    SmallMolecule,
    PairwiseMolecularInteraction,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
from translator_ingest.util.biolink import INFORES_INTACT
from bmt.pydantic import entity_id, build_association_knowledge_sources
from koza.model.graphs import KnowledgeGraph

# PSI-MI interaction type to Biolink predicate mapping
# Based on common PSI-MI interaction types and their semantic equivalents in Biolink
PSI_MI_TYPE_TO_PREDICATE = {
    # Physical interactions
    "physical association": "biolink:physically_interacts_with",
    "association": "biolink:physically_interacts_with",
    "direct interaction": "biolink:physically_interacts_with",
    "colocalization": "biolink:colocalized_with",
    # Molecular interactions
    "phosphorylation reaction": "biolink:affects",
    "ubiquitination reaction": "biolink:affects",
    "acetylation reaction": "biolink:affects",
    "methylation reaction": "biolink:affects",
    "sumoylation reaction": "biolink:affects",
    # Protein complexes
    "protein complex": "biolink:interacts_with",
    # Default fallback
    "self interaction": "biolink:physically_interacts_with",
}

# Default predicate for unmapped interaction types
DEFAULT_PREDICATE = "biolink:interacts_with"


def get_latest_version() -> str:
    """
    Retrieve the latest IntAct release version from the FTP README file.

    IntAct provides release information in their FTP directory README.
    The version format is like "Release 251 - September 2025"
    """
    try:
        # Fetch the README from IntAct FTP directory
        response = requests.get("https://ftp.ebi.ac.uk/pub/databases/intact/current/README")
        response.raise_for_status()

        # Look for release information in the README
        # Example: "Release 251 - September 2025"
        match = re.search(r'Release\s+(\d+)\s*-\s*([A-Za-z]+)\s+(\d{4})', response.text)
        if match:
            release_num = match.group(1)
            month = match.group(2)
            year = match.group(3)
            return f"Release_{release_num}_{month}_{year}"

        # Fallback: try to get last modified date from the header
        if 'Last-Modified' in response.headers:
            last_modified = response.headers['Last-Modified']
            # Parse and format as YYYY-MM-DD
            from datetime import datetime
            dt = datetime.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            return dt.strftime('%Y-%m-%d')

    except Exception:
        # If we can't determine version from README, use a timestamp approach
        pass

    # Final fallback: return a generic version
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d')


def parse_psi_mi_field(field_value: str) -> dict[str, str]:
    """
    Parse a PSI-MI field value and extract the identifier and description.

    PSI-MI fields are formatted as: database:identifier(description)
    or database:"identifier"(description) for quoted identifiers
    or database:identifier or just identifier

    Examples:
        - uniprotkb:P12345(gene_name) -> {'db': 'uniprotkb', 'id': 'P12345', 'desc': 'gene_name'}
        - pubmed:12345678 -> {'db': 'pubmed', 'id': '12345678', 'desc': None}
        - P12345 -> {'db': None, 'id': 'P12345', 'desc': None}
    """
    if not field_value or field_value == "-":
        return {'db': None, 'id': None, 'desc': None}

    # Remove quotes if present
    field_value = field_value.replace('"', '')

    # Try to match database:identifier(description) pattern
    match = re.match(r'^([^:]+):([^(]+)(?:\(([^)]+)\))?$', field_value)
    if match:
        return {
            'db': match.group(1).lower(),
            'id': match.group(2),
            'desc': match.group(3) if match.group(3) else None
        }

    # If no colon, treat the whole value as an identifier
    return {'db': None, 'id': field_value, 'desc': None}


def parse_multi_value_field(field_value: str) -> list[dict[str, str]]:
    """
    Parse a multi-value PSI-MI field (values separated by |).

    Returns a list of parsed field dictionaries.
    """
    if not field_value or field_value == "-":
        return []

    values = field_value.split("|")
    return [parse_psi_mi_field(v.strip()) for v in values if v.strip()]


def extract_curie(parsed_field: dict[str, str], preferred_prefix: str = None) -> str | None:
    """
    Extract a CURIE from a parsed PSI-MI field.

    Args:
        parsed_field: Parsed field dictionary from parse_psi_mi_field
        preferred_prefix: Optional preferred CURIE prefix to use

    Returns:
        CURIE string or None
    """
    if not parsed_field['id']:
        return None

    # Use provided prefix, or the database from the field, or return bare ID
    if preferred_prefix:
        return f"{preferred_prefix}:{parsed_field['id']}"
    elif parsed_field['db']:
        # Normalize common database names to standard CURIE prefixes
        db_to_prefix = {
            'uniprotkb': 'UniProtKB',
            'uniprot': 'UniProtKB',
            'chebi': 'CHEBI',
            'pubmed': 'PMID',
            'ensembl': 'ENSEMBL',
            'entrez gene/locuslink': 'NCBIGene',
            'refseq': 'RefSeq',
            'psi-mi': 'MI',
        }
        prefix = db_to_prefix.get(parsed_field['db'].lower(), parsed_field['db'])
        return f"{prefix}:{parsed_field['id']}"
    else:
        return parsed_field['id']


def get_primary_identifier(id_field: str, alt_ids_field: str) -> tuple[str | None, str | None]:
    """
    Extract the primary identifier and determine entity type.

    Priority: UniProtKB > CHEBI > Ensembl > NCBIGene > RefSeq > first available

    Returns:
        Tuple of (primary_id, entity_type) where entity_type is 'protein', 'gene', 'small_molecule', or None
    """
    # Parse primary ID field
    primary_parsed = parse_psi_mi_field(id_field)

    # Parse alternative IDs
    alt_ids = parse_multi_value_field(alt_ids_field)

    # Combine all IDs with primary first
    all_ids = [primary_parsed] + alt_ids

    # Priority order: UniProtKB (protein), CHEBI (small molecule), gene databases
    for parsed in all_ids:
        if not parsed['id']:
            continue
        db = parsed['db'].lower() if parsed['db'] else ''

        # UniProtKB indicates a protein
        if db in ['uniprotkb', 'uniprot']:
            return extract_curie(parsed, 'UniProtKB'), 'protein'

        # CHEBI indicates a small molecule
        if db == 'chebi':
            return extract_curie(parsed, 'CHEBI'), 'small_molecule'

    # Second pass for gene databases
    for parsed in all_ids:
        if not parsed['id']:
            continue
        db = parsed['db'].lower() if parsed['db'] else ''

        if db in ['ensembl']:
            return extract_curie(parsed, 'ENSEMBL'), 'gene'
        if db in ['entrez gene/locuslink']:
            return extract_curie(parsed, 'NCBIGene'), 'gene'
        if db == 'refseq':
            return extract_curie(parsed, 'RefSeq'), 'gene'

    # Fallback: use primary ID or first alt ID
    for parsed in all_ids:
        if parsed['id']:
            curie = extract_curie(parsed)
            if curie:
                # Default to protein if we can't determine type
                return curie, 'protein'

    return None, None


def extract_publications(publications_field: str) -> list[str] | None:
    """
    Extract publication identifiers from the publicationIDs field.

    Only PMIDs are included, as per RIG guidance.
    """
    if not publications_field or publications_field == "-":
        return None

    parsed_pubs = parse_multi_value_field(publications_field)
    pmids = []

    for pub in parsed_pubs:
        if pub['db'] and pub['db'].lower() == 'pubmed' and pub['id']:
            pmids.append(f"PMID:{pub['id']}")

    return pmids if pmids else None


def get_predicate_from_interaction_type(interaction_types_field: str) -> str:
    """
    Map PSI-MI interaction type to Biolink predicate.

    Returns the most specific predicate found, or DEFAULT_PREDICATE.
    """
    if not interaction_types_field or interaction_types_field == "-":
        return DEFAULT_PREDICATE

    parsed_types = parse_multi_value_field(interaction_types_field)

    for parsed in parsed_types:
        if parsed['desc']:
            type_desc = parsed['desc'].lower()
            if type_desc in PSI_MI_TYPE_TO_PREDICATE:
                return PSI_MI_TYPE_TO_PREDICATE[type_desc]

    return DEFAULT_PREDICATE


def extract_name_from_aliases(aliases_field: str) -> str | None:
    """
    Extract a human-readable name from the aliases field.

    Prefers gene names over other alias types.
    """
    if not aliases_field or aliases_field == "-":
        return None

    parsed_aliases = parse_multi_value_field(aliases_field)

    # First pass: look for gene names
    for alias in parsed_aliases:
        if alias['desc'] and 'gene name' in alias['desc'].lower() and alias['id']:
            return alias['id']

    # Second pass: use first available alias
    for alias in parsed_aliases:
        if alias['id']:
            return alias['id']

    return None


@koza.transform_record()
def transform_record(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    """
    Transform a single IntAct MITAB record into Biolink nodes and edges.

    This function processes protein-protein interactions from IntAct, creating
    Protein/Gene nodes and PairwiseMolecularInteraction edges.

    The implementation focuses on:
    1. Extracting primary identifiers from complex PSI-MI ID fields
    2. Determining entity types (protein, gene, small molecule)
    3. Mapping PSI-MI interaction types to Biolink predicates
    4. Preserving key metadata (publications, detection methods, confidence)
    """

    # Filter for human-human interactions only
    # TaxidA and TaxidB fields can contain multiple pipe-separated values
    taxid_a = record.get('taxidA', '')
    taxid_b = record.get('taxidB', '')

    if not taxid_a or not taxid_b:
        return None

    # Check if both contain human taxid (9606)
    if 'taxid:9606' not in taxid_a or 'taxid:9606' not in taxid_b:
        return None

    # Extract identifiers and determine entity types
    id_a, type_a = get_primary_identifier(record['idA'], record['altIdsA'])
    id_b, type_b = get_primary_identifier(record['idB'], record['altIdsB'])

    # Skip if we couldn't extract valid identifiers
    if not id_a or not id_b:
        koza.log(f"Skipping record with missing identifiers: idA={record['idA']}, idB={record['idB']}", level="WARNING")
        return None

    # Extract human-readable names
    name_a = extract_name_from_aliases(record['aliasesA'])
    name_b = extract_name_from_aliases(record['aliasesB'])

    # Create entity nodes based on type
    entity_a_class = Protein if type_a == 'protein' else (Gene if type_a == 'gene' else SmallMolecule)
    entity_b_class = Protein if type_b == 'protein' else (Gene if type_b == 'gene' else SmallMolecule)

    entity_a = entity_a_class(
        id=id_a,
        name=name_a if name_a else id_a,
        category=entity_a_class.model_fields['category'].default,
        in_taxon=["NCBITaxon:9606"],  # Human only, as per filters
    )

    entity_b = entity_b_class(
        id=id_b,
        name=name_b if name_b else id_b,
        category=entity_b_class.model_fields['category'].default,
        in_taxon=["NCBITaxon:9606"],  # Human only, as per filters
    )

    # Extract predicate from interaction type
    predicate = get_predicate_from_interaction_type(record['interactionTypes'])

    # Extract publications (PMIDs only)
    publications = extract_publications(record['publicationIDs'])

    # Create the interaction edge
    interaction = PairwiseMolecularInteraction(
        id=entity_id(),
        subject=entity_a.id,
        predicate=predicate,
        object=entity_b.id,
        publications=publications,
        sources=build_association_knowledge_sources(primary=INFORES_INTACT),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )

    return KnowledgeGraph(nodes=[entity_a, entity_b], edges=[interaction])
