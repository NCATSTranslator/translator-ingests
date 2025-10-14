"""
Alliance of Genome Resources ingest for mouse and rat data.

This ingest processes gene, disease, phenotype, expression, genotype, and allele data
from the Alliance of Genome Resources, filtering for mouse (NCBITaxon:10090) and
rat (NCBITaxon:10116) only.

Unlike the alliance-ingest implementation, this translator ingest takes ALL mouse and rat
data without additional content filtering (e.g., experimental conditions, modifiers, etc.).
"""

import uuid
import koza
from typing import Any, List
from loguru import logger
from koza.model.graphs import KnowledgeGraph
from pathlib import Path
import duckdb

from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    Association,
    GeneToDiseaseAssociation,
    GenotypeToDiseaseAssociation,
    VariantToDiseaseAssociation,
    GeneToPhenotypicFeatureAssociation,
    GenotypeToPhenotypicFeatureAssociation,
    VariantToPhenotypicFeatureAssociation,
    GeneToExpressionSiteAssociation,
    Genotype,
    GenotypeToGeneAssociation,
    GenotypeToVariantAssociation,
    SequenceVariant,
    VariantToGeneAssociation,
    Disease,
    PhenotypicFeature,
    AnatomicalEntity,
    CellularComponent,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

# Source mapping for infores
SOURCE_MAP = {
    "FB": "infores:flybase",
    "MGI": "infores:mgi",
    "RGD": "infores:rgd",
    "HGNC": "infores:rgd",
    "SGD": "infores:sgd",
    "WB": "infores:wormbase",
    "Xenbase": "infores:xenbase",
    "ZFIN": "infores:zfin",
}

# Mouse and rat taxon IDs
MOUSE_TAXON = "NCBITaxon:10090"
RAT_TAXON = "NCBITaxon:10116"
MOUSE_RAT_TAXA = {MOUSE_TAXON, RAT_TAXON}

# Taxon label mapping
TAXON_LABEL_MAP = {
    "NCBITaxon:10090": "Mus musculus",
    "NCBITaxon:10116": "Rattus norvegicus",
}

# Global variable to hold the DuckDB connection for entity lookup
_entity_lookup_conn = None


def build_entity_lookup_db(data_dir: str = "data/alliance"):
    """
    Build in-memory DuckDB table from Alliance source files.

    Creates a table with (id, category) for all genes, genotypes, and alleles
    by reading directly from the compressed JSON and TSV source files.
    """
    global _entity_lookup_conn

    if _entity_lookup_conn is not None:
        logger.debug("Entity lookup DB already built, skipping")
        return  # Already built

    logger.info("Building entity lookup DB...")

    conn = duckdb.connect(":memory:")
    data_path = Path(data_dir)

    try:
        # Load genes from BGI JSON files - DuckDB reads .gz directly
        conn.execute(f"""
            CREATE TABLE entities AS
            SELECT
                unnest.basicGeneticEntity.primaryId as id,
                'biolink:Gene' as category
            FROM read_json('{data_path}/BGI_*.json.gz',
                           format='auto',
                           maximum_object_size=2000000000),
            unnest(data)
            WHERE unnest.basicGeneticEntity.primaryId IS NOT NULL
        """)

        # Load genotypes from AGM JSON files
        conn.execute(f"""
            INSERT INTO entities
            SELECT
                unnest.primaryID as id,
                'biolink:Genotype' as category
            FROM read_json('{data_path}/AGM_*.json.gz',
                           format='auto',
                           maximum_object_size=2000000000),
            unnest(data)
            WHERE unnest.primaryID IS NOT NULL
        """)

        # Load alleles from VARIANT-ALLELE TSV files
        conn.execute(f"""
            INSERT INTO entities
            SELECT DISTINCT
                AlleleId as id,
                'biolink:SequenceVariant' as category
            FROM read_csv('{data_path}/VARIANT-ALLELE_*.tsv.gz',
                          delim='\t',
                          header=true)
            WHERE AlleleId IS NOT NULL AND AlleleId != '-'
        """)

        # Create index for fast lookup
        conn.execute("CREATE INDEX idx_entity_id ON entities(id)")

        entity_count = conn.execute('SELECT COUNT(*) FROM entities').fetchone()[0]
        logger.info(f"Built entity lookup table with {entity_count:,} entities")

        _entity_lookup_conn = conn

    except Exception as e:
        logger.error(f"Error building entity lookup DB: {e}")
        if conn:
            conn.close()
        raise


def lookup_entity_category(entity_id: str) -> str | None:
    """
    Look up the biolink category for an entity ID.

    Args:
        entity_id: The entity identifier (e.g., "MGI:12345")

    Returns:
        The biolink category (e.g., "biolink:Gene") or None if not found.
    """
    if _entity_lookup_conn is None:
        logger.warning("Entity lookup DB not initialized")
        return None

    try:
        result = _entity_lookup_conn.execute(
            "SELECT category FROM entities WHERE id = ?",
            [entity_id]
        ).fetchone()

        return result[0] if result else None
    except Exception as e:
        logger.error(f"Error looking up entity {entity_id}: {e}")
        return None


def cleanup_entity_lookup_db():
    """Close the DuckDB connection and free resources."""
    global _entity_lookup_conn
    if _entity_lookup_conn:
        _entity_lookup_conn.close()
        _entity_lookup_conn = None


def get_latest_version() -> str:
    """
    Get the latest Alliance release version from the Alliance API.

    Fetches release information from https://www.alliancegenome.org/api/releaseInfo
    which returns JSON with 'releaseVersion', 'releaseDate', and 'snapShotDate'.

    Returns:
        str: The release version (e.g., "8.2.0") or "unknown" if unable to fetch.
    """
    try:
        import requests

        response = requests.get("https://www.alliancegenome.org/api/releaseInfo", timeout=10)
        response.raise_for_status()

        data = response.json()
        version = data.get('releaseVersion')

        if version:
            logger.info(f"Found Alliance version: {version}")
            return version
        else:
            logger.warning("releaseVersion not found in API response")
            return "unknown"

    except Exception as e:
        logger.warning(f"Error fetching Alliance version from API: {e}, using fallback")
        return "unknown"


def get_data(data: dict, key: str):
    """Extract data from nested dict using dot notation."""
    keys = key.split('.')
    value = data
    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return None
    return value


@koza.on_data_begin(tag="disease")
def initialize_entity_lookup_disease(koza_transform):
    """Initialize entity lookup DB before processing disease data."""
    build_entity_lookup_db()


@koza.on_data_begin(tag="phenotype")
def initialize_entity_lookup_phenotype(koza_transform):
    """Initialize entity lookup DB before processing phenotype data."""
    build_entity_lookup_db()


@koza.transform_record(tag="gene")
def transform_gene(koza_transform, row: dict) -> List[Gene]:
    """
    Transform gene records from BGI files.
    Filter: Only process mouse and rat genes.
    """
    # Check taxon - only process mouse and rat
    taxon_id = row["basicGeneticEntity"]["taxonId"]
    if taxon_id not in MOUSE_RAT_TAXA:
        return []

    gene_id = row["basicGeneticEntity"]["primaryId"]

    # Handle Xenbase prefix (keeping alliance-ingest logic)
    gene_id = gene_id.replace("DRSC:XB:", "Xenbase:")

    source = SOURCE_MAP[gene_id.split(":")[0]]

    if "name" not in row.keys():
        row["name"] = row["symbol"]

    # Get taxon label
    if taxon_id == MOUSE_TAXON:
        taxon_label = "Mus musculus"
    elif taxon_id == RAT_TAXON:
        taxon_label = "Rattus norvegicus"
    else:
        taxon_label = None

    gene = Gene(
        id=gene_id,
        category=["biolink:Gene"],
        symbol=row["symbol"],
        name=row["symbol"],
        full_name=row["name"].replace("\r", ""),  # Remove stray carriage returns
        type=[row["soTermId"]],
        in_taxon=[taxon_id],
        in_taxon_label=taxon_label,
        provided_by=[source],
    )

    if row["basicGeneticEntity"].get("crossReferences"):
        gene.xref = [xref["id"] for xref in row["basicGeneticEntity"]["crossReferences"]]
    if row["basicGeneticEntity"].get("synonyms"):
        gene.synonym = [synonym.replace("\r", "") for synonym in row["basicGeneticEntity"]["synonyms"]]

    return [gene]


@koza.transform_record(tag="disease")
def transform_disease(koza_transform, row: dict) -> List:
    """
    Transform disease association records.
    Filter: Only process mouse and rat associations.
    NOTE: Unlike alliance-ingest, we do NOT filter out experimental conditions or modifiers.
    """
    # Filter for mouse/rat by taxon
    taxon = row.get("Taxon")
    if taxon not in MOUSE_RAT_TAXA:
        return []

    object_id = row["DBObjectID"]

    # Look up the entity category from the DuckDB lookup
    category = lookup_entity_category(object_id)

    # Check if lookup succeeded
    if category is None:
        # Track missing IDs by prefix
        prefix = object_id.split(":")[0]
        if "missing_disease_ids" not in koza_transform.state:
            koza_transform.state["missing_disease_ids"] = {}
        if prefix not in koza_transform.state["missing_disease_ids"]:
            koza_transform.state["missing_disease_ids"][prefix] = 0
        koza_transform.state["missing_disease_ids"][prefix] += 1
        return []

    # Determine the appropriate association class based on category
    if category == 'biolink:Gene':
        AssociationClass = GeneToDiseaseAssociation
        association_category = ["biolink:GeneToDiseaseAssociation"]
        predicate = "biolink:related_to"
    elif category == 'biolink:SequenceVariant':
        AssociationClass = VariantToDiseaseAssociation
        association_category = ["biolink:VariantToDiseaseAssociation"]
        predicate = "biolink:related_to"
    elif category == 'biolink:Genotype':
        AssociationClass = GenotypeToDiseaseAssociation
        association_category = ["biolink:GenotypeToDiseaseAssociation"]
        predicate = "biolink:model_of"
    else:
        logger.warning(f"Unknown category {category} for {object_id}, skipping")
        return []

    # Map association type to predicate
    if row["AssociationType"] == "is_model_of":
        predicate = "biolink:model_of"
    # NOTE: Unlike alliance-ingest, we don't skip other association types
    # We keep all mouse/rat data

    # Create disease node stub
    disease = Disease(id=row["DOID"])

    association = AssociationClass(
        id=str(uuid.uuid1()),
        category=association_category,
        subject=object_id,
        predicate=predicate,
        object=row["DOID"],
        has_evidence=[row["EvidenceCode"]],
        publications=[row["Reference"]],
        primary_knowledge_source=SOURCE_MAP[object_id.split(':')[0]],
        aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent
    )

    return [disease, association]


@koza.transform_record(tag="phenotype")
def transform_phenotype(koza_transform, row: dict) -> List:
    """
    Transform phenotype association records.
    NOTE: Unlike alliance-ingest, we keep records with multiple phenotype terms.
    """
    # NOTE: Files are already mouse/rat specific, no taxon filtering needed

    # Unlike alliance-ingest, we handle multiple phenotype terms
    if len(row.get("phenotypeTermIdentifiers", [])) == 0:
        logger.warning(f"Phenotype record has 0 phenotype terms: {str(row)}")
        return []

    object_id = row["objectId"]

    # Look up the entity category from the DuckDB lookup
    category = lookup_entity_category(object_id)

    # Check if lookup succeeded
    if category is None:
        # Track missing IDs by prefix
        prefix = object_id.split(":")[0]
        if "missing_phenotype_ids" not in koza_transform.state:
            koza_transform.state["missing_phenotype_ids"] = {}
        if prefix not in koza_transform.state["missing_phenotype_ids"]:
            koza_transform.state["missing_phenotype_ids"][prefix] = 0
        koza_transform.state["missing_phenotype_ids"][prefix] += 1
        return []

    # Determine the appropriate association class based on category
    if category == 'biolink:Gene':
        EdgeClass = GeneToPhenotypicFeatureAssociation
    elif category == 'biolink:Genotype':
        EdgeClass = GenotypeToPhenotypicFeatureAssociation
    elif category == 'biolink:SequenceVariant':
        EdgeClass = VariantToPhenotypicFeatureAssociation
    else:
        logger.warning(f"Unknown category {category} for {object_id}, skipping")
        return []

    entities = []
    associations = []

    # Process all phenotype terms (unlike alliance-ingest which skips multi-phenotype)
    for pheno_term in row["phenotypeTermIdentifiers"]:
        phenotypic_feature_id = pheno_term["termId"]
        # Remove extra WB: prefix if necessary
        phenotypic_feature_id = phenotypic_feature_id.replace("WB:WBPhenotype:", "WBPhenotype:")

        # Create phenotypic feature node stub
        phenotype = PhenotypicFeature(id=phenotypic_feature_id)
        entities.append(phenotype)

        # Determine the category for the association
        if category == 'biolink:Gene':
            association_category = ["biolink:GeneToPhenotypicFeatureAssociation"]
        elif category == 'biolink:Genotype':
            association_category = ["biolink:GenotypeToPhenotypicFeatureAssociation"]
        elif category == 'biolink:SequenceVariant':
            association_category = ["biolink:VariantToPhenotypicFeatureAssociation"]

        association = EdgeClass(
            id="uuid:" + str(uuid.uuid1()),
            category=association_category,
            subject=object_id,
            predicate="biolink:has_phenotype",
            object=phenotypic_feature_id,
            publications=[row["evidence"]["publicationId"]],
            aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
            primary_knowledge_source=SOURCE_MAP[object_id.split(':')[0]],
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )

        # Add qualifiers if present
        if "conditionRelations" in row.keys() and row["conditionRelations"] is not None:
            qualifiers: List[str] = []
            for conditionRelation in row["conditionRelations"]:
                for condition in conditionRelation["conditions"]:
                    if condition.get("conditionClassId"):
                        qualifiers.append(condition["conditionClassId"])
            if qualifiers:
                association.qualifiers = qualifiers

        associations.append(association)

    return entities + associations


@koza.transform_record(tag="expression")
def transform_expression(koza_transform, row: dict) -> List:
    """
    Transform expression records.
    NOTE: Files are already mouse/rat specific.
    """
    try:
        gene_id = get_data(row, "geneId")

        # Handle Xenbase prefix
        gene_id = gene_id.replace("DRSC:XB:", "Xenbase:")

        db = gene_id.split(":")[0]
        source = SOURCE_MAP[db]

        cellular_component_id = get_data(row, "whereExpressed.cellularComponentTermId")
        anatomical_entity_id = get_data(row, "whereExpressed.anatomicalStructureTermId")
        stage_term_id = get_data(row, "whenExpressed.stageTermId")

        publication_ids = [get_data(row, "evidence.publicationId")]
        xref = get_data(row, "crossReference.id")
        if xref:
            publication_ids.append(xref)

        entities = []
        associations = []

        # Prefer anatomical structure, fall back to cellular component
        if anatomical_entity_id:
            # Create anatomical entity node stub
            anatomy = AnatomicalEntity(id=anatomical_entity_id)
            entities.append(anatomy)

            associations.append(
                GeneToExpressionSiteAssociation(
                    id="uuid:" + str(uuid.uuid1()),
                    category=["biolink:GeneToExpressionSiteAssociation"],
                    subject=gene_id,
                    predicate="biolink:expressed_in",
                    object=anatomical_entity_id,
                    stage_qualifier=stage_term_id,
                    qualifiers=([get_data(row, "assay")] if get_data(row, "assay") else None),
                    publications=publication_ids,
                    aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
                    primary_knowledge_source=source,
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
            )
        elif cellular_component_id:
            # Create cellular component node stub
            cell_component = CellularComponent(id=cellular_component_id)
            entities.append(cell_component)

            associations.append(
                GeneToExpressionSiteAssociation(
                    id="uuid:" + str(uuid.uuid1()),
                    category=["biolink:GeneToExpressionSiteAssociation"],
                    subject=gene_id,
                    predicate="biolink:expressed_in",
                    object=cellular_component_id,
                    stage_qualifier=stage_term_id,
                    qualifiers=([get_data(row, "assay")] if get_data(row, "assay") else None),
                    publications=publication_ids,
                    aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
                    primary_knowledge_source=source,
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
            )
        else:
            logger.error(
                f"Gene expression record has no ontology terms specified for expression site: {str(row)}"
            )
            return []

        return entities + associations

    except Exception as exc:
        logger.error(f"Alliance gene expression ingest parsing exception for data row: {str(row)}\n{str(exc)}")
        return []


@koza.transform_record(tag="genotype")
def transform_genotype(koza_transform, row: dict) -> List:
    """
    Transform genotype/AGM records.
    NOTE: Files are already mouse/rat specific.
    """
    # NOTE: Files are already mouse/rat specific, no taxon filtering needed

    genotype = Genotype(
        id=row["primaryID"],
        category=["biolink:Genotype"],
        type=[row["subtype"]] if "subtype" in row else None,
        name=row["name"],
        in_taxon=[row["taxonId"]],
        in_taxon_label=TAXON_LABEL_MAP.get(row["taxonId"]),
    )
    entities = [genotype]

    # Create associations for alleles/variants
    for allele in row.get("affectedGenomicModelComponents", []):
        allele_id = allele["alleleID"]

        genotype_to_variant_association = GenotypeToVariantAssociation(
            id=str(uuid.uuid4()),
            category=["biolink:GenotypeToVariantAssociation"],
            subject=genotype.id,
            predicate="biolink:has_sequence_variant",
            object=allele_id,
            qualifier=allele.get("zygosity"),
            primary_knowledge_source=SOURCE_MAP[row["primaryID"].split(':')[0]],
            aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        entities.append(genotype_to_variant_association)

    return entities


@koza.transform_record(tag="allele")
def transform_allele(koza_transform, row: dict) -> List:
    """
    Transform allele/variant records.
    NOTE: Files are already mouse/rat specific (by taxon).
    """
    # NOTE: Files are already mouse/rat specific, no taxon filtering needed

    # Skip rows without allele IDs
    if not row.get("AlleleId") or row["AlleleId"] == "-":
        return []

    allele_id = row["AlleleId"]
    source = allele_id.split(":")[0] if ":" in allele_id else None

    # Parse synonyms if available
    synonyms = []
    if row.get("AlleleSynonyms") and row["AlleleSynonyms"] != "-":
        synonyms = [syn.strip() for syn in row["AlleleSynonyms"].split(",")]

    allele = SequenceVariant(
        id=allele_id,
        category=["biolink:SequenceVariant"],
        name=row["AlleleSymbol"],
        in_taxon=[row["Taxon"]],
        in_taxon_label=row["SpeciesName"],
        synonym=synonyms if synonyms else None,
    )

    entities = [allele]

    # Add allele to gene associations if gene IDs are available
    if row.get("AlleleAssociatedGeneId") and row["AlleleAssociatedGeneId"] != "-":
        allele_to_gene = VariantToGeneAssociation(
            id=str(uuid.uuid4()),
            category=["biolink:VariantToGeneAssociation"],
            subject=allele_id,
            predicate="biolink:is_sequence_variant_of",
            object=row["AlleleAssociatedGeneId"],
            primary_knowledge_source=SOURCE_MAP.get(source, "infores:agrkb"),
            aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        entities.append(allele_to_gene)

    return entities


@koza.on_data_end(tag="disease")
def report_and_cleanup_disease(koza_transform):
    """Report missing disease ID counts and cleanup at the end of processing."""
    if "missing_disease_ids" in koza_transform.state and koza_transform.state["missing_disease_ids"]:
        logger.warning("Disease association IDs not found in entity lookup:")
        for prefix, count in sorted(koza_transform.state["missing_disease_ids"].items()):
            logger.warning(f"  {prefix}: {count} IDs")
    cleanup_entity_lookup_db()


@koza.on_data_end(tag="phenotype")
def report_and_cleanup_phenotype(koza_transform):
    """Report missing phenotype ID counts and cleanup at the end of processing."""
    if "missing_phenotype_ids" in koza_transform.state and koza_transform.state["missing_phenotype_ids"]:
        logger.warning("Phenotype association IDs not found in entity lookup:")
        for prefix, count in sorted(koza_transform.state["missing_phenotype_ids"].items()):
            logger.warning(f"  {prefix}: {count} IDs")
    cleanup_entity_lookup_db()
