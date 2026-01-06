"""
Alliance of Genome Resources ingest for mouse and rat data.

This ingest processes gene-to-phenotype and gene-to-expression associations
from the Alliance of Genome Resources, for mouse (NCBITaxon:10090) and
rat (NCBITaxon:10116) only.

All nodes are minimal placeholders (id only) intended to be merged
with richer data from other sources.
"""

from typing import Any

import duckdb
import koza
from koza.model.graphs import KnowledgeGraph
from loguru import logger
from pathlib import Path

from bmt.pydantic import entity_id, build_association_knowledge_sources
from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    GeneToPhenotypicFeatureAssociation,
    GeneToExpressionSiteAssociation,
    PhenotypicFeature,
    AnatomicalEntity,
    CellularComponent,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)
# Aggregator infores
INFORES_AGRKB = "infores:agrkb"

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
    Build in-memory DuckDB table of gene IDs for filtering phenotype records.

    Creates a table with gene IDs by reading directly from the compressed
    BGI JSON files. Used to determine which phenotype records are for genes
    (vs genotypes or variants).
    """
    global _entity_lookup_conn

    if _entity_lookup_conn is not None:
        logger.debug("Entity lookup DB already built, skipping")
        return  # Already built

    logger.info("Building gene ID lookup DB...")

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
                AND unnest.basicGeneticEntity.taxonId IN ('NCBITaxon:10090', 'NCBITaxon:10116')
        """)

        # Create index for fast lookup
        conn.execute("CREATE INDEX idx_entity_id ON entities(id)")

        gene_count = conn.execute('SELECT COUNT(*) FROM entities').fetchone()[0]
        logger.info(f"Built gene lookup table with {gene_count:,} genes")

        _entity_lookup_conn = conn

    except Exception as e:
        logger.error(f"Error building gene lookup DB: {e}")
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


@koza.on_data_begin(tag="phenotype")
def initialize_entity_lookup_phenotype(koza_transform):
    """Initialize entity lookup DB before processing phenotype data."""
    build_entity_lookup_db(data_dir=koza_transform.input_files_dir)


@koza.transform_record(tag="phenotype")
def transform_phenotype(
    koza_transform: koza.KozaTransform, row: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform gene-to-phenotype associations.

    Only processes associations where objectId is a gene (skips genotypes/variants).
    Creates placeholder Gene and PhenotypicFeature nodes.
    """
    # Validate phenotype terms exist
    if len(row.get("phenotypeTermIdentifiers", [])) == 0:
        logger.warning(f"Phenotype record has 0 phenotype terms: {str(row)}")
        return None

    gene_id = row["objectId"]

    # Look up whether this objectId is a gene
    entity_category = lookup_entity_category(gene_id)

    # Only process gene-phenotype associations (skip genotypes/variants)
    if entity_category != 'biolink:Gene':
        return None

    nodes = []
    edges = []

    # Create placeholder gene node
    gene = Gene(id=gene_id)
    nodes.append(gene)

    # Determine primary knowledge source from gene ID prefix
    primary_source = SOURCE_MAP[gene_id.split(':')[0]]

    # Process all phenotype terms
    for pheno_term in row["phenotypeTermIdentifiers"]:
        phenotypic_feature_id = pheno_term["termId"]
        # Remove extra WB: prefix if necessary
        phenotypic_feature_id = phenotypic_feature_id.replace("WB:WBPhenotype:", "WBPhenotype:")

        # Create placeholder phenotypic feature node
        phenotype = PhenotypicFeature(id=phenotypic_feature_id)
        nodes.append(phenotype)

        association = GeneToPhenotypicFeatureAssociation(
            id=entity_id(),
            subject=gene_id,
            predicate="biolink:has_phenotype",
            object=phenotypic_feature_id,
            publications=[row["evidence"]["publicationId"]],
            sources=build_association_knowledge_sources(
                primary=primary_source,
                aggregating=INFORES_AGRKB,
            ),
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )

        # Add qualifiers if present
        if "conditionRelations" in row.keys() and row["conditionRelations"] is not None:
            qualifiers: list[str] = []
            for conditionRelation in row["conditionRelations"]:
                for condition in conditionRelation["conditions"]:
                    if condition.get("conditionClassId"):
                        qualifiers.append(condition["conditionClassId"])
            if qualifiers:
                association.qualifiers = qualifiers

        edges.append(association)

    return KnowledgeGraph(nodes=nodes, edges=edges)


@koza.transform_record(tag="expression")
def transform_expression(
    koza_transform: koza.KozaTransform, row: dict[str, Any]
) -> KnowledgeGraph | None:
    """
    Transform gene-to-expression site associations.

    Creates placeholder Gene, AnatomicalEntity, and CellularComponent nodes.
    """
    try:
        gene_id = get_data(row, "geneId")

        # Handle Xenbase prefix
        gene_id = gene_id.replace("DRSC:XB:", "Xenbase:")

        db = gene_id.split(":")[0]
        primary_source = SOURCE_MAP[db]

        cellular_component_id = get_data(row, "whereExpressed.cellularComponentTermId")
        anatomical_entity_id = get_data(row, "whereExpressed.anatomicalStructureTermId")
        stage_term_id = get_data(row, "whenExpressed.stageTermId")

        publication_ids = [get_data(row, "evidence.publicationId")]
        xref = get_data(row, "crossReference.id")
        if xref:
            publication_ids.append(xref)

        nodes = []
        edges = []

        # Create placeholder gene node
        gene = Gene(id=gene_id)
        nodes.append(gene)

        # Build knowledge sources once for reuse
        sources = build_association_knowledge_sources(
            primary=primary_source,
            aggregating=INFORES_AGRKB,
        )

        # Get assay qualifier if present
        assay = get_data(row, "assay")

        # Prefer anatomical structure, fall back to cellular component
        if anatomical_entity_id:
            # Create placeholder anatomical entity node
            anatomy = AnatomicalEntity(id=anatomical_entity_id)
            nodes.append(anatomy)

            edges.append(
                GeneToExpressionSiteAssociation(
                    id=entity_id(),
                    subject=gene_id,
                    predicate="biolink:expressed_in",
                    object=anatomical_entity_id,
                    stage_qualifier=stage_term_id,
                    qualifiers=[assay] if assay else None,
                    publications=publication_ids,
                    sources=sources,
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
            )
        elif cellular_component_id:
            # Create placeholder cellular component node
            cell_component = CellularComponent(id=cellular_component_id)
            nodes.append(cell_component)

            edges.append(
                GeneToExpressionSiteAssociation(
                    id=entity_id(),
                    subject=gene_id,
                    predicate="biolink:expressed_in",
                    object=cellular_component_id,
                    stage_qualifier=stage_term_id,
                    qualifiers=[assay] if assay else None,
                    publications=publication_ids,
                    sources=sources,
                    knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                    agent_type=AgentTypeEnum.manual_agent,
                )
            )
        else:
            logger.error(
                f"Gene expression record has no ontology terms specified for expression site: {str(row)}"
            )
            return None

        return KnowledgeGraph(nodes=nodes, edges=edges)

    except Exception as exc:
        logger.error(f"Alliance gene expression ingest parsing exception for data row: {str(row)}\n{str(exc)}")
        return None


@koza.on_data_end(tag="phenotype")
def report_and_cleanup_phenotype(koza_transform):
    """Cleanup after phenotype processing."""
    cleanup_entity_lookup_db()
