import csv
import sys
import uuid

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    KnowledgeLevelEnum,
    SequenceVariant,
    VariantToGeneAssociation,
)
from koza.cli_utils import get_koza_app

# There's some very large chunks of sequence in the ingest file, this lets koza load them
csv.field_size_limit(sys.maxsize)

koza_app = get_koza_app("alliance_allele")

source_map = {
    "MGI": "infores:mgi",
    "RGD": "infores:rgd",
    "ZFIN": "infores:zfin",
}


def get_predicate(variant_type: str) -> str:
    # TODO: Allele type to predicate mapping is not straightforward right now by SO

    # Allele types in file:
    #  SO:0002007  MNV (Multiple Nucleotide Variant)
    #  SO:0000667 Insertion
    #  SO:0000159 Deletion
    # SO:1000008 point mutation
    # SO:1000032 delins

    # variant_of predicates in Biolink Model:
    #  SO:0002054 biolink:is_sequence_variant_of
    #  SO:0001589 biolink:is_frameshift_variant_of, aliases: ['frameshift variant', 'start lost', 'stop lost']
    #  SO:0001629 biolink:is_splice_site_variant_of
    #  ?                    biolink:is_nearby_variant_of
    #  ?                    biolink:is_non_coding_variant_of

    return "biolink:is_sequence_variant_of"


# Extract source from allele ID (e.g., "MGI:123456" -> "MGI")
def get_source_from_id(curie_id):
    if ":" in curie_id:
        return curie_id.split(":")[0]
    return None


while (row := koza_app.get_row()) is not None:
    # Skip rows without allele IDs
    if not row["AlleleId"] or row["AlleleId"] == "-":
        continue

    # Create allele/variant entity
    allele_id = row["AlleleId"]
    source = get_source_from_id(allele_id)

    # Parse synonyms if available
    synonyms = []
    if row["AlleleSynonyms"] and row["AlleleSynonyms"] != "-":
        synonyms = [syn.strip() for syn in row["AlleleSynonyms"].split(",")]

    allele = SequenceVariant(
        id=allele_id,
        name=row["AlleleSymbol"],  # Use symbol as name
        in_taxon=[row["Taxon"]],
        in_taxon_label=row["SpeciesName"],
        synonym=synonyms if synonyms else None,
    )

    # Create entities list for output
    entities = [allele]

    # Add allele to gene associations if gene IDs are available
    if row.get("AlleleAssociatedGeneId") and row["AlleleAssociatedGeneId"] != "-":
        allele_to_gene = VariantToGeneAssociation(
            id=str(uuid.uuid4()),
            subject=allele_id,
            predicate=get_predicate(row["VariantsTypeId"]),
            original_predicate=row["VariantsTypeId"],
            object=row["AlleleAssociatedGeneId"],
            primary_knowledge_source=source_map.get(source, "infores:agrkb"),
            aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        entities.append(allele_to_gene)

    # Write all entities
    koza_app.write(*entities)
