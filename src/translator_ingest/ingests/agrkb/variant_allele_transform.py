import uuid  # For generating UUIDs for associations

from biolink_model.datamodel.pydanticmodel_v2 import (SequenceVariant,
                                                      VariantToGeneAssociation,
                                                      KnowledgeLevelEnum,
                                                      AgentTypeEnum)
from koza.cli_utils import get_koza_app
import csv
import sys

# There's some very large chunks of sequence in the ingest file, this lets koza load them
csv.field_size_limit(sys.maxsize)

koza_app = get_koza_app("alliance_allele")

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

while (row := koza_app.get_row()) is not None:
    # Code to transform each row of data
    # For more information, see https://koza.monarchinitiative.org/Ingests/transform

   #  print(row)

    entities = []

    sequence_variant = SequenceVariant(
        id = row["AlleleId"],
        in_taxon = [row["Taxon"]],
        in_taxon_label = row["SpeciesName"],
        name=row["AlleleSymbol"],
    )
    entities.append(sequence_variant)

    if row["AlleleAssociatedGeneId"]:
        variant_to_gene = VariantToGeneAssociation(
            id=str(uuid.uuid4()),
            subject=sequence_variant.id,
            predicate=get_predicate(row["VariantsTypeId"]),
            object=row["AlleleAssociatedGeneId"],
            # TODO: PKS should be the MOD
            primary_knowledge_source="infores:agrkb",
            aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent
        )
        entities.append(variant_to_gene)

    koza_app.write(*entities)
