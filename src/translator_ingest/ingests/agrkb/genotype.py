import uuid  # For generating UUIDs for associations

from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Genotype,
    GenotypeToGeneAssociation,
    GenotypeToVariantAssociation,
    KnowledgeLevelEnum,
)
from koza.cli_utils import get_koza_app

koza_app = get_koza_app("alliance_genotype")
allele_to_gene_lookup = koza_app.get_map("allele_to_gene")

source_map = {
    "FB": "infores:flybase",
    "MGI": "infores:mgi",
    "RGD": "infores:rgd",
    "HGNC": "infores:rgd",  # Alliance contains RGD curation of human genes
    "SGD": "infores:sgd",
    "WB": "infores:wormbase",
    "Xenbase": "infores:xenbase",
    "ZFIN": "infores:zfin",
}

taxon_label_map = {
    "NCBITaxon:7955": "Danio rerio",
    "NCBITaxon:10090": "Mus musculus",
    "NCBITaxon:10116": "Rattus norvegicus",
}

while (row := koza_app.get_row()) is not None:
    # Code to transform each row of data
    # For more information, see https://koza.monarchinitiative.org/Ingests/transform
    #    print(row)
    #    print(row["crossReference"])
    #    print([xref.id for xref in row["crossReference"]])
    genotype = Genotype(
        id=row["primaryID"],
        type=[row["subtype"]] if "subtype" in row else None,
        name=row["name"],
        in_taxon=[row["taxonId"]],
        in_taxon_label=taxon_label_map[row["taxonId"]],
    )
    entities = [genotype]

    for allele in row["affectedGenomicModelComponents"] if "affectedGenomicModelComponents" in row else []:
        genotype_to_variant_association = GenotypeToVariantAssociation(
            id=str(uuid.uuid4()),
            subject=genotype.id,
            predicate="biolink:has_sequence_variant",
            object=allele["alleleID"],
            qualifier=allele["zygosity"] if "zygosity" in allele else None,
            primary_knowledge_source=source_map[row["primaryID"].split(':')[0]],
            aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
        )
        entities.append(genotype_to_variant_association)

        gene = allele_to_gene_lookup.get(allele["alleleID"], {}).get('AlleleAssociatedGeneId', None)
        print("gene: ", gene)
        if gene:
            genotype_to_gene_association = GenotypeToGeneAssociation(
                id=str(uuid.uuid4()),
                subject=genotype.id,
                # More specific predicate may come eventually, keeping it vague for now
                predicate="biolink:related_to",
                object=gene,
                primary_knowledge_source=source_map[row["primaryID"].split(':')[0]],
                aggregator_knowledge_source=["infores:monarchinitiative", "infores:agrkb"],
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
            )
            entities.append(genotype_to_gene_association)

    koza_app.write(*entities)
