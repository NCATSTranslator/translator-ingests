"""
Ingest of Reference Genome Orthologs from Panther
"""

# Imports
import uuid
from koza.cli_utils import get_koza_app
from biolink_model.datamodel.pydanticmodel_v2 import GeneToGeneHomologyAssociation, KnowledgeLevelEnum, AgentTypeEnum

# Custom pantherdb specific function, and constants respectively
from panther_orthologs_utils import (make_ncbi_taxon_gene_map, parse_gene_info)
from panther_orthologs_utils import (panther_taxon_map, relevant_ncbi_cols, relevant_ncbi_taxons, db_to_curie_map)


# Initiate koza app prior to reading in gene map
koza_app = get_koza_app("panther_genome_orthologs")

# Custom function to make a mapping of taxon-specific gene identifiers --> ncbi gene identifiers
ncbi_map_file = "./data/gene_info.gz"
tx_gmap = make_ncbi_taxon_gene_map(gene_info_file=ncbi_map_file, 
                                   relevant_columns=relevant_ncbi_cols, 
                                   taxon_catalog=relevant_ncbi_taxons)
species_pair_max = {}
species_pair_stats = {}

cc = 0
while (row := koza_app.get_row()) is not None:
    
    # Parse the gene information for both species and format gene id to curie:gene_id 
    # (Gene and Ortholog columns are formatted the same, but for different species/gene info)
    species_a, gene_a = parse_gene_info(row["Gene"], panther_taxon_map, db_to_curie_map, tx_gmap)
    species_b, gene_b = parse_gene_info(row["Ortholog"], panther_taxon_map, db_to_curie_map, tx_gmap)
    
    # Only consume species we are interested in (i.e., those that are in our NCBI Taxon catalog)
    if (not species_a) or (not species_b):
        continue

    # Format our species names to NCBI Taxon IDs
    ncbitaxon_a = "NCBITaxon:{}".format(panther_taxon_map[species_a])
    ncbitaxon_b = "NCBITaxon:{}".format(panther_taxon_map[species_b])

    # Our ortholog identifier (panther protein family name), and predicate
    panther_ortholog_id = row["Panther Ortholog ID"]
    predicate = "biolink:orthologous_to"

    # Generate our association object
    association = GeneToGeneHomologyAssociation(id="uuid:{}".format(str(uuid.uuid1())),
                                                subject=gene_a,
                                                object=gene_b,
                                                predicate=predicate,
                                                has_evidence=["PANTHER.FAMILY:{}".format(panther_ortholog_id)],
                                                aggregator_knowledge_source=["infores:monarchinitiative"],
                                                primary_knowledge_source="infores:panther",
                                                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                                                agent_type=AgentTypeEnum.not_provided)
    
    # Write the captured Association out
    koza_app.write(association)