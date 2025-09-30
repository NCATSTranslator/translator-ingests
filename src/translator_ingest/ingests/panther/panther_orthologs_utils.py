"""
Utility functions for Panther Orthology data processing
"""
# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# import gzip
# from collections import Counter
# NCBI_MAP_FILE_PATH = "./data/panther/gene_info.gz"

# These names should match pantherdb shorthand names for each species
# Example... https://www.pantherdb.org/genomes/genome.jsp?taxonId=9606 --> Short Name: HUMAN
panther_taxon_map = {
    "HUMAN": "9606",
    "MOUSE": "10090",
    "RAT": "10116",

    # The September 2025 implementation of the
    # Translator Phase 3 Panther data ingestion
    # only contains data for the above 3 species.
    #
    # "CANLF": "9615",   # Canis lupus familiaris - domestic dog
    # "BOVIN": "9913",   # Bos taurus - cow
    # "PIG": "9823",     # Sus scrofa - pig
    # "CHICK": "9031",
    # "XENTR": "8364",   # Xenopus tropicalis - tropical clawed frog
    # "DANRE": "7955",
    # "DROME": "7227",
    # "CAEEL": "6239",
    # "DICDI": "44689",
    # "EMENI": "227321",  # Emericella nidulans (strain FGSC A4 etc.) (Aspergillus nidulans)
    # "SCHPO": "4896",
    # "YEAST": "4932"
    # Additional species for future here...
    # "FELCA": "9685",  # Felis catus - domestic cat
}


# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# # Full list of NCBI columns is:
# relevant_ncbi_cols = [
#     '#tax_id',
#     'GeneID',
#     'Symbol',
#     'LocusTag',
#     'Synonyms',
#     'dbXrefs',
#     'Symbol_from_nomenclature_authority',
#     'Full_name_from_nomenclature_authority',
#     'Other_designations'
# ]

# Entries with Gene/Orthology identifier namespaces that need modifying
# to match our CURIEs.  Keys are the pantherdb namespace, and values
# are the CURIE namespace (Note: many key/value pairs are the same
# for simplicity in downstream processing)
db_to_curie_map = {
    "HGNC":"HGNC",
    "MGI":"MGI",
    "RGD":"RGD",
    "Ensembl": "ENSEMBL",

    # These identifier namespaces for non-target species are ignored for now.
    # We assume that they don't slip by the gauntlet of the taxonomic filter in the code.
    # "SGD":"SGD",
    # "ZFIN":"ZFIN",
    # "dictyBase":"dictyBase",
    # "PomBase":"PomBase",
    # "Xenbase":"Xenbase",
    # "FlyBase":"FB",
    # "WormBase":"WB",

    ## For future reference... Genes with this prefix (EnsembleGenome)
    # appear to be in the symbol name space...
    ## Rather than ENSEMBL gene name space (i.e., ENS0000123..))
    ## So we simply use the gene name as is and attempt
    # to map back to ncbi gene id, and uniprot as fallback
    ##"EnsemblGenome": "ENSEMBL"
}


# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# # Used in make_ncbi_taxon_gene_map function to filter for only species we are interested in
# relevant_ncbi_taxons = {v:'' for v in panther_taxon_map.values()}

# RMB: 29-Sept-2025: NCBI gene lookup is not used for now
# def make_ncbi_taxon_gene_map(gene_info_file: str, relevant_columns: list, taxon_catalog: dict):
#
#     # Ensure relevant columns has #tx_id as the first entry
#     if relevant_columns[0] != "#tax_id":
#         raise RuntimeError("- '#tax_id' must be first element present in relevant_columns arg... Exiting")
#
#     # We don't want entries equivalent to this from this file
#     exclude_terms = {"-":''}
#
#     # Don't want to use pandas here (for memory and other reasons relating to speed)
#     taxa_gene_map = {tx_id:{} for tx_id in taxon_catalog}   # Many-->1 mapping dictionary
#     taxa_remove_map = {tx_id:Counter() for tx_id in taxon_catalog} # Removes unreliable mapping keys from taxa_gene_map
#
#     with gzip.open(gene_info_file, 'rt') as infile:
#
#         # Read the header line into memory to index relevant column fields
#         hinfo = {hfield:i for i,hfield in enumerate(infile.readline().strip('\r').strip('\n').split('\t'))}
#
#         ccc = 0
#         # Now loop through each line and create a map back to taxon / NCBIGene:xyz ...
#         for line in infile:
#             cols = line.strip('\r').strip('\n').split('\t')
#             rel_data = [str(cols[hinfo[r]]) for r in relevant_columns]
#             tx_id = rel_data[0]
#
#             # Only consume species we are interested in
#             if tx_id not in taxon_catalog:
#                 continue
#
#             ncbi_gene_id = cols[hinfo["GeneID"]]
#
#             # Find reliable mapping keys to this NCBI gene id
#             # We take the set() of relevant mapping keys here... that if the same_id is reported on the same line
#             # This removes the possibility of removing an id that is reported twice on the same line
#             removed = []
#             for map_key in set(rel_data[1:]):
#
#                 # Some columns like dbxref contain this character,
#                 # which separates common names from each other. So we loop through them
#                 # (a minority, not a majority, have this in them)
#                 mk_cols = map_key.split("|")
#                 for key_to_ncbi in mk_cols:
#
#                     # Deal with entries like MGI:MGI:95886, where we want to remove one of the MGI: prefix
#                     key_split = key_to_ncbi.split(":")
#                     if len(key_split) >= 2:
#                         if key_split[0] == key_split[1]:
#                             key_to_ncbi = "{}:{}".format(key_split[0], key_split[-1])
#
#                     if key_to_ncbi not in taxa_gene_map[tx_id]:
#                         taxa_gene_map[tx_id].update({key_to_ncbi:ncbi_gene_id})
#                     else:
#                         taxa_remove_map[tx_id][key_to_ncbi] += 1
#
#
#     # Remove unreliable mapping keys to this NCBI gene id
#     for tx_id in taxa_remove_map:
#         for remove_key, rcount in taxa_remove_map[tx_id].items():
#             ##print("- Taxon {} | Removing {} | Count {}".format(tx_id, remove_key, rcount))
#             del taxa_gene_map[tx_id][remove_key]
#
#     # Return cleaned-up mapping to a ncbi gene id
#     # that can be normalized later down the road
#     return taxa_gene_map


def parse_gene_info(
        gene_info,
        taxon_map,
        curie_map,
        # fallback_map - we don't use this NCBI Gene lookup at the moment, but we keep it here for now for reference'
):
    """
    This function takes a panther gene information string and returns the species name and gene identifier in a
    standardized format. This is done by converting to CURIEs based on a predefined mapping in a table and using
    uniprotkb id as a fallback. We also remove ensemble version/transcript ids from the tail end of ensembl ids,
    and we also filter out species that are not in our taxon map. Below are examples of the transformation process

    HUMAN|HGNC=16435|UniProtKB=Q8TCT9 --> HUMAN, HGNC:16435
    SCHPO|PomBase=SPBP23A10.09|UniProtKB=Q9P7X6 --> SCHPO, PomBase:SPBP23A10.09
    CANLF|Ensembl=ENSCAFG00845009646.1|UniProtKB=A0A8I3N1X7 --> CANLF, Ensembl:ENSCAFG00845009646

    :param gene_info: This is a string of the format species|gene|uniprotkb_id
    :param taxon_map: This is a dictionary of the Panther standard species name to NCBI taxon id
    :param curie_map: This is a dictionary of the gene  CURIE prefix mappings
    :return:
    """
    
    cols = gene_info.split("|") # species|gene|uniprotkb_id
    species = cols[0]

    # Exit condition (saves compute when there are many rows to process...)
    if species not in taxon_map:
        return None, None
    
    # Now assign our gene to its "rightful" prefix...
    # If no reasonable prefix exists (HGNC, MGI, etc.),
    # then we use the UniprotKB ID prefix as a fallback.
    # Connections can be rescued through
    # a normalization process, via UniProtKB protein ids
    
    # Our preferred order is Organism specific (HGNC, PomBase, ZFIN)
    # RMB: 29-Sept-2025: taxon_id only used for NCBI gene lookup, which is itself unused for now
    # taxon_id = taxon_map[species]
    gene_split = cols[1].split("=")

    # TODO: not sure what's the point of these counters here... not reported anywhere?
    # matched = 0
    # fback = 0
    # unikb = 0
    
    # Check if gene id can be mapped directly to kg build preferred gene ids
    if gene_split[0] in curie_map:
        # We use -1 here to avoid things like MGI=MGI=95886
        gene = "{}:{}".format(curie_map[gene_split[0]], gene_split[-1])
        # matched = 1

    # TODO: RMB 29-Sept-2025: This is commented out for now:
    #       The NCBIGene resolution is skipped to simply
    #       and speed up the ingestion process for now.
    # # If the gene identifier is already in the 'curie_map'
    # # then we use that as the gene identifier.
    # elif gene_split[1] in curie_map:
    #     gene = "{}:{}".format(curie_map[gene_split[1]], gene_split[-1])
    # # Otherwise, the gene identifiers for the target species
    # # are not the ones in the 'curie_map' so we
    # # fall back onto NCBI Gene map, if possible.
    # elif gene_split[1] in fallback_map[taxon_id]:
    #     g_id = fallback_map[taxon_id][gene_split[1]]
    #     gene = "NCBIGene:{}".format(g_id)
    #     # fback = 1
    
    # Use uniprotkb id as a last resort and format e.g. UniProtKB=Q8TCT9 => "UniProtKB:Q8TCT9"
    else:
        gene = "{}".format(cols[-1].replace("=", ":"))
        # unikb += 1
        
    # Lastly we need to strip version numbers off from ENSEMBL IDs,
    # (e.g. ENSG00000123456.1 => ENSG00000123456)
    if gene.startswith("ENSEMBL:") and (":ENS" in gene):
        gene = gene.split(".")[0]
    
    return species, gene,
