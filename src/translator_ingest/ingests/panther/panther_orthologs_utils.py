"""
Utility functions for Panther Orthology data processing
"""
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
    gene_split = cols[1].split("=")

    # Check if gene id can be mapped directly to kg build preferred gene ids
    if gene_split[0] in curie_map:
        # We use -1 here to avoid things like MGI=MGI=95886
        gene = "{}:{}".format(curie_map[gene_split[0]], gene_split[-1])
        # matched = 1

    # Use the UniProtKB id as a last resort and
    # format e.g., UniProtKB=Q8TCT9 => "UniProtKB:Q8TCT9"
    else:
        gene = "{}".format(cols[-1].replace("=", ":"))
        # unikb += 1
        
    # Lastly we need to strip version numbers off from ENSEMBL IDs,
    # (e.g. ENSG00000123456.1 => ENSG00000123456)
    if gene.startswith("ENSEMBL:") and (":ENS" in gene):
        gene = gene.split(".")[0]
    
    return species, gene,
