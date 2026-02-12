"""
Utility functions for Panther Orthology data processing
"""
import tarfile
from pathlib import Path

import polars as pl

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


# Column name constants for the raw TSV (headerless, assigned by polars)
GENE_COL = "Gene"
ORTHOLOG_COL = "Ortholog"
TYPE_OF_ORTHOLOG_COL = "Type of ortholog"
COMMON_ANCESTOR_COL = "Common ancestor for the orthologs"
PANTHER_ORTHOLOG_ID_COL = "Panther Ortholog ID"

# Derived column names produced by extract_panther_data_polars
GENE_A_ID_COL = "gene_a_id"
GENE_B_ID_COL = "gene_b_id"
NCBITAXON_A_COL = "ncbitaxon_a"
NCBITAXON_B_COL = "ncbitaxon_b"
GENE_FAMILY_ID_COL = "gene_family_id"

# Column names for the raw TSV, in order
RAW_COLUMNS = [GENE_COL, ORTHOLOG_COL, TYPE_OF_ORTHOLOG_COL, COMMON_ANCESTOR_COL, PANTHER_ORTHOLOG_ID_COL]

# Target species for filtering
TARGET_SPECIES = set(panther_taxon_map.keys())


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


def _resolve_gene_curie(col_name: str) -> pl.Expr:
    """
    Build a polars expression that replicates parse_gene_info CURIE resolution logic
    for a given column (Gene or Ortholog).

    The input column value is formatted as: ``species|DB=id|UniProtKB=uniprotid``

    The logic:
    1. Split on ``|`` to get ``[species, db_id, uniprot]`` parts
    2. Extract the DB prefix from the second part (before ``=``)
    3. Use ``when/then`` chains for curie_map lookup (HGNC, MGI, RGD, Ensembl)
    4. Falls back to UniProtKB (replace ``=`` with ``:``)
    5. Strips ENSEMBL version numbers (``.N`` suffix)

    :param col_name: Name of the column to resolve (e.g. "Gene" or "Ortholog")
    :return: A polars expression producing the resolved gene CURIE string
    """
    parts = pl.col(col_name).str.split("|")

    # Second part is like "HGNC=16435" or "MGI=MGI=95886" or "Gene=P12LL_HUMAN"
    db_id_part = parts.list.get(1)
    db_prefix = db_id_part.str.split("=").list.first()
    # Use last element after "=" to handle cases like "MGI=MGI=95886" → "95886"
    db_id_value = db_id_part.str.split("=").list.last()

    # Third part is UniProtKB fallback like "UniProtKB=Q8TCT9"
    uniprot_part = parts.list.last()
    uniprot_curie = uniprot_part.str.replace("=", ":")

    # Build the CURIE using when/then chains for curie_map entries
    gene_expr = (
        pl.when(db_prefix == "HGNC")
        .then(pl.lit("HGNC:") + db_id_value)
        .when(db_prefix == "MGI")
        .then(pl.lit("MGI:") + db_id_value)
        .when(db_prefix == "RGD")
        .then(pl.lit("RGD:") + db_id_value)
        .when(db_prefix == "Ensembl")
        .then(pl.lit("ENSEMBL:") + db_id_value)
        .otherwise(uniprot_curie)
    )

    # Strip ENSEMBL version numbers: "ENSEMBL:ENSG00000275949.5" → "ENSEMBL:ENSG00000275949"
    # Only strip when it starts with "ENSEMBL:" and contains ":ENS" (real ENSEMBL IDs)
    gene_expr = (
        pl.when(gene_expr.str.starts_with("ENSEMBL:") & gene_expr.str.contains(":ENS"))
        .then(gene_expr.str.split(".").list.first())
        .otherwise(gene_expr)
    )

    return gene_expr


def extract_panther_data_polars(
        data_archive_path: Path,
        target_species: set[str] | None = None
) -> pl.DataFrame:
    """
    Read the Panther RefGenomeOrthologs tar.gz archive, filter by target species,
    and resolve gene CURIEs using polars vectorized operations.

    :param data_archive_path: Path to the RefGenomeOrthologs.tar.gz archive
    :param target_species: Set of species short names to include (default: TARGET_SPECIES)
    :return: A polars DataFrame with columns: gene_a_id, gene_b_id, ncbitaxon_a, ncbitaxon_b, gene_family_id
    """
    if target_species is None:
        target_species = TARGET_SPECIES

    # Read the tar.gz archive — it contains a single TSV file
    with tarfile.open(data_archive_path, "r:gz") as tar:
        members = tar.getmembers()
        # Find the TSV data file (skip directories, READMEs, etc.)
        data_member = next(m for m in members if m.isfile() and not m.name.upper().endswith("README"))
        with tar.extractfile(data_member) as f:
            df = pl.read_csv(
                f.read(),
                separator="\t",
                has_header=False,
                new_columns=RAW_COLUMNS,
                schema_overrides={col: pl.Utf8 for col in RAW_COLUMNS},
            )

    # Extract species from Gene and Ortholog columns (first part before "|")
    species_a = pl.col(GENE_COL).str.split("|").list.first()
    species_b = pl.col(ORTHOLOG_COL).str.split("|").list.first()

    # Filter: both Gene and Ortholog species must be in target_species
    df = df.filter(
        species_a.is_in(target_species) & species_b.is_in(target_species)
    )

    # Filter out rows with empty/null Gene or Ortholog
    df = df.filter(
        pl.col(GENE_COL).is_not_null()
        & (pl.col(GENE_COL) != "")
        & pl.col(ORTHOLOG_COL).is_not_null()
        & (pl.col(ORTHOLOG_COL) != "")
    )

    # Build taxon lookup DataFrame
    taxon_df = pl.DataFrame({
        "species": list(panther_taxon_map.keys()),
        "taxon_id": [f"NCBITaxon:{tid}" for tid in panther_taxon_map.values()]
    })

    # Resolve gene CURIEs and build derived columns
    df = df.with_columns([
        _resolve_gene_curie(GENE_COL).alias(GENE_A_ID_COL),
        _resolve_gene_curie(ORTHOLOG_COL).alias(GENE_B_ID_COL),
        species_a.alias("_species_a"),
        species_b.alias("_species_b"),
        (pl.lit("PANTHER.FAMILY:") + pl.col(PANTHER_ORTHOLOG_ID_COL)).alias(GENE_FAMILY_ID_COL),
    ])

    # Join taxon IDs for species A
    df = df.join(
        taxon_df.rename({"species": "_species_a", "taxon_id": NCBITAXON_A_COL}),
        on="_species_a",
        how="left"
    )
    # Join taxon IDs for species B
    df = df.join(
        taxon_df.rename({"species": "_species_b", "taxon_id": NCBITAXON_B_COL}),
        on="_species_b",
        how="left"
    )

    # Select only the derived columns needed downstream
    df = df.select([GENE_A_ID_COL, GENE_B_ID_COL, NCBITAXON_A_COL, NCBITAXON_B_COL, GENE_FAMILY_ID_COL])

    return df
