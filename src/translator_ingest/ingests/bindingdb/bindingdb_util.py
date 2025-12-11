"""
Utility methods for BindingDB input and parsing.
Adapted from sample code prototyped by CLAUDE.ai
"""
import polars as pl
import koza

#
# Core BindingDb Record Field Name Keys - currently ignored fields commented out
#
REACTANT_SET_ID = "BindingDB Reactant_set_id"
LIGAND_SMILES = "Ligand SMILES"
MONOMER_ID = "BindingDB MonomerID"
LIGAND_NAME = "BindingDB Ligand Name"
TARGET_NAME = "Target Name"
SOURCE_ORGANISM = "Target Source Organism According to Curator or DataSource"
# KI = "Ki (nM)"
# IC50 = "IC50 (nM)"
# KD = "Kd (nM)"
# EC50 = "EC50 (nM)"
# KON = "kon (M-1-s-1)"
# KOFF = "koff (s-1)"
# "pH" = "7.4",
# "Temp (C)" = "25.00",
CURATION_DATASOURCE = "Curation/DataSource"
ARTICLE_DOI = "Article DOI"
PMID = "PMID"
PATENT_NUMBER = "Patent Number"
PUBCHEM_CID = "PubChem CID"
UNIPROT_ID = "UniProt (SwissProt) Primary ID of Target Chain 1"

PUBLICATION = "publication"
SUPPORTING_DATA_ID = "supporting_data_id"

BINDINGDB_COLUMNS = (
    REACTANT_SET_ID,
    MONOMER_ID,
    PUBCHEM_CID,
    TARGET_NAME,
    SOURCE_ORGANISM,
    UNIPROT_ID,
    CURATION_DATASOURCE,
    ARTICLE_DOI,
    PMID,
    PATENT_NUMBER
)

CURATION_DATA_SOURCE_TO_INFORES_MAPPING = {
    "CSAR": "infores:community-sar",
    "ChEMBL": "infores:chembl",
    "D3R": "infores:drug-design",
    "PDSP Ki": "infores:ki-database",
    "PubChem": "infores:pubchem",
    "Taylor Research Group, UCSD": "infores:taylor-research-group-ucsd",
    "US Patent": "infores:uspto-patent"
}

SOURCE_ORGANISM_TO_TAXON_ID_MAPPING = {
    "Homo sapiens": "9606",
    "Mus musculus": "10090",
    "Rattus norvegicus": "10116",
    "Bos taurus": "9913",   # cattle
    "Sus scrofa": "9823",     # swine
    "Xenopus laevis": "8355",   # Xenopus laevis (African clawed frog)
    "Xenopus tropicalis": "8364",   # Xenopus tropicalis - tropical clawed frog
    "Danio rerio": "7955",
    "Drosophila melanogaster": "7227",
    "Caenorhabditis elegans": "6239",
    "Schizosaccharomyces pombe": "4896",
    "Saccharomyces cerevisiae": "4932"
}


# We don't need these yet...
# BASE_LINK_TO_MONOMER: str = "http://www.bindingdb.org/bind/chemsearch/marvin/MolStructure.jsp?monomerid={monomerid}"
# BASE_LINK_TO_TARGET: str = ("http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
#                             "?energyterm=kJ/mole"
#                             "&tag=com"
#                             "&complexid=56"
#                             "&target={target}"
#                             "&column=ki&startPg=0&Increment=50&submit=Search")

# ...but would like to use this to publish the source_record_urls for the
#    BindindDb primary_knowledge_source RetrievalSource provenance metadata.
LINK_TO_LIGAND_TARGET_PAIR: str = (
    "http://www.bindingdb.org/rwd/jsp/dbsearch/PrimarySearch_ki.jsp"
    "?energyterm=kJ/mole&tag=r21&monomerid={monomerid}"
    "&enzyme={enzyme}"
    "&column=ki&startPg=0&Increment=50&submit=Search"
)


BINDINGDB_INPUT_FILE = "BindingDB_All_tsv.zip"


def get_bindingdb_input_file():
    return BINDINGDB_INPUT_FILE


def set_bindingdb_input_file(filename: str):
    global BINDINGDB_INPUT_FILE
    BINDINGDB_INPUT_FILE = filename


def extract_bindingdb_columns_polars(
    file_path: str,
    columns: list[str] = BINDINGDB_COLUMNS,
    target_taxa: list[str] = SOURCE_ORGANISM_TO_TAXON_ID_MAPPING.keys(),
) -> pl.DataFrame:
    """
    Extract only specified columns from the BindingDB TSV file using polars.

    This is the FASTEST approach:
    - Only parses 8 columns instead of 640 (~80x less data)
    - Lazy evaluation optimizes the query
    - Native zip file support

    >>> df = extract_bindingdb_columns_polars("BindingDB_All_current_tsv.zip")
    >>> print(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    >>> print(df.columns)
    """
    # Polars can read from zip files directly
    df = (
        pl.scan_csv(
            file_path,
            separator="\t",
            has_header=True,  # header_mode: 0 means that the first row is the header
            dtypes={
                "BindingDB MonomerID": pl.Utf8,
                "PubChem CID": pl.Utf8,
                "Target Name": pl.Utf8,
                "UniProt (SwissProt) Primary ID of Target Chain 1": pl.Utf8,
                "Curation/DataSource": pl.Utf8,
                "Article DOI": pl.Utf8,
                "PMID": pl.Utf8,
                "Patent Number": pl.Utf8,
            }
        )
        # CRITICAL: Only select the required columns - massive performance gain
        .select(columns)
        # Execute the optimized query
        .collect()
    )

    # Filtering to only human targets
    if SOURCE_ORGANISM in columns:
        df = df.filter(
            pl.col(SOURCE_ORGANISM).is_in(target_taxa)
        )

    return df

def process_publications(
        koza_transform: koza.KozaTransform,
        df: pl.DataFrame
)-> pl.DataFrame:
    # Add the publication column (same logic as the current implementation)
    df = df.with_columns([
        pl.when(pl.col(PMID).is_not_null())
        .then(pl.concat_str([pl.lit("PMID:"), pl.col(PMID)]))
        .when(pl.col(PATENT_NUMBER).is_not_null())
        .then(
            pl.concat_str([
                pl.lit("uspto-patent:"),
                pl.col(PATENT_NUMBER).str.replace("US", "")
            ])
        )
        .when(pl.col(ARTICLE_DOI).is_not_null())
        .then(pl.concat_str([pl.lit("doi:"), pl.col(ARTICLE_DOI)]))
        .otherwise(None)
        .alias(PUBLICATION)
    ])

    # Count rows without publications
    rows_missing_pubs = df.filter(pl.col(PUBLICATION).is_null()).height
    koza_transform.transform_metadata["ingest_by_record"]["rows_missing_publications"] = rows_missing_pubs

    # Filter out rows without publications
    df = df.filter(pl.col(PUBLICATION).is_not_null())

    return df
