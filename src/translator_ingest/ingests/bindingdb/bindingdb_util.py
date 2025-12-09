"""
Utility methods for BindingDB input and parsing.
Adapted from sample code prototyped by CLAUDE.ai
"""
import polars as pl

#
# Core BindingDb Record Field Name Keys - currently ignored fields commented out
#
MONOMER_ID = "BindingDB MonomerID"
LIGAND_NAME = "BindingDB Ligand Name"
TARGET_NAME = "Target Name"
# "Target Source Organism According to Curator or DataSource" = "Homo sapiens",
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
PATENT_ID = "Patent Number"
PUBMED_CID = "PubChem CID"
UNIPROT_ID = "UniProt (SwissProt) Primary ID of Target Chain 1"

PUBLICATION = "publication"
SUPPORTING_DATA_ID = "supporting_data_id"

BINDINGDB_COLUMNS = (
    MONOMER_ID,
    PUBMED_CID,
    TARGET_NAME,
    UNIPROT_ID,
    CURATION_DATASOURCE,
    ARTICLE_DOI,
    PMID,
    PATENT_ID
)

DATASOURCE_TO_IDENTIFIER_MAPPING = {
    "CSAR": "infores:community-sar",
    "ChEMBL": "infores:chembl",
    "D3R": "infores:drug-design",
    "PDSP Ki": "infores:ki-database",
    "PubChem": "infores:pubchem",
    "Taylor Research Group, UCSD": "infores:taylor-research-group-ucsd",
    "US Patent": "infores:uspto-patent"
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


BINDINGDB_INPUT_FILE = "BindingDB_All_current_tsv.zip"


def get_bindingdb_input_file():
    return BINDINGDB_INPUT_FILE


def set_bindingdb_input_file(filename: str):
    global BINDINGDB_INPUT_FILE
    BINDINGDB_INPUT_FILE = filename


def extract_bindingdb_columns_polars(
    file_path: str,
    columns: list[str] = BINDINGDB_COLUMNS
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
        )
        # CRITICAL: Only select the required columns - massive performance gain
        .select(columns)
        # Execute the optimized query
        .collect()
    )

    return df
