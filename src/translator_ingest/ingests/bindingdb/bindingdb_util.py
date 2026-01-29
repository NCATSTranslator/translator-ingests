"""
Utility methods for BindingDB input and parsing.
Adapted from sample code prototyped by CLAUDE.ai
"""
from pathlib import Path
from zipfile import ZipFile
import polars as pl
import koza

from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)

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

CURATION_DATA_SOURCE_TO_INFORES_MAPPING = {
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

_WEB_MAPPINGS: dict[str, str] = {
    " ": "+",
    ",": "%2C",
    "{": "%7B",
    "}": "%7D",
    "[": "%5B",
    "]": "%5D",
    "|": "%7C"
}
def web_string(s: str) -> str:
    """
    :param s: The input string to be encoded
    :return: The input string encoded with web-encoded characters
    """
    # Web string sanitization
    for a,b in _WEB_MAPPINGS.items():
        s = s.replace(a, b)
    return s


def extract_bindingdb_columns_polars(
    data_archive_path: Path,
    columns: tuple[str,...],
    target_taxa: tuple[str,...],
) -> pl.DataFrame:
    """
     Extract only specified columns from the BindingDB TSV file using polars.

    This is the FASTEST approach:
    - Only parses the specified subset of columns instead of the full 640 (~80x less data)
    - Filters data by desired taxa
    - Lazy evaluation optimizes the query

    :param data_archive_path: Path to BindingDB TSV archive.
    :param columns: Target BindingDB columns to extract.
    :param target_taxa: Target species to be included in extracted BindingDB data.
    :return: A Polars DataFrame containing BindingDB data rows with only specified columns.
    """
    with ZipFile(data_archive_path) as z:
        with z.open("BindingDB_All.tsv") as datafile:
            df = (
                pl.scan_csv(
                    datafile,
                    separator="\t",
                    has_header=True,  # header_mode: 0 means that the first row is the header
                    schema_overrides={
                        MONOMER_ID: pl.Utf8,
                        PUBCHEM_CID: pl.Utf8,
                        TARGET_NAME: pl.Utf8,
                        UNIPROT_ID: pl.Utf8,
                        CURATION_DATASOURCE: pl.Utf8,
                        ARTICLE_DOI: pl.Utf8,
                        PMID: pl.Utf8,
                        PATENT_NUMBER: pl.Utf8,
                    },
                    # not ideal to skip problematic BindingDB data rows, but if
                    # most of the other data can be read, we still make progress
                    ignore_errors=True
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

    logger.info(f"Loaded {len(df)} rows with {len(df.columns)} columns")
    logger.info(df.columns)

    return df

MISSING_PUBS = "rows_missing_publications"

def process_publications(
        koza_transform: koza.KozaTransform,
        df: pl.DataFrame
)-> pl.DataFrame:
    """
    Capture and process publications for BindingDb records.
    :param koza_transform: Ingest context
    :param df: Polars data frame whose entries contain BindingDb records
    :return: Polars data frame with publications processed into a PUBLICATIONS column
    """
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
    if rows_missing_pubs != 0:
        koza_transform.transform_metadata[MISSING_PUBS] = rows_missing_pubs

    # Filter out rows without publications
    df = df.filter(pl.col(PUBLICATION).is_not_null())

    return df
