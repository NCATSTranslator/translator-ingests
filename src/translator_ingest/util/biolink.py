"""Biolink Model support for Translator Ingests"""
from functools import lru_cache
from importlib.resources import files

from linkml_runtime.utils.schemaview import SchemaView

from translator_ingest.util.logging_utils import get_logger

logger = get_logger(__name__)

# knowledge source InfoRes curies
INFORES_MONARCHINITIATIVE = "infores:monarchinitiative"
INFORES_OMIM = "infores:omim"
INFORES_ORPHANET = "infores:orphanet"
INFORES_MEDGEN = "infores:medgen"
INFORES_DECIFER = "infores:decifer"
INFORES_HPOA = "infores:hpo-annotations"
INFORES_CTD = "infores:ctd"
INFORES_GOA = "infores:goa"
INFORES_PATHBANK = "infores:pathbank"
INFORES_SEMMEDDB = "infores:semmeddb"
INFORES_BIOLINK = "infores:biolink"
INFORES_TTD = "infores:ttd"
INFORES_BGEE = "infores:bgee"
INFORES_TEXT_MINING_KP = "infores:text-mining-provider-cooccurrence"
INFORES_INTACT = "infores:intact"
INFORES_DGIDB = "infores:dgidb"
## from dgidb ingest, can move above if others use it
INFORES_CGI = "infores:cgi"
INFORES_CIVIC = "infores:civic"
INFORES_CKB_CORE = "infores:ckb-core"
INFORES_COSMIC = "infores:cosmic"
INFORES_CANCERCOMMONS = "infores:cancercommons"
INFORES_CHEMBL = "infores:chembl"
INFORES_CLEARITY_BIOMARKERS = "infores:clearity-biomarkers"
INFORES_CLEARITY_CLINICAL = "infores:clearity-clinical-trial"
INFORES_DTC = "infores:dtc"
INFORES_DOCM = "infores:docm"
INFORES_FDA_PGX = "infores:fda-pgx"
INFORES_GTOPDB = "infores:gtopdb"
INFORES_MYCANCERGENOME = "infores:mycancergenome"
INFORES_MYCANCERGENOME_TRIALS = "infores:mycancergenome-trials"
INFORES_NCIT = "infores:ncit"
INFORES_ONCOKB = "infores:oncokb"
INFORES_PHARMGKB = "infores:pharmgkb"


@lru_cache(maxsize=1)
def get_biolink_schema() -> SchemaView:
    """Get cached Biolink schema, loading it if not already cached."""

    # Try to load from the local Biolink Model package
    # from the locally installed distribution
    try:
        schema_path = files("biolink_model.schema").joinpath("biolink_model.yaml")
        schema_view = SchemaView(str(schema_path))
        logger.debug("Successfully loaded Biolink schema from local file")
        return schema_view
    except Exception as e:
        logger.warning(f"Failed to load local Biolink schema: {e}")
        # Fallback to loading from official URL
        schema_view = SchemaView("https://w3id.org/biolink/biolink-model.yaml")
        logger.debug("Successfully loaded Biolink schema from URL")
        return schema_view

def get_current_biolink_version() -> str:
    return get_biolink_schema().schema.version
