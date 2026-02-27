## FROM template, modified for this ingest
import koza
from koza.model.graphs import KnowledgeGraph
from typing import Any, Iterable
## build_association_knowledge_sources should be able to handle source_record_urls
from bmt.pydantic import entity_id, build_association_knowledge_sources
## using bmt to get UMLS semantic types for DiseaseOrPheno
from translator_ingest.util.biolink import INFORES_DRUGCENTRAL, get_biolink_model_toolkit
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    DiseaseOrPhenotypicFeature,
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

## ADDED packages for this ingest
import pandas as pd
## psycopg is underlying dependency
from sqlalchemy import create_engine, URL, text
## FYI: get bmt toolkit from util.biolink function, so not importing entire package here
from translator_ingest.ingests.drugcentral.mappings import OMOP_RELATION_MAPPING


## HARD-CODED values and mappings
## connection info for public Postgres database 
DIALECT = "postgresql"
DRIVER = "psycopg"    ## dependency
## from DrugCentral downloads page
USER = "drugman"
PASSWORD = "dosage"
HOST = "unmtid-dbs.net"
PORT = 5433
DBNAME = "drugcentral"
## omop_relationship
## actually using this view, which adds doid column to omop_relationship. 
##   Just in case we later want to use other ID namespaces for DoP objects
OMOP_TABLE = "omop_relationship_doid_view"
OMOP_MAIN_COLUMNS = ["struct_id", "relationship_name", "umls_cui", "cui_semantic_type"]
## DoP objects to filter out (umls_cui values)
DOP_TO_FILTER = [
    "C0085228",   ## Fluvoxamine (drug): doesn't match predicate (contraindicated) or Association range
    "C0022650",   ## Kidney Calculi (aka stones, NodeNorm maps to AnatomicalEntity): doesn't match Association range
]


## CUSTOM FUNCTIONS
def get_server_url(dialect, driver, user, password, host, port: int, dbname):
    """
    Uses SQLAlchemy method to compose server url for SQLAlchemy engine, 
    rather than using hard-coded string formatting. 
    
    Returns: sqlalchemy.engine.url.URL
    """
    return URL.create(
        drivername = f"{dialect}+{driver}",
        username=user,
        password=password,
        host=host,
        port=port,
        database=dbname,
    )


## PIPELINE MAIN FUNCTIONS
def get_latest_version() -> str:
    """
    Returns database's version date in the format "%Y_%m_%d"
    """
    ## setup access to database
    server_url = get_server_url(
        dialect=DIALECT, driver=DRIVER, user=USER, password=PASSWORD, host=HOST, port=PORT, dbname=DBNAME
    )
    engine = create_engine(server_url)

    ## closes connection automatically afterwards
    with engine.connect() as db_conn: 
        result = db_conn.execute(text("SELECT * FROM dbversion"))
        ## date is second element (idx 1) in row. First element is data version number
        version_date = result.fetchone()[1]

    strformat = "%Y_%m_%d"
    return version_date.strftime(strformat)


## omop_relationship parsing
@koza.prepare_data(tag="omop_relationship")
def omop_prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    """
    Access database and load Drug - DiseaseOrPheno (DoP) table into pandas. Then process.
    """
    ## setup access to database
    server_url = get_server_url(
        dialect=DIALECT, driver=DRIVER, user=USER, password=PASSWORD, host=HOST, port=PORT, dbname=DBNAME
    )
    engine = create_engine(server_url)

    ## load table into pandas: only the columns we're currently using
    koza.log(f"Loading {OMOP_TABLE} into pandas...")
    koza.log(f"Only working with these columns: {", ".join(OMOP_MAIN_COLUMNS)}")
    with engine.connect() as db_conn: 
        df2 = pd.read_sql_table(table_name=OMOP_TABLE, con=db_conn, columns=OMOP_MAIN_COLUMNS)
    koza.log(f"Successfully loaded {OMOP_TABLE}: {df2.shape[0]} rows")

    ## FILTERING
    ## drop rows with NA values
    ##   for 2023_11_01 data, dropping NA only matters for umls_cui. But this way seems more foolproof
    ##   (no NA in struct_id or relationship_name, filtering done on cui_semantic_type)
    df2.dropna(ignore_index=True, inplace=True)
    koza.log(f"{df2.shape[0]} rows kept after filtering out missing values (NA).")
    ## drop rows with problematic umls_cui values - see variable above and its comments
    df2 = df2[~ df2["umls_cui"].isin(DOP_TO_FILTER)].copy()
    koza.log(f"{df2.shape[0]} rows kept after filtering out these IDs (not DoP): {", ".join(DOP_TO_FILTER)}")

    ## FILTERING: only keep rows where cui_semantic_type maps to DoP or its descendants
    tk = get_biolink_model_toolkit()
    ## get list of DoP and descendants (default returns self)
    dop_descendants = tk.get_descendants("disease or phenotypic feature")
    ## get UMLS cui_semantic_type values that map to these categories
    dop_semantic_types = list()
    koza.transform_metadata["dop_semantic_mapping"] = dict()    ## just in case, to see filter details
    ## easier to read/make both at same time, rather than list/dict comprehension
    for i in df2["cui_semantic_type"].unique():
        temp = tk.get_element_by_mapping("STY:" + i)
        if temp in dop_descendants:
            dop_semantic_types.append(i)
            koza.transform_metadata["dop_semantic_mapping"][i] = temp
    ## filter - only rows with these cui_semantic_type values
    df2 = df2[df2["cui_semantic_type"].isin(dop_semantic_types)].copy()
    koza.log(f"{df2.shape[0]} rows kept - cui_semantic_type maps to DiseaseOrPheno or its descendants.")

    ## PREPROCESS, REMOVE DUPLICATES
    ## remove whitespace found during EDA. Good to do before removing duplicates, adding prefix
    df2["umls_cui"] = df2["umls_cui"].str.strip()
    ## drop cui_semantic_type - no longer needed, don't want to consider when removing duplicates
    df2.drop(columns="cui_semantic_type", inplace=True)
    ## remove duplicates
    df2.drop_duplicates(inplace=True, ignore_index=True)
    koza.log(f"{df2.shape[0]} rows kept after removing duplicates.")

    ## DONE - output to transform step
    return df2.to_dict(orient="records")


@koza.transform_record(tag="omop_relationship")
def omop_transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## generate DrugCentral url
    drugcentral_url = f"https://drugcentral.org/drugcard/{record["struct_id"]}#druguse"

    chemical = ChemicalEntity(id=f"DRUGCENTRAL:{record["struct_id"]}")
    dop = DiseaseOrPhenotypicFeature(id=f"UMLS:{record["umls_cui"]}")

    ## get mapped predicate/edge attribute for relationship_name 
    data_modeling = OMOP_RELATION_MAPPING[record["relationship_name"]]

    association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate=data_modeling["predicate"],
        object=dop.id,
        sources= build_association_knowledge_sources(
            ## set param as tuple to include source_record_urls list
            primary=(INFORES_DRUGCENTRAL, [drugcentral_url])
        ),
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_validation_of_automated_agent,
        ## return empty dict when OMOP_RELATION_MAPPING doesn't have this key. to avoid error
        **data_modeling.get("edge-attributes", dict())
    )

    return KnowledgeGraph(nodes=[chemical, dop], edges=[association])
