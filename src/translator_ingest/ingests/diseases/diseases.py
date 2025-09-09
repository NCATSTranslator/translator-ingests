import uuid
import koza
from typing import Any, Iterable
from koza.model.graphs import KnowledgeGraph

## ADDED packages for this ingest
from datetime import datetime
import pandas as pd
## ADJUST based on what I am actually using
from biolink_model.datamodel.pydanticmodel_v2 import (
    Protein,
    Disease,
    CorrelatedGeneToDiseaseAssociation,
    GeneToDiseaseAssociation,
    RetrievalSource,
    ResourceRoleEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)


BIOLINK_OCCURS_IN_LIT_WITH = "biolink:occurs_together_in_literature_with"
BIOLINK_ASSOCIATED_WITH = "biolink:associated_with"
INFORES_DISEASES = "infores:diseases"
INFORES_MEDLINEPLUS = "infores:medlineplus"
INFORES_AMYCO = "infores:amyco"

## used to only keep rows with IDs (protein_id column starts with ENSP, disease_id column starts with DOID)
## see @koza.prepare_data, keep_rows_with_IDs for use
ID_start_strings = {
    "protein_id": "ENSP",
    "disease_id": "DOID",
}


## ?? template doesn't have any tag on this
def get_latest_version() -> str:
    """
    Returns the current time with no spaces "%Y-%m-%dT%H:%M:%S.%f%:z"
    Assuming this function is run at almost the same time that the resource files are downloaded
    """

    return datetime.now(datetime.now().astimezone().tzinfo).isoformat()


def remove_duplicates(dataframe: pd.DataFrame):
    """
    Removes completely duplicated rows
    Returns tuple of updated dataframe, count of rows removed
    """
    ## will count all except first occurrence (default)
    temp_duplicate_count = dataframe[dataframe.duplicated()].shape[0]    

    if temp_duplicate_count > 0:
        ## remove duplicated rows in place
        dataframe.drop_duplicates(inplace=True, ignore_index=True)
        ## return count of rows removed
        return dataframe, temp_duplicate_count
    else:
        ## to keep duplicate row count 0
        return dataframe, 0


def keep_rows_with_IDs(dataframe: pd.DataFrame, starting_strings: dict):
    """
    starting_strings: key is column name, value is STARTING substring that means the row has an ID (we want to keep it)

    Returns tuple of updated dataframe, dict nrows_removed
    Dict includes each starting_strings value + "total" rows removed
    """
    nrows_removed = dict()
    nrows_start = dataframe.shape[0]

    ## loop through starting_strings, get counts of rows that don't match criteria (don't have expected IDs, will be removed)
    ## this way, have independent counts for each criterion
    for k,v in starting_strings.items():
        ## ~ means NOT
        temp_count = dataframe[~dataframe[k].str.startswith(v)].shape[0]
        ## include STARTING substring, count
        nrows_removed.update({v: temp_count})

    ## loop through starting_strings, only keep rows that match criteria
    for k,v in starting_strings.items():
        dataframe = dataframe[dataframe[k].str.startswith(v)]
    
    ## calculate total rows removed, save
    nrows_removed.update({"total": nrows_start - dataframe.shape[0]})
    
    return dataframe, nrows_removed


## ?? workflow for textmining vs knowledge are the same. But I'm not sure if I can set/mutate the koza.state variables in a separate, non-tagged function. So having these workflows written twice right now.  
@koza.prepare_data(tag="textmining")
def textmining_prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    ## remove rows we don't want to process, using pandas
    ## set up counts for removed rows, save in state for later use
    koza.state["textmining_row_counts"] = {
        "duplicate_rows": 0,    ## just in case
        "no_ENSP_IDs": 0,
        "no_DOIDs": 0,
        "total_no_IDs": 0,
    }

    df = pd.DataFrame.from_records(data)
    ## data was loaded with empty values = "". Just in case, replace these empty strings with None so isna() methods will work
    df.replace(to_replace="", value=None, inplace=True)
    # ## debugging
    # print(df.shape)

    ## just in case, remove duplicates. Record count of duplicates, if any
    df, koza.state["textmining_row_counts"]["duplicate_rows"] = remove_duplicates(df)
    # ## debugging
    # print(f"After removing duplicates: {df.shape}")


    ## only keep rows with IDs (protein_id column starts with ENSP, disease_id column starts with DOID)
    ## get counts of rows removed (each criterion and total)
    no_ID_counts = dict()
    df, no_ID_counts = keep_rows_with_IDs(df, ID_start_strings)
    ## update koza.state variables
    koza.state["textmining_row_counts"]["no_ENSP_IDs"] = no_ID_counts["ENSP"]
    koza.state["textmining_row_counts"]["no_DOIDs"] = no_ID_counts["DOID"]
    koza.state["textmining_row_counts"]["total_no_IDs"] = no_ID_counts["total"]
    # ## debugging
    # print(f"After removing rows without IDs: {df.shape}")

    ## return updated dataset
    return df.to_dict(orient="records")


@koza.prepare_data(tag="knowledge")
def knowledge_prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    ## remove rows we don't want to process, using pandas
    ## set up counts for removed rows, save in state for later use
    koza.state["knowledge_row_counts"] = {
        "duplicate_rows": 0,
        "no_ENSP_IDs": 0,
        "no_DOIDs": 0,
        "total_no_IDs": 0,
    }

    df = pd.DataFrame.from_records(data)
    ## data was loaded with empty values = "". Just in case, replace these empty strings with None so isna() methods will work
    df.replace(to_replace="", value=None, inplace=True)
    ## debugging: keep because it tells us how much was removed by filter
    print(f"After filter, but before prepare_data: {df.shape}")

    ## remove duplicates, record count of duplicates
    df, koza.state["knowledge_row_counts"]["duplicate_rows"] = remove_duplicates(df)
    # ## debugging
    # print(f"After removing duplicates: {df.shape}")

    ## only keep rows with IDs (protein_id column starts with ENSP, disease_id column starts with DOID). get counts of rows removed (each criterion and total)
    no_ID_counts = dict()
    df, no_ID_counts = keep_rows_with_IDs(df, ID_start_strings)
    ## update koza.state variables
    koza.state["knowledge_row_counts"]["no_ENSP_IDs"] = no_ID_counts["ENSP"]
    koza.state["knowledge_row_counts"]["no_DOIDs"] = no_ID_counts["DOID"]
    koza.state["knowledge_row_counts"]["total_no_IDs"] = no_ID_counts["total"]
    # ## debugging
    # print(f"After removing rows without IDs: {df.shape}")

    ## return updated dataset
    return df.to_dict(orient="records")


@koza.transform_record(tag="textmining")
def textmining_transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## add prefix (data only has value ENSP##########)
    protein = Protein(id = "ENSEMBL:"+record["protein_id"])
    ## disease column DOIDs are already in correct prefix/format for Translator
    disease = Disease(id = record["disease_id"])

    association = CorrelatedGeneToDiseaseAssociation(
        ## creating arbitrary ID for edge right now
        id = str(uuid.uuid4()),
        subject = protein.id,
        predicate = BIOLINK_OCCURS_IN_LIT_WITH,
        object = disease.id,
        sources = [
            RetrievalSource(
                ## making the ID the same as infores for now, which is what go_cam did
                id = INFORES_DISEASES,
                resource_id = INFORES_DISEASES,
                resource_role = ResourceRoleEnum.primary_knowledge_source,
                source_record_urls = [record["url"]],
            )
        ],
        knowledge_level = KnowledgeLevelEnum.statistical_association,
        agent_type = AgentTypeEnum.text_mining_agent,
        z_score = record["z_score"],
        has_confidence_score = record["confidence_score"],
    )

    return KnowledgeGraph(nodes=[protein, disease], edges=[association])


@koza.transform_record(tag="knowledge")
def knowledge_transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## add prefix (data only has value ENSP##########)
    protein = Protein(id = "ENSEMBL:"+record["protein_id"])
    ## disease column DOIDs are already in correct prefix/format for Translator
    disease = Disease(id = record["disease_id"])

    ## set up sources: depends on source_db value
    if record["source_db"] == "MedlinePlus":
        current_sources = [
            ## making the ID the same as infores for now, which is what go_cam did
            RetrievalSource(
                id = INFORES_MEDLINEPLUS,
                resource_id = INFORES_MEDLINEPLUS,
                resource_role = ResourceRoleEnum.primary_knowledge_source,
            ),
            RetrievalSource(
                id = INFORES_DISEASES,
                resource_id = INFORES_DISEASES,
                resource_role = ResourceRoleEnum.aggregator_knowledge_source,
                upstream_resource_ids = [INFORES_MEDLINEPLUS]
            ),
        ]
    elif record["source_db"] == "AmyCo":
        current_sources = [
            ## making the ID the same as infores for now, which is what go_cam did
            RetrievalSource(
                id = INFORES_AMYCO,
                resource_id = INFORES_AMYCO,
                resource_role = ResourceRoleEnum.primary_knowledge_source,
            ),
            RetrievalSource(
                id = INFORES_DISEASES,
                resource_id = INFORES_DISEASES,
                resource_role = ResourceRoleEnum.aggregator_knowledge_source,
                upstream_resource_ids = [INFORES_AMYCO]
            ),
        ]
    else:
        raise ValueError(f"Unexpected source_db value during source mapping: {record["source_db"]}. Explore data and adjust python code.")

    association = GeneToDiseaseAssociation(
        ## creating arbitrary ID for edge right now
        id = str(uuid.uuid4()),
        subject = protein.id,
        predicate = BIOLINK_ASSOCIATED_WITH,
        object = disease.id,
        sources = current_sources,
        knowledge_level = KnowledgeLevelEnum.knowledge_assertion,
        agent_type = AgentTypeEnum.manual_agent,
        has_confidence_score = record["confidence_score"],
    )

    return KnowledgeGraph(nodes=[protein, disease], edges=[association])

## ?? workflow for textmining vs knowledge are the same. But I'm not sure if I can access koza.state variables in a separate, non-tagged function. So having these workflows written twice right now.
@koza.on_data_end(tag="textmining")
def textmining_on_end(koza: koza.KozaTransform) -> None:
    """
    add logs based on counts
    """
    if koza.state["textmining_row_counts"]["duplicate_rows"] > 0:
        koza.log(f"{koza.state["textmining_row_counts"]["duplicate_rows"]} rows were discarded for being duplicates.", level="INFO")
    ## report total "no IDs" first 
    if koza.state["textmining_row_counts"]["total_no_IDs"] > 0:
        koza.log(f"{koza.state["textmining_row_counts"]["total_no_IDs"]} rows were discarded for having either no protein ID (starts with 'ENSP') or no disease ID (starts with 'DOID').", level="INFO")
    ## then report individual 
    if koza.state["textmining_row_counts"]["no_ENSP_IDs"] > 0:
        koza.log(f"{koza.state["textmining_row_counts"]["no_ENSP_IDs"]} rows had no protein ID (starts with 'ENSP').", level="INFO")
    if koza.state["textmining_row_counts"]["no_DOIDs"] > 0:
        koza.log(f"{koza.state["textmining_row_counts"]["no_DOIDs"]} rows had no disease ID (starts with 'DOID').", level="INFO")


@koza.on_data_end(tag="knowledge")
def knowledge_on_end(koza: koza.KozaTransform) -> None:
    """
    add logs based on counts
    """
    if koza.state["knowledge_row_counts"]["duplicate_rows"] > 0:
        koza.log(f"{koza.state["knowledge_row_counts"]["duplicate_rows"]} rows were discarded for being duplicates.", level="INFO")
    ## report total "no IDs" first 
    if koza.state["knowledge_row_counts"]["total_no_IDs"] > 0:
        koza.log(f"{koza.state["knowledge_row_counts"]["total_no_IDs"]} rows were discarded for having either no protein ID (starts with 'ENSP') or no disease ID (starts with 'DOID').", level="INFO")
    ## then report individual 
    if koza.state["knowledge_row_counts"]["no_ENSP_IDs"] > 0:
        koza.log(f"{koza.state["knowledge_row_counts"]["no_ENSP_IDs"]} rows had no protein ID (starts with 'ENSP').", level="INFO")
    if koza.state["knowledge_row_counts"]["no_DOIDs"] > 0:
        koza.log(f"{koza.state["knowledge_row_counts"]["no_DOIDs"]} rows had no disease ID (starts with 'DOID').", level="INFO")