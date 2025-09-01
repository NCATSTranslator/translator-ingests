import uuid
import koza
from typing import Any, Iterable
from koza.model.graphs import KnowledgeGraph

## ADDED packages for this ingest
from datetime import datetime
import pandas as pd
import requests
## ADJUST based on what I am actually using
from biolink_model.datamodel.pydanticmodel_v2 import (
    Gene,
    Disease,
    ChemicalOrGeneOrGeneProductFormOrVariantEnum,
    GeneToDiseaseAssociation,
    RetrievalSource,
    ResourceRoleEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)


BIOLINK_ASSOCIATED_WITH = "biolink:associated_with"
BIOLINK_CAUSES = "biolink:causes"
INFORES_EBI_G2P = "infores:ebi-gene2phenotype"


## EBI G2P's "allelic requirement" values. Biolink-model requires these to be mapped to the synonymous HP IDs. Mapping all, not just those currently in the data. 
## Using OLS API with HP's synonym info to map some values 
ALLELIC_REQ_TO_MAP = [
    "biallelic_autosomal",
    "monoallelic_autosomal",
    "biallelic_PAR",
    "monoallelic_PAR",
    "mitochondrial",
    "monoallelic_Y_hemizygous",
]
## Using hard-coded mappings for some values due to errors in HP's data right now
## Once an HP release / OLS update fixes these errors, can move values to ALLELIC_REQ_TO_MAP
HARDCODED_ALLELIC_REQ_MAPPINGS = {
    "monoallelic_X": "HP:0001417",
    "monoallelic_X_hemizygous": "HP:0001419",
    "monoallelic_X_heterozygous": "HP:0001423",
}
## hard-coded mapping of EBI G2P's "molecular mechanism" values to biolink's `form_or_variant_qualifier` values
## uses `genetic_variant_form` or its descendants
FORM_OR_VARIANT_QUALIFIER_MAPPINGS = {
    "loss of function": ChemicalOrGeneOrGeneProductFormOrVariantEnum.loss_of_function_variant_form,
    "undetermined": ChemicalOrGeneOrGeneProductFormOrVariantEnum.genetic_variant_form,
    "gain of function": ChemicalOrGeneOrGeneProductFormOrVariantEnum.gain_of_function_variant_form,
    "dominant negative": ChemicalOrGeneOrGeneProductFormOrVariantEnum.dominant_negative_variant_form,
    "undetermined non-loss-of-function": ChemicalOrGeneOrGeneProductFormOrVariantEnum.non_loss_of_function_variant_form,
}


def get_latest_version() -> str:
    ## gets the current time with no spaces "%Y-%m-%dT%H:%M:%S.%f%:z"
    ## assuming this function is run at almost the same time that the resource file is downloaded
    return datetime.now(datetime.now().astimezone().tzinfo).isoformat()


## used in `on_data_begin` to build mapping of EBI G2P's allelic requirement values -> HP terms 
def build_allelic_req_mappings(allelic_req_val):
    ## queries OLS to find what HP term has the allelic requirement value as an exact synonym (OLS uses the latest HPO release)
    ols_request = f"https://www.ebi.ac.uk/ols4/api/search?q={allelic_req_val}&ontology=hp&queryFields=synonym&exact=true"
    try:
        response = requests.get(ols_request, timeout=5)
        if response.status_code == 200:
            temp = response.json()
            return temp["response"]["docs"][0]["obo_id"]    ## only need the HP ID
        else:
            print(f"Error encountered on '{allelic_req_val}': {response.status_code}")
    except requests.RequestException as e:
        print(f"Request exemption encountered on '{allelic_req_val}': {e}")


@koza.on_data_begin()
def on_begin(koza: koza.KozaTransform) -> None:
    ## ?? does it need to be saved for later in state vs mutating the existing dictionary?
    ## dynamically create allelic req mappings - dictionary comprehension
    koza.state["allelicreq_mappings"] = {i: build_allelic_req_mappings(i) for i in ALLELIC_REQ_TO_MAP}
    ## add current manual mappings
    koza.state["allelicreq_mappings"].update(HARDCODED_ALLELIC_REQ_MAPPINGS)

    ## counting removed rows
    koza.state["no_diseaseID_stats"] = {
        "n_rows": 0,
        "n_names": 0,
    }
    koza.state["other_row_counts"] = {
        "no_gene_IDs": 0,    ## just in case
        "duplicate_rows": 0,    ## just in case
    }


@koza.on_data_end()
def on_end(koza: koza.KozaTransform) -> None:
    ## add logs based on counts
    if koza.state["no_disease_IDs"] > 0:
        koza.log(f"{koza.state['no_diseaseID_stats']['n_rows']} rows (with {koza.state['no_diseaseID_stats']['n_names']} unique disease names) were discarded for having no disease ID.", level="INFO")
    if koza.state["no_gene_IDs"] > 0:
        koza.log(f"{koza.state['row_counts']['no_gene_IDs']} rows were discarded for having no gene ID.", level="INFO")
    if koza.state["duplicate_rows"] > 0:
        koza.log(f"{koza.state['row_counts']['duplicate_rows']} rows were discarded for being duplicates.", level="INFO")


@koza.prepare_data()
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    ## remove rows we don't want to process, using pandas
    df = pd.DataFrame(data)

    ## currently, there are rows where both disease ID columns have no value. Check, count, remove
    temp_no_disease = df[df["disease mim"].isna() & df["disease MONDO"].isna()]
    if temp_no_disease.shape[0] > 0:    ## number of rows
        ## add to counts
        koza.state["no_diseaseID_stats"]["n_rows"] = temp_no_disease.shape[0]
        koza.state["no_diseaseID_stats"]["n_names"] = len(temp_no_disease["disease name"].unique())
        ## remove rows when BOTH disease ID columns have no value
        df.dropna(how="all", subset=["disease mim", "disease MONDO"], inplace=True, ignore_index=True)

    ## just in case, check for rows with no gene HGNC ID (not in data currently). Count, remove if they're there
    if df[df["hgnc id"].isna()].shape[0] > 0:
        ## add to counts
        koza.state["other_row_counts"]["no_gene_IDs"] = df[df["hgnc id"].isna()].shape[0]
        ## remove
        df.dropna(subset=["hgnc id"], inplace=True, ignore_index=True)

    ## Check for duplicate rows just in case (not in data currently). Count, remove if they're there
    temp_duplicates = df[df.duplicated()]    ## will count all except first occurrence (default)
    if temp_duplicates.shape[0] > 0:
        ## add to counts
        koza.state["other_row_counts"]["duplicate_rows"] = temp_duplicates.shape[0]
        ## remove
        df.drop_duplicates(inplace=True, ignore_index=True)

    ## return updated dataset
    return df.to_dict(orient="records")


@koza.transform_record()
def transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## processing `publications` field 
    publications = ["PMID:"+i.strip() for i in record["publications"].split(";")] if record["publications"] else None
    ## creating url
    url = "https://www.ebi.ac.uk/gene2phenotype/lgd/" + record["g2p id"]
    ## truncating date to only YYYY-MM-DD. Entire date is hitting pydantic date_from_datetime_inexact error 
    date = record["date of last review"][0:10]

    ## ?? okay not to include name?
    gene = Gene(id = "HGNC:"+record["hgnc id"])
    ## picking disease ID: prefer "disease mim" over "disease MONDO"
    if record["disease mim"]:
        ## assuming the value will always be a string
        ## check if value is numeric (OMIM ID) or not
        if record["disease mim"].isnumeric():
            disease = Disease(id = "OMIM:"+record["disease mim"])
        else:    ## these have been orphanet IDs in format Orphanet:######, Translator prefix is all-lowercase
            disease = Disease(id = record["disease mim"].lower())
    else:    ## use "disease MONDO" column, which already has the correct prefix/format for Translator
        disease = Disease(id = record["disease MONDO"])

    ## ?? copying ctd / go_cam, where publications is included even when it is None. Is that fine?
    association = GeneToDiseaseAssociation(
        ## ?? is ID required?
        id = str(uuid.uuid4()),
        subject = gene.id,
        predicate = BIOLINK_ASSOCIATED_WITH,
        qualified_predicate = BIOLINK_CAUSES,
        subject_form_or_variant_qualifier = FORM_OR_VARIANT_QUALIFIER_MAPPINGS[record["molecular mechanism"]],
        object = disease.id,
        sources = [
            RetrievalSource(
                ## ?? current pydantic requires an id field. Doing what go_cam did
                id = INFORES_EBI_G2P,
                resource_id = INFORES_EBI_G2P,
                resource_role = ResourceRoleEnum.primary_knowledge_source,
                source_record_urls = [url]
            )
        ],
        knowledge_level = KnowledgeLevelEnum.knowledge_assertion,
        agent_type = AgentTypeEnum.manual_agent,
        update_date = date,
        allelic_requirement = koza.state["allelicreq_mappings"][record["allelic requirement"]],
        ## include publications!!!
        publications = publications,
    )

    return KnowledgeGraph(nodes=[gene, disease], edges=[association])