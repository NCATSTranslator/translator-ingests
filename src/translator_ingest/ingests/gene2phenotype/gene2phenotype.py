## FROM template, modified for this ingest
import koza
from typing import Any, Iterable
from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import entity_id
from translator_ingest.util.biolink import INFORES_EBI_G2P
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
## ADDED packages for this ingest
from datetime import datetime
import pandas as pd
import requests


## HARD-CODED VALUES
BIOLINK_ASSOCIATED_WITH = "biolink:associated_with"
BIOLINK_CAUSES = "biolink:causes"
## EBI G2P's "allelic requirement" values. Biolink-model requires these to be mapped to the synonymous HP IDs.
## Dynamically mapping all possible values (not just those in the data) using OLS API with HP's synonym info
ALLELIC_REQ_TO_MAP = [
    "biallelic_autosomal",
    "monoallelic_autosomal",
    "biallelic_PAR",
    "monoallelic_PAR",
    "mitochondrial",
    "monoallelic_Y_hemizygous",
    "monoallelic_X",
    "monoallelic_X_hemizygous",
    "monoallelic_X_heterozygous",
]
## confidence values to filter out
CONFIDENCE_TO_FILTER = ["limited", "disputed", "refuted"]
## hard-coded mapping of EBI G2P's "molecular mechanism" values to biolink's `form_or_variant_qualifier` values
## uses `genetic_variant_form` or its descendants
FORM_OR_VARIANT_QUALIFIER_MAPPINGS = {
    "loss of function": ChemicalOrGeneOrGeneProductFormOrVariantEnum.loss_of_function_variant_form,
    "undetermined": ChemicalOrGeneOrGeneProductFormOrVariantEnum.genetic_variant_form,
    "gain of function": ChemicalOrGeneOrGeneProductFormOrVariantEnum.gain_of_function_variant_form,
    "dominant negative": ChemicalOrGeneOrGeneProductFormOrVariantEnum.dominant_negative_variant_form,
    "undetermined non-loss-of-function": ChemicalOrGeneOrGeneProductFormOrVariantEnum.non_loss_of_function_variant_form,
}

## CUSTOM FUNCTIONS
## used in `on_data_begin` to build mapping of EBI G2P's allelic requirement values -> HP terms
def build_allelic_req_mappings(allelic_req_val):
    ## queries OLS to find what HP term has the allelic requirement value as an exact synonym (OLS uses the latest HPO release)
    ols_request = (
        f"https://www.ebi.ac.uk/ols4/api/search?q={allelic_req_val}&ontology=hp&queryFields=synonym&exact=true"
    )
    try:
        response = requests.get(ols_request, timeout=5)
        if response.status_code == 200:
            temp = response.json()
            return temp["response"]["docs"][0]["obo_id"]  ## only need the HP ID
        else:
            print(f"Error encountered on '{allelic_req_val}': {response.status_code}")
    except requests.RequestException as e:
        print(f"Request exemption encountered on '{allelic_req_val}': {e}")


## PIPELINE MAIN FUNCTIONS
def get_latest_version() -> str:
    ## gets the current time with no spaces "%Y_%m_%d"
    ## assuming this function is run at almost the same time that the resource file is downloaded
    return datetime.now().strftime("%Y_%m_%d")

@koza.on_data_begin()
def on_begin(koza: koza.KozaTransform) -> None:
    ## generate allelic req mappings
    koza.transform_metadata["allelicreq_mappings"] = {i: build_allelic_req_mappings(i) for i in ALLELIC_REQ_TO_MAP}


@koza.prepare_data()
def prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    df = pd.DataFrame.from_records(data)
    ## data was loaded with empty values = "". Replace these empty strings with None, so isna() methods will work
    df.replace(to_replace="", value=None, inplace=True)
    ## check that there's NAs, isna() methods work
    koza.log(f"{df[df["disease mim"].isna() & df["disease MONDO"].isna()].shape[0]} rows are missing disease IDs (NA)")
    koza.log(f"{df[df["publications"].isna()].shape[0]} rows are missing publications (NA).")

    ## check for orphanet IDs, duplicates just in case (currently not in data, so not handled)
    koza.log(f"{df["disease mim"].str.contains("orpha", case=False, na=False).sum()} rows with orphanet ID in 'disease mim' column")
    koza.log(f"{df.duplicated(keep=False).sum()} duplicate rows")

    ## FILTERING
    ## remove rows with specific confidence values: negated or likely-no relationship
    df = df[~ df["confidence"].isin(CONFIDENCE_TO_FILTER)]
    koza.log(f"{df.shape[0]} rows after removing confidence values: {", ".join(CONFIDENCE_TO_FILTER)}")
    ## remove rows that don't have a disease ID
    df.dropna(how="all", subset=["disease mim", "disease MONDO"], inplace=True, ignore_index=True)
    koza.log(f"{df.shape[0]} rows after removing rows with no disease ID")
    ## TEMPORARY filter for OMIM:188400
    ## NodeNorm incorrectly assigns this to a Gene when it's actually a Disease, causing an incorrect edge to be made
    TEMP_OMIM_FILTER = ["188400"]
    df = df[~ df["disease mim"].isin(TEMP_OMIM_FILTER)]
    koza.log(f"{df.shape[0]} rows after removing disease OMIM: {", ".join(TEMP_OMIM_FILTER)}") 

    ## return updated dataset
    return df.to_dict(orient="records")


@koza.transform_record()
def transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## processing `publications` field
    if pd.notna(record["publications"]):
        publications = ["PMID:" + i.strip() for i in record["publications"].split(";")]
    else:
        publications = None
    ## creating url
    url = "https://www.ebi.ac.uk/gene2phenotype/lgd/" + record["g2p id"]
    ## truncating date to only YYYY-MM-DD. Entire date is hitting pydantic date_from_datetime_inexact error
    date = record["date of last review"][0:10]

    gene = Gene(id=f"HGNC:{record["hgnc id"]}")
    ## picking disease ID: prefer "disease mim" over "disease MONDO"
    if pd.notna(record["disease mim"]):
        disease = Disease(id=f"OMIM:{record["disease mim"]}")
    else:  ## use "disease MONDO" column, which already has the correct prefix/format for Translator
        disease = Disease(id=record["disease MONDO"])

    association = GeneToDiseaseAssociation(
        ## creating arbitrary ID for edge right now
        id=entity_id(),
        subject=gene.id,
        predicate=BIOLINK_ASSOCIATED_WITH,
        qualified_predicate=BIOLINK_CAUSES,
        subject_form_or_variant_qualifier=FORM_OR_VARIANT_QUALIFIER_MAPPINGS[record["molecular mechanism"]],
        object=disease.id,
        sources=[
            RetrievalSource(
                ## making the ID the same as infores for now, which is what go_cam did
                id=INFORES_EBI_G2P,
                resource_id=INFORES_EBI_G2P,
                resource_role=ResourceRoleEnum.primary_knowledge_source,
                source_record_urls=[url],
            )
        ],
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
        update_date=date,
        allelic_requirement=koza.transform_metadata["allelicreq_mappings"][record["allelic requirement"]],
        gene2phenotype_confidence_category=record["confidence"],
        ## include publications!!!
        publications=publications,
    )

    return KnowledgeGraph(nodes=[gene, disease], edges=[association])
