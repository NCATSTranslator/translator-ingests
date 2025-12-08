## FROM template, modified for this ingest
import koza
import pandas as pd
from typing import Any, Iterable, Dict, Union
from koza.model.graphs import KnowledgeGraph
from bmt.pydantic import entity_id
from translator_ingest.util.biolink import INFORES_TTD
from translator_ingest.util.http_utils import get_modify_date
from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    DiseaseOrPhenotypicFeature,
    RetrievalSource,
    ResourceRoleEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

## ADDED packages for this ingest
from datetime import datetime
import re
## batched was added in Python 3.12. Pipeline uses Python >=3.12
from itertools import islice, batched
import requests
from translator_ingest.ingests.ttd.mappings import CLINICAL_STATUS_MAP, STRINGS_TO_FILTER, moa_mapping


## hard-coded values and mappings
NAMERES_URL = "https://name-resolution-sri.renci.org/bulk-lookup"  ## DEV instance


## custom functions
def parse_header(file_path) -> Dict[str, Union[str, int]]:
    """
    Parse the headers of txt files with custom format.
    Returns: dict
    - len_header (int): number of lines in header
    - version (str): semantic version from header
    - date (str): date from header
    """
    ## use \\ to escape special characters like ".", "()"
    version_pattern = "^Version ([0-9\\.]+) \\(([0-9\\.]+)\\)"

    line_counter = 0  ## count lines read so far
    dash_counter = 0  ## count dash "divider" lines - header ends after 2nd one

    with open(file_path, "r") as file:
        for line in file:
            if dash_counter == 2:  ## already read the 2nd "divider" line
                break
            else:
                line_counter += 1
                ## if line is a dash "divider" line
                if line.startswith("---") or line.startswith("___"):
                    dash_counter += 1
                ## assuming there's only 1 line in the header that matches this condition
                elif line.startswith("Version"):
                    capture = re.search(version_pattern, line)
                    version = capture.group(1)
                    date = capture.group(2)
                    date = date.replace(".", "-")

    return {"len_header": line_counter, "version": version, "date": date}


def parse_p1_03(file_path, header_len: int) -> Dict[str, list]:
    """
    Parse P1-03 drug mapping file: maps TTD drug IDs to PUBCHEM.COMPOUND IDs (can NodeNorm)

    Returns: dict {TTD: [list of pubchem compound IDs with correct prefixes]}
    """
    ttd_drug_mappings = dict()

    with open(file_path, "r") as file:
        ## iterate from beginning of data (after 2nd dash divider line) to end of file
        for line in islice(file, header_len, None):
            ## skip "blank" lines that only contain whitespace (seem to be "\n")
            if line.isspace():
                continue
            else:
                ## tab-delimited, line ends in "\n" so remove whitespace
                data = [i for i in line.strip().split("\t")]
                ## [0] == TTD ID, [1] == column name, [2] == value

                ## grab pubchem-compound lines
                if data[1] == "PUBCHCID":
                    ## "value" can be "; "-delimited.
                    ## there can be empty values after splitting (ex: splitting "91865905; ")
                    ##   using if to remove these cases
                    ## And its ids don't have prefixes, so add desired prefix
                    ## Only saves if j isn't an empty string ""
                    pubchem_ids = ["PUBCHEM.COMPOUND:" + j.strip() for j in data[2].split(";") if j.strip()]
                    ## add to mappings
                    ttd_drug_mappings[data[0]] = pubchem_ids

    return ttd_drug_mappings


def run_nameres(
    names: Iterable[str],
    url: str,
    batch_size: int = 500,
    types: list | None = None,
    exclude_namespaces: str | None = None,
    score_threshold: int = 0,
):
    """
    Parameters:
    - names: string names to NameRes (iterable - can be a set)
    - url: NameRes url/endpoint to use
    - batch_size: number of strings to include in 1 query to NameRes
    - types (default None): list of biolink categories that NameRes hits should have (hierarchy expansion is supported)
    - exclude_namespaces (default None): |-delimited string of ID namespaces to exclude, for quality
    - score_threshold (default 0): only accept hit if its score is greater than this, for quality

    Returns: tuple of mapping dict and failure stats dict
    """
    ## set up variables to collect output
    mapping = {}
    stats_failures = {
        "unexpected_error": {},
        "returned_empty": [],
        "score_under_threshold": [],
    }
#     ## for debug: stopping early
#     counter = 0

    for batch in batched(names, batch_size):
        req_body = {
            "strings": list(batch),  ## returns tuples -> cast to list
            "autocomplete": False,   ## names are complete search term
            "limit": 1,              ## only want to review top hit
            "biolink_types": types,
            "exclude_prefixes": exclude_namespaces,    ## try to increase quality of hits
        }    
        r = requests.post(url, json=req_body)
        response = r.json()

        ## not doing dict comprehension. allows easier review, logic writing
        for k,v in response.items():
            ## catch unexpected errors
            try:
                ## will catch if v is an empty list (aka NameRes didn't have info)
                if v:
                    ## v is a 1-element list, work with it directly
                    temp = v[0]
                    ## also throw out mapping if score < score_threshold: want better-matching hits
                    if temp["score"] > score_threshold:
                        mapping.update({
                            k: temp["curie"]
                        })
                    else:
                        stats_failures["score_under_threshold"].append(k)
                else:
                    stats_failures["returned_empty"].append(k)
            except Exception as e:
                stats_failures["unexpected_error"].update({k: e})

#         counter += batch_size
#         if counter >= 500:
#             break

    return mapping, stats_failures


## PIPELINE MAIN FUNCTIONS
def get_latest_version() -> str:
    """
    Returns the most recent modify date of the source files, with no spaces "%Y_%m_%d"
    """
    ## planned to use parse_header function on custom txt files.
    ## however, I would need to access the files directly
    ## and I don't know if I can during this step (not in koza?)

    strformat = "%Y_%m_%d"
    # get last-modified for each source data file
    file_links = [
        "https://ttd.idrblab.cn/files/download/P1-03-TTD_crossmatching.txt",
        "https://ttd.idrblab.cn/files/download/P1-05-Drug_disease.txt",
        "https://ttd.idrblab.cn/files/download/P2-01-TTD_uniprot_all.txt",
        "https://ttd.idrblab.cn/files/download/P1-07-Drug-TargetMapping.xlsx",
    ]
    last_modified = [get_modify_date(i) for i in file_links]
    last_modified.sort(reverse=True)  ## does inplace

    ## because of reverse, first element should be the latest
    ## for debugging 
    # print(last_modified[0])
    return last_modified[0]


## P1-05 parsing
@koza.prepare_data(tag="P1_05_parsing")
def p1_05_prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    """
    Access files directly and parse. Take P1-05 indications data (edge-like) and map it into Translator standards
    """
    ## Parse P1-03: maps TTD drug IDs to PUBCHEM.COMPOUND IDs (can NodeNorm)
    koza.log("Parsing P1-03 to retrieve TTD drug ID - PUBCHEM.COMPOUND mappings")

    p1_03_path = f"{koza.input_files_dir}/P1_03_drug_mapping.txt"  ## path to downloaded file
    p1_03_header_info = parse_header(p1_03_path)  ## get number of lines in header
    koza.transform_metadata["ttd_drug_mappings"] = parse_p1_03(p1_03_path, p1_03_header_info["len_header"])
    koza.log(f"Retrieved {len(koza.transform_metadata["ttd_drug_mappings"])} mappings from P1-03")

    ## Parse P1-05: maps TTD drug IDs to PUBCHEM.COMPOUND IDs (can NodeNorm)
    koza.log("Parsing P1-05 to retrieve TTD drug indications data")
    p1_05_path = f"{koza.input_files_dir}/P1_05_indications.txt"  ## path to downloaded file
    p1_05_header_info = parse_header(p1_05_path)  ## get number of lines in header

    ## not wrapping in function because this code won't be reused
    edges = list()  ## list of "edge" objects
    with open(p1_05_path, "r") as file:
        ## iterate from beginning of data (after 2nd dash divider line) to end of file
        for line in islice(file, p1_05_header_info["len_header"], None):
            ## skip "blank" lines that only contain whitespace (seem to be "\n")
            if line.isspace():
                continue
            else:
                ## tab-delimited, has extra whitespace at end of line
                data = [i for i in line.strip().split("\t")]
                ## TTD drug ID line
                if data[0] == "TTDDRUID":
                    ## [0] == column name, [1] == value
                    ## save in temp variable, always seems to be single value
                    ttd_drug = data[1]
                elif data[0] == "INDICATI":
                    ## [0] == column name, [1] == disease name, [2] == disease ICD11 code (not ID! DON'T USE), [3] == clinical status
                    ## don't make edge if indication name seems fake
                    if data[1] == "#N/A":
                        continue
                    else:
                        edges.append(
                            {
                                "subject_ttd_drug": ttd_drug,
                                "object_indication_name": data[1],
                                "clinical_status": data[3],
                            }
                        )
    df = pd.DataFrame(edges)

    ## logging what data looks like right after pre-processing
    koza.log(f"{df.shape[0]} rows retrieved from P1-05")
    koza.log(f"{df["subject_ttd_drug"].nunique()} unique TTD drug IDs")
    koza.log(f"{df["object_indication_name"].nunique()} unique indication names")
    koza.log(f"{df["clinical_status"].nunique()} unique clinical status values")

    ## MAP "clinical status" to biolink predicate
    ## get method returns None if key (clinical status) not found in mapping
    df["biolink_predicate"] = [CLINICAL_STATUS_MAP.get(i) for i in df["clinical_status"]]
    ## log how much data was successfully mapped
    n_mapped = df["biolink_predicate"].notna().sum()
    koza.log(f"{n_mapped} rows with mapped clinical status: {n_mapped / df.shape[0]:.1%}")
    ## save for debugging
    koza.state["clinical_statuses_unmapped"] = sorted(df[df["biolink_predicate"].isna()].clinical_status.unique())
    ## drop rows without predicate mapping
    df.dropna(subset="biolink_predicate", inplace=True, ignore_index=True)

    ## MAP TTD drug IDs to PUBCHEM.COMPOUND (can NodeNorm)
    ## get method returns None if key (TTD ID) not found in mapping
    df["subject_pubchem"] = [koza.transform_metadata["ttd_drug_mappings"].get(i) for i in df["subject_ttd_drug"]]
    ## log how much data was successfully mapped
    n_mapped = df["subject_pubchem"].notna().sum()
    koza.log(f"{n_mapped} rows with mapped TTD drug IDs: {n_mapped / df.shape[0]:.1%}")
    ## drop rows without drug mapping
    df.dropna(subset="subject_pubchem", inplace=True, ignore_index=True)

    ## expand to multiple rows when subject_pubchem list length > 1
    ## also pops every subject_pubchem value out into a string
    df = df.explode("subject_pubchem", ignore_index=True)
    koza.log(f"{df.shape[0]} rows after expanding mappings with multiple ID values")

    ## Filter out some problematic indication names
    ## set case=False so it isn't case-sensitive on matches!
    df = df[~df.object_indication_name.str.contains("|".join(STRINGS_TO_FILTER), case=False)].copy()
    koza.log(f"{df.shape[0]} after filtering out problematic indication names")

    ## MAP indication names to DiseaseOrPheno IDs using NameRes
    ## get set of unique names to put into NameRes
    indication_names = df["object_indication_name"].unique()

    ## GET indication name -> ID mappings
    ## set constants for run_nameres
    indication_types = ["DiseaseOrPhenotypicFeature"]
    indication_exclude_prefixes = "UMLS|MESH"
    indication_score_threshold = 300
    ## use NAMERES_URL initialized earlier, default batch_size
    koza.transform_metadata["indication_mapping"], koza.state["stats_indication_mapping_failures"] = run_nameres(
        names=indication_names,
        url=NAMERES_URL,
        types=indication_types,
        exclude_namespaces=indication_exclude_prefixes,
        score_threshold=indication_score_threshold,
    )
    koza.log(f"Retrieved {len(koza.transform_metadata["indication_mapping"])} indication name -> ID mappings from NameRes")

    ## MAP
    ## get method returns None if key (indication name) not found in mapping 
    df["object_nameres_id"] = [koza.transform_metadata["indication_mapping"].get(i) for i in df["object_indication_name"]]
    ## log how much data was successfully mapped
    n_mapped = df["object_nameres_id"].notna().sum()
    koza.log(f"{n_mapped} rows with mapped indication names: {n_mapped / df.shape[0]:.1%}")
    ## drop rows without indication mapping
    df.dropna(subset="object_nameres_id", inplace=True, ignore_index=True)

    ## Merge rows that look like "duplicates" from Translator output POV
    ## With the current pipeline and data-modeling, only the mapped columns uniquely define an edge
    cols_define_edge = ["subject_pubchem", "biolink_predicate", "object_nameres_id"]
    df = df.groupby(by=cols_define_edge).agg(set).reset_index().copy()
    
    ## log what data looks like at end!
    koza.log(f"{df.shape[0]} rows at end of parsing, after handling 'edge-level' duplicates")
    koza.log(f"{df["subject_pubchem"].nunique()} unique mapped drug IDs")
    koza.log(f"{df["object_nameres_id"].nunique()} unique mapped DiseaseOrPheno IDs")

    ## DONE - output to transform step
    return df.to_dict(orient="records")


@koza.transform_record(tag="P1_05_parsing")
def p1_05_transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## generate TTD urls
    ttd_urls = ["https://ttd.idrblab.cn/data/drug/details/" + i.lower() for i in record["subject_ttd_drug"]]

    chemical = ChemicalEntity(id=record["subject_pubchem"])
    indication = DiseaseOrPhenotypicFeature(id=record["object_nameres_id"])
    association = ChemicalToDiseaseOrPhenotypicFeatureAssociation(
        id=entity_id(),
        subject=chemical.id,
        predicate=record["biolink_predicate"],
        object=indication.id,
        sources=[
            RetrievalSource(
                ## making the ID the same as infores for now, which is what go_cam did
                id=INFORES_TTD,
                resource_id=INFORES_TTD,
                resource_role=ResourceRoleEnum.primary_knowledge_source,
                source_record_urls=ttd_urls,
            )
        ],
        knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
        agent_type=AgentTypeEnum.manual_agent,
    )

    return KnowledgeGraph(nodes=[chemical, indication], edges=[association])
