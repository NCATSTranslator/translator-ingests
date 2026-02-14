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
    ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation,
    DiseaseOrPhenotypicFeature,
    Protein,    ## because ttd gives uniprot names
    ChemicalAffectsGeneAssociation,    ## ONLY for affects
    ChemicalGeneInteractionAssociation,    ## ONLY for interacts_with
    RetrievalSource,
    ResourceRoleEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
)

## ADDED packages for this ingest
from translator_ingest.util.logging_utils import get_logger
import re
## batched was added in Python 3.12. Pipeline uses Python >=3.12
from itertools import islice, batched
import requests
# ## needed for pd.read_excel
# import openpyxl
from translator_ingest.ingests.ttd.mappings import CLINICAL_STATUS_MAP, STRINGS_TO_FILTER, MOA_MAPPING
## increment this when your file changes will affect the output
##   (even with the same resource data) to trigger a new build
TRANSFORM_VERSION = "1.1"


## hard-coded values and mappings
NAMERES_URL = "https://name-lookup.ci.transltr.io/bulk-lookup"  ## CI instance


## custom functions
def parse_header(file_path) -> Dict[str, Union[str, int]]:
    """
    Parse the headers of txt files with custom format.
    Returns: dict
    - len_header (int): number of lines in header
    """
    ## use \\ to escape special characters like ".", "()"
    # version_pattern = "^Version ([0-9\\.]+) \\(([0-9\\.]+)\\)"

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
                # elif line.startswith("Version"):
                #     capture = re.search(version_pattern, line)
                #     version = capture.group(1)
                #     date = capture.group(2)
                #     date = date.replace(".", "-")

    return {"len_header": line_counter}
    # return {"len_header": line_counter, "version": version, "date": date}


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
    logger = get_logger(__name__)
    ## set up variables to collect output
    mapping = {}
    stats_failures = {
        "unexpected_error": {},
        "returned_empty": [],
        "score_under_threshold": [],
    }
    ## for printing progress
    counter = 0

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

        counter += batch_size
        if counter < len(names):   ## will keep going, log where we're at
            logger.info(f"{counter} names processed (out of {len(names)})")
        else:   ## done: use actual length
            logger.info(f"Finished processing {len(names)} names.")
#         ## for debug: tracking progress, stopping early
#         if counter >= 500:
#             break

    return mapping, stats_failures


def parse_p2_01(file_path, header_len: int):
    """
    Parse P2-01 target mapping file: maps TTD target IDs to uniprot names (not IDs!)

    Returns: tuple
    - dict {TTD: {"uniprot_names": [list of uniprot names]}}
    - set of all unique uniprot names
    """
    ## format {TTD: {"uniprot_names": [list]}}
    ttd_target_mappings = dict()
    ## get set of all unique names for mapping step
    all_uniprot_names = set()
    ## saved for this function
    DELIMITER_PAT = "-|/|;"
    INVALID_CHAR = [" ", "(", ")"]

    with open(file_path, "r") as file:
        ## iterate from beginning of data (after 2nd dash divider line) to end of file
        for line in islice(file, header_len, None):
            ## skip "blank" lines that only contain whitespace (seem to be "\n")
            if line.isspace():
                continue
            else:
                ## tab-delimited, may have extra whitespace at end of line
                data = [i for i in line.strip().split("\t")]
                ## [0] == column name, [1] == value

                ## TTD target ID line
                if data[0] == "TARGETID":
                    ## save in temp variable, always seems to be single value
                    ttd_target = data[1]
                elif data[0] == "UNIPROID":
                    ## don't save name if it seems fake
                    ## NOUNIPROTAC seems to be "NO UNIPROT AC"
                    if data[1] != "NOUNIPROTAC":
                        ## split on delimiter chars
                        ## only save element j if it is non-empty string and doesn't have any invalid char in it
                        ## strip surrounding whitespace off j and save
                        temp_name = [j.strip() \
                                        for j in re.split(DELIMITER_PAT, data[1]) \
                                        if j.strip() and not any(letter in INVALID_CHAR for letter in j.strip())]
                        
                        ## only add if has values
                        if temp_name:
                            ## add to unique set
                            all_uniprot_names.update(temp_name)
                            ## add to mappings
                            ttd_target_mappings[ttd_target] = {"uniprot_names": temp_name}

    return ttd_target_mappings,all_uniprot_names 


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
    last_modified = [get_modify_date(i, strformat) for i in file_links]
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

    ## Parse P1-05
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

    ## MAP "clinical status" to biolink predicate, edge-attributes
    ## make case consistent - have stuff like "Phase 3" vs "phase 3"
    df["clinical_status"] = df["clinical_status"].str.lower()
    koza.log(f"Update: {df["clinical_status"].nunique()} unique clinical status values after making case consistent")
    ## MAPPING
    ## get method returns default None if key (clinical status) not found in mapping
    df["biolink_predicate"] = [CLINICAL_STATUS_MAP[i]["predicate"] if CLINICAL_STATUS_MAP.get(i) else None for i in df["clinical_status"]]
    df["clinical_approval_status"] = [
        CLINICAL_STATUS_MAP[i].get("clinical_approval_status") if CLINICAL_STATUS_MAP.get(i) else None
        for i in df["clinical_status"]
    ]
    df["max_research_phase"] = [
        CLINICAL_STATUS_MAP[i].get("max_research_phase") if CLINICAL_STATUS_MAP.get(i) else None
        for i in df["clinical_status"]
    ]
    ## log how much data was successfully mapped
    n_mapped = df["biolink_predicate"].notna().sum()
    koza.log(f"{n_mapped} rows with mapped clinical status: {n_mapped / df.shape[0]:.1%}")
    n_clinical_approval = df["clinical_approval_status"].notna().sum()
    koza.log(f"{n_clinical_approval} rows with clinical_approval_status")
    n_max_res = df["max_research_phase"].notna().sum()
    koza.log(f"{n_max_res} rows with max_research_phase")
    ## save for debugging
    koza.transform_metadata["clinical_statuses_unmapped"] = sorted(df[df["biolink_predicate"].isna()].clinical_status.unique())
    ## drop rows without predicate mapping - shortcut for having a mapping at all
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
    koza.log("Running NameRes on indication names.")
    indication_types = ["DiseaseOrPhenotypicFeature"]
    indication_exclude_prefixes = "UMLS|MESH"
    indication_score_threshold = 300
    ## use NAMERES_URL initialized earlier, default batch_size
    koza.transform_metadata["indication_mapping"], koza.transform_metadata["stats_indication_mapping_failures"] = run_nameres(
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
    cols_define_edge = ["subject_pubchem", "biolink_predicate", "object_nameres_id", "clinical_approval_status", "max_research_phase"]
    ## dropna is so it handles None values in edge-attribute columns
    df = df.groupby(by=cols_define_edge, dropna=False).agg(set).reset_index().copy()

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

    ## process edge-attributes values - if NA, convert to None
    if pd.isna(record["clinical_approval_status"]):
        app_status = None
    else:
        app_status = record["clinical_approval_status"]
    if pd.isna(record["max_research_phase"]):
        max_res = None
    else:
        max_res = record["max_research_phase"]

    chemical = ChemicalEntity(id=record["subject_pubchem"])
    indication = DiseaseOrPhenotypicFeature(id=record["object_nameres_id"])
    association = ChemicalEntityToDiseaseOrPhenotypicFeatureAssociation(
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
        clinical_approval_status=app_status,
        max_research_phase=max_res,
    )

    return KnowledgeGraph(nodes=[chemical, indication], edges=[association])


## P1-07 parsing
@koza.prepare_data(tag="P1_07_parsing")
def p1_07_prepare(koza: koza.KozaTransform, data: Iterable[dict[str, Any]]) -> Iterable[dict[str, Any]] | None:
    """
    Access files directly and parse. Take P1-07 drug-target data (edge-like) and map it into Translator standards
    """
    ## Parse P1-03: maps TTD drug IDs to PUBCHEM.COMPOUND IDs (can NodeNorm)
    koza.log("Parsing P1-03 to retrieve TTD drug ID - PUBCHEM.COMPOUND mappings")
    p1_03_path = f"{koza.input_files_dir}/P1_03_drug_mapping.txt"  ## path to downloaded file
    p1_03_header_info = parse_header(p1_03_path)  ## get number of lines in header
    koza.transform_metadata["ttd_drug_mappings"] = parse_p1_03(p1_03_path, p1_03_header_info["len_header"])
    koza.log(f"Retrieved {len(koza.transform_metadata["ttd_drug_mappings"])} mappings from P1-03")

    ## Parse P2-01: maps TTD target IDs to uniprot names, then use names to get NodeNorm-able IDs
    koza.log("Parsing P2-01 to retrieve TTD target ID - gene/protein ID mappings")
    p2_01_path = f"{koza.input_files_dir}/P2-01-TTD_uniprot_all.txt"  ## path to downloaded file
    p2_01_header_info = parse_header(p2_01_path)  ## get number of lines in header

    ## don't save to koza until done (mapped to IDs)
    ttd_target_mappings, all_uniprot_names = parse_p2_01(p2_01_path, p2_01_header_info["len_header"])
    koza.log(f"Retrieved {len(ttd_target_mappings)} initial mappings from P2-01")

    ## run NameRes on the uniprot names
    koza.log("Running NameRes on uniprot names.")
    target_types = ["GeneOrGeneProduct"]
    # target_exclude_prefixes = "UMLS"    ## not using to speed up responses
    ## use NAMERES_URL initialized earlier, default batch_size
    koza.transform_metadata["uniprot_name_to_id"], koza.transform_metadata["stats_target_mapping_failures"] = run_nameres(
        names=all_uniprot_names,
        url=NAMERES_URL, 
        types=target_types,
        # exclude_namespaces=target_exclude_prefixes,     ## not using to speed up responses 
    )
    ## catch mappings to IDs that aren't UniProtKB (name to ID direct) or NCBIGene (conflated to Gene)
    invalid_mappings = dict()
    for k,v in koza.transform_metadata["uniprot_name_to_id"].items():
        if not (v.startswith("NCBIGene") or v.startswith("UniProtKB")):
            ## for debugging
            # print(k,v)
            invalid_mappings.update({k: v})
    ## remove these mappings
    for k in invalid_mappings.keys():
        del koza.transform_metadata["uniprot_name_to_id"][k]
    koza.log(f"Retrieved {len(koza.transform_metadata["uniprot_name_to_id"])} mappings from uniprot names to entity IDs in NameRes")

    ## add mapped IDs to ttd_target_mappings, collect target names that failed nameres process
    ## ttd_target_mappings format {TTD: {"uniprot_names": [list], "mapped_ids": [list]}}
    failed_TTD_targets = set()
    for k,v in ttd_target_mappings.items():
        ## use set so no duplicate IDs
        temp_ids = set()
        for i in v["uniprot_names"]:
            temp =  koza.transform_metadata["uniprot_name_to_id"].get(i)
            ## found mapping
            if temp:
                temp_ids.add(temp)
        ## temp_ids isn't empty
        if temp_ids:
            ttd_target_mappings[k]["mapped_ids"] = list(temp_ids)
        else:
            failed_TTD_targets.add(k)
    ## delete TTD target mappings that don't have any mapped IDs
    for i in failed_TTD_targets:
        del ttd_target_mappings[i]
    koza.log(f"Retrieved {len(ttd_target_mappings)} final mappings from P2-01")
    ## save final versions of this info
    koza.transform_metadata["ttd_target_mappings"] = ttd_target_mappings
    ## koza uses json dump, which doesn't accept sets
    koza.transform_metadata["targets_unmapped"] = list(failed_TTD_targets)

    ## Parse P1-07
    koza.log("Parsing P1-07 to retrieve drug-target data")
    p1_07_path = f"{koza.input_files_dir}/P1-07-Drug-TargetMapping.xlsx"  ## path to downloaded file
    ## only import columns needed
    df_07 = pd.read_excel(io=p1_07_path, usecols=["TargetID", "DrugID", "MOA"], na_values=".")
    koza.log(f"{df_07.shape[0]} rows loaded.")

    ## first round of cleaning up MOA column
    df_07["MOA"] = df_07["MOA"].str.split(";")    ## ";"-delimited, split
    ## expand to multiple rows when MOA list length > 1
    ## also pops every MOA value out into a string
    df_07 = df_07.explode("MOA", ignore_index=True)
    koza.log(f"{df_07.shape[0]} rows after expanding MOAs with multiple values")
    ## clean up things that aren't actually unique values
    df_07["MOA"] = df_07["MOA"].str.strip()    ## remove whitespace 
    df_07["MOA"] = df_07["MOA"].str.lower()    ## make case consistent - have stuff like "Inhibitor" vs "inhibitor"
    ## typos
    ## replacements are trickier because there's NA values in column
    ## replaces np.nan with None
    df_07["MOA"] = [re.sub("agonis$", "agonist", i) if pd.notna(i) else None for i in df_07["MOA"]]
    df_07["MOA"] = [re.sub("stablizer", "stabilizer", i) if pd.notna(i) else None for i in df_07["MOA"]]
    ## replace None with "NO_VALUE"
    ## otherwise will have buggy behavior with groupby having None in key column and dropping these rows
    df_07["MOA"] = df_07["MOA"].fillna("NO_VALUE")
    ## save info, log
    koza.transform_metadata["all_moa"] = sorted(df_07["MOA"].unique())
    koza.log(f"Cleaned up MOA column: {len(koza.transform_metadata["all_moa"])} unique values")

    ## MAP TTD drug IDs to PUBCHEM.COMPOUND (can NodeNorm)
    ## get method returns None if key (TTD ID) not found in mapping
    df_07["subject_pubchem"] = [koza.transform_metadata["ttd_drug_mappings"].get(i) for i in df_07["DrugID"]]
    ## log how much data was successfully mapped
    n_mapped = df_07["subject_pubchem"].notna().sum()
    koza.log(f"{n_mapped} rows with mapped TTD drug IDs: {n_mapped / df_07.shape[0]:.1%}")
    ## drop rows without drug mapping
    df_07.dropna(subset="subject_pubchem", inplace=True, ignore_index=True)
    ## expand to multiple rows when subject_pubchem list length > 1
    ## also pops every subject_pubchem value out into a string
    df_07 = df_07.explode("subject_pubchem", ignore_index=True)
    koza.log(f"{df_07.shape[0]} rows after expanding mappings with multiple ID values")

    ## MAP TTD target IDs to gene/protein IDs
    ## using the variables directly, not stuff saved in koza.transform_metadata
    ## if mapping exists (key found by get), then mapping should have "mapped_ids"
    df_07["object_id"] = [ttd_target_mappings[i]["mapped_ids"] if ttd_target_mappings.get(i) else None for i in df_07["TargetID"]]
    ## log how much data was successfully mapped
    n_mapped = df_07["object_id"].notna().sum()
    koza.log(f"{n_mapped} rows with mapped TTD target IDs: {n_mapped / df_07.shape[0]:.1%}")
    ## drop rows without target mapping
    df_07.dropna(subset="object_id", inplace=True, ignore_index=True)
    ## expand to multiple rows when object_id list length > 1
    df_07 = df_07.explode("object_id", ignore_index=True)
    koza.log(f"{df_07.shape[0]} rows after expanding mappings with multiple ID values")

    ## create new column mod_moa with 1 "moa" value per each unique data-modeling
    ##   this is helpful for merging rows into "edges" later
    ## current MOA values with the same data-modeling
    BINDING_TYPES = {"binder", "ligand"}
    BLOCKING_TYPES = {"blocker", "blocker (channel blocker)"}
    df_07["mod_moa"] = [
        "BINDING" if i in BINDING_TYPES \
        else "BLOCKING" if i in BLOCKING_TYPES \
        else i
        for i in df_07["MOA"]
    ]

    ## FILTER out rows with mod_moa values that aren't in MOA_MAPPING
    ## getting the unmapped values
    mod_moa = set(df_07["mod_moa"])
    notmapped_indata = mod_moa - set(MOA_MAPPING.keys())
    ## make regex version to correctly select rows with these unmapped values
    regex_unmapped_moa = set()
    for i in notmapped_indata:
        temp = i
        ## add escape characters
        temp = temp.replace("(", "\\(")
        temp = temp.replace(")", "\\)")
        ## add beginning and ending marks, to avoid selecting partial matches inadvertently (not really a problem currently, but still)
        temp = "^" + temp + "$"
        regex_unmapped_moa.add(temp)
    ## remove rows with these unmapped values
    n_before = df_07.shape[0]
    ## use mod_moa to be consistent, mostly working with this col going forward
    df_07 = df_07[~ df_07.mod_moa.str.contains('|'.join(regex_unmapped_moa))]
    n_after = df_07.shape[0]
    final_moa = set(df_07["MOA"])
    final_mod_moa = set(df_07["mod_moa"])
    koza.log(f"{n_after} rows with mapped MOA: {n_after / n_before:.1%}")
    koza.log(f"{len(final_moa)} unique MOA values kept (corresponds to {len(final_mod_moa)} mapping keys)")
    ## save lists of unused moa (based on original MOA column values, not mod_moa)
    ## koza uses json dump, which doesn't accept sets
    ## no mapping but is in the data
    koza.transform_metadata["moa_notmapped_indata"] = sorted(notmapped_indata)
    ## has mapping but wasn't in the data after mapping/filtering TTD IDs
    koza.transform_metadata["moa_mapped_nodata"] = sorted(set(MOA_MAPPING.keys()) - mod_moa)
    ## no mapping and wasn't in the data after mapping/filtering TTD IDs
    ## starting MOA column values - mapped,indata - mapped,not_in_data - not_mapped,in_data
    ## using MOA column because all_moa is based on it
    koza.transform_metadata["moa_notmapped_nodata"] = sorted(
        set(koza.transform_metadata["all_moa"])
        - final_moa
        - set(koza.transform_metadata["moa_mapped_nodata"])
        - set(koza.transform_metadata["moa_notmapped_indata"])
    )

    ## Merge rows that look like "duplicates" from Translator output POV
    ##   With the current pipeline and data-modeling, only the mapped columns uniquely define an edge
    cols_define_edge = ["subject_pubchem", "object_id", "mod_moa"]
    df_07 = df_07.groupby(by=cols_define_edge).agg(set).reset_index().copy()

    ## log what data looks like at end!
    koza.log(f"{df_07.shape[0]} rows at end of parsing, after handling 'edge-level' duplicates")
    koza.log(f"{df_07["subject_pubchem"].nunique()} unique mapped drug IDs")
    koza.log(f"{df_07["object_id"].nunique()} unique mapped target IDs")

    ## DONE - output to transform step
    return df_07.to_dict(orient="records")


@koza.transform_record(tag="P1_07_parsing")
def p1_07_transform(koza: koza.KozaTransform, record: dict[str, Any]) -> KnowledgeGraph | None:
    ## generate TTD urls to drug - based on manual review, target page sometimes doesn't have this info
    ## details: https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues/30#issuecomment-3209944640
    ttd_urls = ["https://ttd.idrblab.cn/data/drug/details/" + i.lower() for i in record["DrugID"]]
    ttd_source=[
        RetrievalSource(
            ## making the ID the same as infores for now, which is what go_cam did
            id=INFORES_TTD,
            resource_id=INFORES_TTD,
            resource_role=ResourceRoleEnum.primary_knowledge_source,
            source_record_urls=ttd_urls,
        )
    ]            
    
    ## Nodes
    chemical = ChemicalEntity(id=record["subject_pubchem"])
    protein = Protein(id=record["object_id"])

    ## diff Association type depending on predicate
    ## assuming record's mod_moa value is in MOA_MAPPING keys. If not, should stop/error, then MOA_MAPPING or p1_07_prepare needs adjustment.  
    data_modeling = MOA_MAPPING[record["mod_moa"]]

    ## diff Association type depending on predicate
    if "interacts_with" in data_modeling["predicate"]:
    ## covers "interacts_with" and descendants with substring
    ## ASSUMING no special logic, so only 1 edge made
        association = ChemicalGeneInteractionAssociation(
            id=entity_id(),
            subject=chemical.id,
            object=protein.id,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
            sources=ttd_source,
            predicate=data_modeling["predicate"],
            ## return empty dict in scenario that MOA_MAPPING doesn't have "qualifiers". to avoid error
            **data_modeling.get("qualifiers", dict())
        )
        return KnowledgeGraph(nodes=[chemical, protein], edges=[association])
    elif "affects" in data_modeling["predicate"]:
    ## currently covers all other cases
        ## MAIN EDGE
        association = ChemicalAffectsGeneAssociation(
            id=entity_id(),
            subject=chemical.id,
            object=protein.id,
            knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
            agent_type=AgentTypeEnum.manual_agent,
            sources=ttd_source,
            predicate=data_modeling["predicate"],
            ## return empty dict in unlikely scenario that MOA_MAPPING doesn't have "qualifiers". to avoid error
                **data_modeling.get("qualifiers", dict())
        )
        ## if there's an extra edge field
        if data_modeling.get("extra_edge_pred"):
            ## SPECIAL logic: create extra "physical interaction" edge for some "affects" edges
            ## should be identical to original edge, except predicate/no qualifiers
            extra_assoc = ChemicalGeneInteractionAssociation(
                id=entity_id(),
                subject=chemical.id,
                object=protein.id,
                knowledge_level=KnowledgeLevelEnum.knowledge_assertion,
                agent_type=AgentTypeEnum.manual_agent,
                sources=ttd_source,
                predicate=data_modeling["extra_edge_pred"],
            )
            ## return both edges
            return KnowledgeGraph(nodes=[chemical, protein], edges=[association, extra_assoc])
        else:
            ## return only 1 edge
            return KnowledgeGraph(nodes=[chemical, protein], edges=[association])
