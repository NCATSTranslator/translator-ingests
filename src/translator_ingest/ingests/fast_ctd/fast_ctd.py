from typing import Iterator, Iterable

import requests
import pandas
import itertools

from biolink_model.datamodel.pydanticmodel_v2 import (
    ChemicalEntity,
    ChemicalToDiseaseOrPhenotypicFeatureAssociation,
    Disease,
    NamedThing,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    Association
)
from bs4 import BeautifulSoup
import translator_ingest.ingests.fast_ctd.ingest_utils as iu

# NOTE - This version is mostly stolen from Stephen Ramsey's pandas based implementation of CTD
# it could be faster than the iterate through single records implementation, but it still uses the koza/pydantic data
# loader which is probably the bottleneck, so it's not much faster.


BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT = "biolink:treats_or_applied_or_studied_to_treat"
INFORES_CTD = "infores:ctd"


def get_latest_version():
    # CTD doesn't provide a great programmatic way to determine the latest version, but it does have a Data Status page
    # with a version on it. Fetch the html and parse it to determine the current version.
    html_page: requests.Response = requests.get('http://ctdbase.org/about/dataStatus.go')
    resp: BeautifulSoup = BeautifulSoup(html_page.content, 'html.parser')
    version_header: BeautifulSoup.Tag = resp.find(id='pgheading')
    if version_header is not None:
        # pgheading looks like "Data Status: July 2025", convert it to "July_2025"
        return version_header.text.split(':')[1].strip().replace(' ', '_')
    else:
        raise RuntimeError('Could not determine latest version for CTD, "pgheading" header was missing...')


def transform(records: Iterator[dict]) -> Iterable[tuple[Iterable[NamedThing], Iterable[Association]]]:
    df = pandas.DataFrame(records)
    return iter([(iter(get_nodes(df)), iter(get_edges(df)))])


def get_nodes(ctd_df: pandas.DataFrame) -> tuple[NamedThing]:
    return itertools.chain.from_iterable(map(lambda args: iu.make_nodes(ctd_df, *args),
                                                   [("ChemicalID", "ChemicalName", ChemicalEntity),
                                                    ("DiseaseID", "DiseaseName", Disease)]))


def get_edges(ctd_df: pandas.DataFrame) -> tuple[Association]:
    return iu.make_assocs(ctd_df,
                          "ChemicalID",
                          "DiseaseID",
                          "PubMedIDs",
                          {'predicate': BIOLINK_TREATS_OR_APPLIED_OR_STUDIED_TO_TREAT,
                           'primary_knowledge_source': INFORES_CTD,
                           'knowledge_level': KnowledgeLevelEnum.knowledge_assertion,
                           'agent_type': AgentTypeEnum.manual_agent},
                          (iu.make_tx_col_func("PubMedIDs",
                                               lambda ps: tuple(map(lambda p: 'PMID:' + p, ps.split("|")))),),
                          ChemicalToDiseaseOrPhenotypicFeatureAssociation)