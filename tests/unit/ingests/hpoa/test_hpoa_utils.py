"""
Tests of HPOA Utils methods
"""
from typing import Optional
from re import compile

import pytest

from src.translator_ingest.util.github import GitHubReleases
from translator_ingest.ingests.hpoa.phenotype_ingest_utils import (
    FrequencyHpoTerm,
    Frequency,
    get_frequency_hpo_term,
    map_percentage_frequency_to_hpo_term,
    phenotype_frequency_to_hpo_term,
)

vre = compile(pattern=r"^20\d\d-\d\d-\d\d$")

def test_hpoa_latest_version():
    ghr = GitHubReleases(git_org="obophenotype", git_repo="human-phenotype-ontology")
    version = ghr.get_latest_version()
    assert version is not None and vre.match(version)

def test_get_hpo_term():
    assert get_frequency_hpo_term("HP:0040282") == FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79)

@pytest.mark.parametrize(
    "query",
    [
        (0, FrequencyHpoTerm(curie="HP:0040285", name="Excluded", lower=0, upper=0)),
        (1, FrequencyHpoTerm(curie="HP:0040284", name="Very rare", lower=1, upper=4)),
        (2, FrequencyHpoTerm(curie="HP:0040284", name="Very rare", lower=1, upper=4)),
        (4, FrequencyHpoTerm(curie="HP:0040284", name="Very rare", lower=1, upper=4)),
        (20, FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29)),
        (50, FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79)),
        (85, FrequencyHpoTerm(curie="HP:0040281", name="Very frequent", lower=80, upper=99)),
        (100, FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100)),
    ],
)
def test_map_percentage_frequency_to_hpo_term(query: tuple[int, Optional[FrequencyHpoTerm]]):
    assert map_percentage_frequency_to_hpo_term(query[0]) == query[1]


@pytest.mark.parametrize(
    "query",
    [-1, 101]
)
def test_invalid_query_to_map_percentage_frequency_to_hpo_term(query: int):
    with pytest.raises(ValueError):
        map_percentage_frequency_to_hpo_term(query)


@pytest.mark.parametrize(
    "query,frequency",
    [
        (None, Frequency()),          # query 0 - None input
        ("", Frequency()),            # query 1 - empty string input
        ("0", Frequency()),           # query 2 - not a raw number... has to be tagged as a percentage?
        ("HP:0040279", Frequency()),  # query 3 - the sub-ontology term below HP:0040279, outside HPO term
        (   # query 4 - exact matches to global lower bounds should be sent back accurately
            "0%",
            # FrequencyHpoTerm(curie="HP:0040285", name="Excluded", lower=0, upper=0),
            Frequency(
                frequency_qualifier="HP:0040285",
                has_percentage=0.0,
                has_quotient=0.0,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 5 - exact matches to lower bounds should be sent back accurately
            "5%",
            # FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29),
            Frequency(
                frequency_qualifier="HP:0040283",
                has_percentage=5.0,
                has_quotient=0.05,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 6 - matches within percentage range bounds should be sent back accurately
            "17%",
            # FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29),
            Frequency(
                frequency_qualifier="HP:0040283",
                has_percentage=17.0,
                has_quotient=0.17,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 7 - exact matches to upper bounds should be sent back accurately
            "29%",
            # FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29),
            Frequency(
                frequency_qualifier="HP:0040283",
                has_percentage=29.0,
                has_quotient=0.29,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 8 - exact matches to global upper bounds should be sent back accurately
            "100%",
            # FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100),
            Frequency(
                frequency_qualifier="HP:0040280",
                has_percentage=100.0,
                has_quotient=1.0,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 9 - rounds down if decimal value < 0.5 of above an upper bound
            "79.4%",
            # FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=100, upper=100),
            Frequency(
                frequency_qualifier="HP:0040282",
                has_percentage=79.4,
                has_quotient=0.79,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 10 - rounds upwards if decimal value >= 0.5 above an upper bound
            "79.5%",
            # FrequencyHpoTerm(curie="HP:0040281", name="Very frequent", lower=100, upper=100),
            Frequency(
                frequency_qualifier="HP:0040281",
                has_percentage=79.5,
                has_quotient=0.80,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 11 - if a valid 'Frequency' HP ontology term is
            #            already given, then it should be sent back,
            #            but without any indicative ranges
            "HP:0040282",
            # FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79),
            Frequency(
                frequency_qualifier="HP:0040282",
                has_percentage=None,
                has_quotient=None,
                has_count=None,
                has_total=None
            )
        ),
        (   # query 12 - division ratios converted to percentages (i.e., 7/13 ~ 53.8%) that match
            # within a specific percentage range should be sent back accurately
            "7/13",
            # FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79),
            Frequency(
                frequency_qualifier="HP:0040282",
                has_percentage=54.0,
                has_quotient=0.54,
                has_count=7,
                has_total=13
            )
        ),
        (  # query 13 - obligate percentage value
            "1/1",
            # FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100),
            Frequency(
                frequency_qualifier="HP:0040280",
                has_percentage=100.0,
                has_quotient=1.0,
                has_count=1,
                has_total=1
            )
        ),
        (  # query 14 - still an obligate percentage value
            "2/2",
            # FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100),
            Frequency(
                frequency_qualifier="HP:0040280",
                has_percentage=100.0,
                has_quotient=1.0,
                has_count=2,
                has_total=2
            )
        ),
        (   # query 15 - frequent ratio/percentage value
            "1/2",
            # FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79),
            Frequency(
                frequency_qualifier="HP:0040282",
                has_percentage=50.0,
                has_quotient=0.5,
                has_count=1,
                has_total=2
            )
        ),
    ],
)
def test_phenotype_frequency_to_hpo_term(query: Optional[str], frequency: Optional[Frequency]):
    result: Optional[Frequency]  = phenotype_frequency_to_hpo_term(query)
    assert result == frequency
