"""
Tests of HPOA Utils methods
"""
from typing import Optional, Tuple

import pytest

from src.translator_ingest.ingests.hpoa.phenotype_ingest_utils import (
    FrequencyHpoTerm,
    Frequency,
    get_hpo_term,
    map_percentage_frequency_to_hpo_term,
    phenotype_frequency_to_hpo_term,
)


def test_get_hpo_term():
    assert get_hpo_term("HP:0040282") == FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79)


@pytest.mark.parametrize(
    "query",
    [
        (-1, None),
        (0, FrequencyHpoTerm(curie="HP:0040285", name="Excluded", lower=0, upper=0)),
        (1, FrequencyHpoTerm(curie="HP:0040284", name="Very rare", lower=1, upper=4)),
        (2, FrequencyHpoTerm(curie="HP:0040284", name="Very rare", lower=1, upper=4)),
        (4, FrequencyHpoTerm(curie="HP:0040284", name="Very rare", lower=1, upper=4)),
        (20, FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29)),
        (50, FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79)),
        (85, FrequencyHpoTerm(curie="HP:0040281", name="Very frequent", lower=80, upper=99)),
        (100, FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100)),
        (101, None),
    ],
)
def test_map_percentage_frequency_to_hpo_term(query: Tuple[int, Optional[FrequencyHpoTerm]]):
    assert map_percentage_frequency_to_hpo_term(query[0]) == query[1]


@pytest.mark.parametrize(
    "query,frequency",
    [
        # Frequency(
        #         frequency_qualifier=hpo_term.curie if hpo_term else None,
        #         has_percentage=percentage,
        #         has_quotient=quotient,
        #         has_count=has_count,
        #         has_total=has_total
        # )
        (None, None),          # query 0 - None input
        ("", None),            # query 1 - empty string input
        ("0", None),           # query 2 - not a raw number... has to be tagged as a percentage?
        ("HP:0040279", Frequency()),  # query 3 - the sub-ontology term below HP:0040279, outside HPO term
        #
        # TODO: Need to fix these test data, carry over from an older method protocol
        # (   # query 4 - exact matches to global lower bounds should be sent back accurately
        #     "0%",
        #     FrequencyHpoTerm(curie="HP:0040285", name="Excluded", lower=0, upper=0),
        #     0.0,
        #     None
        #     # Frequency(
        #     #         frequency_qualifier=hpo_term.curie if hpo_term else None,
        #     #         has_percentage=percentage,
        #     #         has_quotient=quotient,
        #     #         has_count=has_count,
        #     #         has_total=has_total
        #     # )
        # ),
        # (   # query 5 - exact matches to lower bounds should be sent back accurately
        #     "5%",
        #     FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29),
        #     5.0,
        #     None
        # ),
        # (   # query 6 - matches within percentage range bounds should be sent back accurately
        #     "17%",
        #     FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29),
        #     17.0,
        #     None
        # ),
        # (   # query 7 - exact matches to upper bounds should be sent back accurately
        #     "29%",
        #     FrequencyHpoTerm(curie="HP:0040283", name="Occasional", lower=5, upper=29),
        #     29.0,
        #     None
        # ),
        # (   # query 8 - exact matches to global upper bounds should be sent back accurately
        #     "100%",
        #     FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100),
        #     100.0,
        #     None
        # ),
        # (   # query 9 - if a valid 'Frequency' HPO subontology term already, should be sent back
        #     "HP:0040282",
        #     FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79),
        #     None,
        #     None
        # ),
        # (   # query 10 - division ratios converted to percentages (i.e., 7/13 ~ 53.8%) that match
        #     # within a specific percentage range should be sent back accurately
        #     "7/13",
        #     FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79),
        #     None,
        #     0.54
        # ),
        # (  # query 11 - obligate percentage value
        #     "1/1",
        #     FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100),
        #     None,
        #     1.0,
        # ),
        # (  # query 12 - still an obligate percentage value
        #     "2/2",
        #     FrequencyHpoTerm(curie="HP:0040280", name="Obligate", lower=100, upper=100),
        #     None,
        #     1.0,
        # ),
        # (   # query 13 - frequent percentage value
        #     "1/2", FrequencyHpoTerm(curie="HP:0040282", name="Frequent", lower=30, upper=79), None, 0.5
        # ),
    ],
)
def test_phenotype_frequency_to_hpo_term(query: Optional[str], frequency: Optional[Frequency]):
    result: Optional[Frequency]  = phenotype_frequency_to_hpo_term(query)
    assert result == frequency
