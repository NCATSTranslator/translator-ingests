"""
Tests for the GO-CAM RO -> Biolink edge shape table.

The table is a static map, so these tests check the contract it has to hold: everything
it emits is a real Biolink predicate, nothing is silently degraded to related_to, and the
qualified-statement parts are only ever emitted as a coherent set.
"""

import doctest

import pytest

from translator_ingest.ingests.go_cam import ro_predicate_mapping
from translator_ingest.ingests.go_cam.ro_predicate_mapping import (
    RO_PREDICATE_MAP,
    map_causal_predicate,
    normalize_ro_curie,
)
from translator_ingest.util.biolink import get_biolink_model_toolkit

# One Toolkit per module, reused across tests - constructing it is expensive.
#
# get_biolink_model_toolkit() builds the Toolkit from the *installed* biolink_model
# schema. A bare Toolkit() would load whatever schema version bmt happens to default to,
# which is not necessarily the version the pydantic classes were generated from, and the
# two disagree about which qualifiers a class permits.
TOOLKIT = get_biolink_model_toolkit()

MAPPED = [(ro_id, mapping) for ro_id, mapping in RO_PREDICATE_MAP.items() if mapping.predicate]
DROPPED = [(ro_id, mapping) for ro_id, mapping in RO_PREDICATE_MAP.items() if not mapping.predicate]


def test_doctests():
    results = doctest.testmod(ro_predicate_mapping)
    assert results.failed == 0


def test_table_is_populated():
    """An empty table would drop every edge in the ingest."""
    assert len(MAPPED) >= 15


@pytest.mark.parametrize("ro_id,mapping", MAPPED, ids=[r for r, _ in MAPPED])
def test_every_emitted_predicate_is_a_biolink_predicate(ro_id, mapping):
    """
    Nothing in this table may emit a raw ontology term as the edge predicate.

    RO and BFO CURIEs belong in original_predicate, never in predicate.
    """
    assert mapping.predicate.startswith("biolink:"), f"{ro_id} emits a non-biolink predicate"
    name = mapping.predicate.removeprefix("biolink:").replace("_", " ")
    assert TOOLKIT.get_element(name) is not None, f"{mapping.predicate} is not in biolink-model"
    assert TOOLKIT.is_predicate(name), f"{mapping.predicate} is not a biolink predicate"


@pytest.mark.parametrize("ro_id,mapping", MAPPED, ids=[r for r, _ in MAPPED])
def test_no_predicate_is_degraded_to_related_to(ro_id, mapping):
    """
    related_to carries no reasoning signal, so it is never a mapping target.

    An edge we cannot map is dropped and counted instead - absence is measurable in a
    way that a graph full of related_to edges is not.
    """
    assert mapping.predicate != "biolink:related_to"


@pytest.mark.parametrize("ro_id,mapping", RO_PREDICATE_MAP.items(), ids=list(RO_PREDICATE_MAP))
def test_every_row_records_its_derivation(ro_id, mapping):
    """Each row must say where it came from, including the rows that drop edges."""
    assert mapping.provenance
    assert mapping.ro_label or mapping.provenance == "obsolete in RO"


@pytest.mark.parametrize("ro_id,mapping", RO_PREDICATE_MAP.items(), ids=list(RO_PREDICATE_MAP))
def test_direction_values_are_valid(ro_id, mapping):
    """Direction, when present, must be a biolink DirectionQualifierEnum value."""
    from biolink_model.datamodel.pydanticmodel_v2 import DirectionQualifierEnum

    if mapping.direction is not None:
        assert mapping.direction in {v.value for v in DirectionQualifierEnum}


@pytest.mark.parametrize(
    "ro_id,predicate,direction",
    [
        # Regulatory predicates resolve through RO's gene-product projection
        # (enables o P o "enabled by"), whose target maps exactly to biolink:regulates.
        ("RO:0002629", "biolink:regulates", "increased"),  # directly positively regulates
        ("RO:0002630", "biolink:regulates", "decreased"),  # directly negatively regulates
        ("RO:0002407", "biolink:regulates", "increased"),  # indirectly positively regulates
        ("RO:0002409", "biolink:regulates", "decreased"),  # indirectly negatively regulates
        ("RO:0002213", "biolink:regulates", "increased"),  # positively regulates
        ("RO:0002578", "biolink:regulates", None),  # directly regulates, no direction
        # The causally-upstream family has no gene-product projection, so it resolves
        # directly; RO:0002411 is a narrow mapping of biolink:precedes. None of these
        # carries a direction - RO annotates several of them as positive or negative
        # forms, but that effect is on the execution of a downstream process, not on an
        # activity of the object gene, so there is no aspect to attach it to.
        ("RO:0002411", "biolink:precedes", None),
        ("RO:0002412", "biolink:precedes", None),
        ("RO:0002304", "biolink:precedes", None),
        ("RO:0002305", "biolink:precedes", None),
        ("RO:0012009", "biolink:precedes", None),  # constitutively upstream of
        ("RO:0012010", "biolink:precedes", None),  # removes input for
        ("RO:0002413", "biolink:precedes", None),  # provides input for
        # Exact mappings that need no projection
        ("BFO:0000050", "biolink:part_of", None),
        ("BFO:0000051", "biolink:has_part", None),
        ("RO:0002233", "biolink:has_input", None),
        ("RO:0002333", "biolink:enabled_by", None),
        ("RO:0002215", "biolink:capable_of", None),
    ],
)
def test_known_mappings(ro_id, predicate, direction):
    mapping = map_causal_predicate(ro_id)
    assert mapping is not None, f"{ro_id} missing from the table"
    assert mapping.predicate == predicate
    assert mapping.direction == direction


@pytest.mark.parametrize(
    "ro_id,reason",
    [
        ("RO:0002408", "obsolete in RO"),  # obsolete directly inhibits
        ("RO:0002418", "no sound biolink mapping"),  # causally upstream of or within
        ("RO:0002332", "no sound biolink mapping"),  # regulates levels of
        ("RO:0004046", "no sound biolink mapping"),
        ("RO:0004047", "no sound biolink mapping"),
        ("RO:0002614", "no sound biolink mapping"),  # is evidence with support from
    ],
)
def test_known_drops(ro_id, reason):
    """Predicates with no sound target are listed with a reason, not omitted."""
    mapping = map_causal_predicate(ro_id)
    assert mapping is not None, f"{ro_id} should be listed so the drop can be explained"
    assert mapping.predicate is None
    assert mapping.provenance == reason


def test_unknown_predicate_is_distinguishable_from_a_known_drop():
    """A predicate new to a future RO release is unknown, not known-unmappable."""
    assert map_causal_predicate("RO:9999999") is None
    assert map_causal_predicate(None) is None


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("RO:0002629", "RO:0002629"),
        ("obo:RO#RO_0002629", "RO:0002629"),
        ("http://purl.obolibrary.org/obo/RO_0002629", "RO:0002629"),
        ("http://purl.obolibrary.org/obo/BFO_0000050", "BFO:0000050"),
        ("  RO:0002629  ", "RO:0002629"),
    ],
)
def test_normalize_ro_curie(raw, expected):
    assert normalize_ro_curie(raw) == expected


def test_occurs_in_maps_to_the_biolink_exact_mapping():
    """
    BFO:0000066 "occurs in" is the Biolink exact mapping of biolink:occurs_in.

    It appears on one edge, and only became visible once rows carrying several causal
    predicates were split rather than resolved to the first.
    """
    mapping = map_causal_predicate("BFO:0000066")
    assert mapping.predicate == "biolink:occurs_in"
    assert mapping.qualified_predicate is None
    assert mapping.object_aspect is None
    assert mapping.direction is None
