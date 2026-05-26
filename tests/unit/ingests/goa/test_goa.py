from pathlib import Path

import pytest
import yaml

from biolink_model.datamodel.pydanticmodel_v2 import (
    GeneToGoTermAssociation,
    Association,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)
from translator_ingest.ingests.goa.goa import (
    ASPECT_TO_PREDICATE,
    QUALIFIER_TO_PREDICATE,
    get_supporting_data_sources,
)


GOA_RIG_PATH = (
    Path(__file__).resolve().parents[4]
    / "src"
    / "translator_ingest"
    / "ingests"
    / "goa"
    / "goa_rig.yaml"
)


def _get_goa_rig_predicates() -> set[str]:
    """Return all edge predicates declared in the GOA RIG."""
    with GOA_RIG_PATH.open(encoding="utf-8") as stream:
        rig = yaml.safe_load(stream)
    edge_type_info = rig["target_info"]["edge_type_info"]
    return {predicate for edge_type in edge_type_info for predicate in edge_type["predicates"]}


def test_goa_predicates_in_code_match_rig() -> None:
    """Ensure GOA transform predicates align with GOA RIG edge predicate definitions."""
    rig_predicates = _get_goa_rig_predicates()
    code_predicates = set(QUALIFIER_TO_PREDICATE.values())
    assert code_predicates == rig_predicates


@pytest.mark.parametrize(
    ("qualifier", "expected_predicate"),
    [
        ("involved_in", "biolink:involved_in"),
        ("enables", "biolink:enables"),
        ("located_in", "biolink:located_in"),
        ("is_active_in", "biolink:active_in"),
        ("active_in", "biolink:active_in"),
    ],
)
def test_goa_qualifier_mapping(qualifier: str, expected_predicate: str) -> None:
    """Check key qualifier mappings that are explicitly documented in GOA RIG."""
    assert QUALIFIER_TO_PREDICATE[qualifier] == expected_predicate


def test_goa_aspect_fallback_for_biological_process() -> None:
    """Ensure biological process fallback predicate matches GOA RIG."""
    assert ASPECT_TO_PREDICATE["P"] == "biolink:involved_in"


@pytest.mark.parametrize(
    ("assigned_by", "expected_supporting"),
    [
        ("MGI", ["infores:mgi"]),
        ("RGD", ["infores:rgd"]),
        ("Reactome", ["infores:reactome"]),
        ("IntAct", ["infores:intact"]),
        ("GO_Central", ["infores:go-cam"]),
        ("GOC", ["infores:go-cam"]),
        ("UniProt", None),
        ("", None),
        (None, None),
    ],
)
def test_goa_assigned_by_to_supporting_source_mapping(
    assigned_by: str | None, expected_supporting: list[str] | None
) -> None:
    """Ensure Assigned_By provenance is mapped to supporting sources when available."""
    assert get_supporting_data_sources(assigned_by) == expected_supporting


# ── Pydantic round-trip fixtures & test ──────────────────────────────

_GOA_SOURCES = [
    RetrievalSource(
        id="infores:goa",
        resource_id="infores:goa",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

EDGE_FIXTURES = [
    {
        "association_class": GeneToGoTermAssociation,
        "params": {
            "id": "uuid:goa-test-1",
            "subject": "MGI:101757",
            "predicate": "biolink:enables",
            "object": "GO:0005515",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": _GOA_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:goa-test-2",
            "subject": "UniProtKB:A0A024RBG1",
            "predicate": "biolink:enables",
            "object": "GO:0003700",
            "knowledge_level": KnowledgeLevelEnum.prediction,
            "agent_type": AgentTypeEnum.automated_agent,
            "sources": _GOA_SOURCES,
        },
    },
]


@pytest.mark.parametrize(
    "fixture",
    EDGE_FIXTURES,
    ids=lambda f: f["association_class"].__name__,
)
def test_pydantic_roundtrip(fixture):
    """Instantiate the association and round-trip through Pydantic serialization."""
    cls = fixture["association_class"]
    obj = cls(**fixture["params"])
    dumped = obj.model_dump()
    restored = cls.model_validate(dumped)
    assert restored == obj
