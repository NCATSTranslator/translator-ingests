import pytest

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsGeneAssociation,
    PairwiseMolecularInteraction,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    CausalMechanismQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)

from translator_ingest.ingests.gtopdb.gtopdb import (
    GTOPDB_VERSION_PATTERN,
    get_latest_version,
)


GTOPDB_SOURCES = [
    RetrievalSource(
        id="infores:gtopdb",
        resource_id="infores:gtopdb",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per edge type declared in gtopdb_rig.yaml / gtopdb.py ────
EDGE_FIXTURES = [
    {
        "association_class": ChemicalAffectsGeneAssociation,
        "params": {
            "id": "uuid:gtopdb-chem-affects-gene",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:affects",
            "object": "UniProtKB:P08588",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": GeneOrGeneProductOrChemicalEntityAspectEnum.activity,
            "object_direction_qualifier": DirectionQualifierEnum.increased,
            "causal_mechanism_qualifier": CausalMechanismQualifierEnum.agonism,
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
            "publications": ["PMID:12345678"],
        },
    },
    {
        "association_class": PairwiseMolecularInteraction,
        "params": {
            "id": "uuid:gtopdb-pairwise-interaction",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:directly_physically_interacts_with",
            "object": "UniProtKB:P08588",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
        },
    },
    {
        "association_class": Association,
        "params": {
            "id": "uuid:gtopdb-generic-related",
            "subject": "PUBCHEM.COMPOUND:5311",
            "predicate": "biolink:related_to",
            "object": "UniProtKB:Q14416",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": GTOPDB_SOURCES,
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


# ── Version parsing ───────────────────────────────────────────────────────
@pytest.mark.parametrize(
    "first_line,expected",
    [
        ('"# GtoPdb Version: 2026.2 - published: 2026-06-15"', "2026.2"),
        ("# GtoPdb Version: 2025.4 - published: 2025-12-01", "2025.4"),
        ('"# GtoPdb Version:2026.10 - published: 2026-11-02"', "2026.10"),
    ],
)
def test_gtopdb_version_pattern(first_line: str, expected: str):
    """The version is parsed from the metadata comment on the first line of the data files."""
    match = GTOPDB_VERSION_PATTERN.search(first_line)
    assert match is not None
    assert match.group("version") == expected


@pytest.mark.parametrize("first_line", ["", '"Target","Target ID"', "# some other comment"])
def test_gtopdb_version_pattern_no_match(first_line: str):
    """Lines without the metadata comment do not yield a version."""
    assert GTOPDB_VERSION_PATTERN.search(first_line) is None


# Network-dependent: streams the first line of guidetopharmacology.org's interactions.csv.
# Skipped to keep CI hermetic, matching the convention in test_panther.py.
@pytest.mark.skip(reason="hits guidetopharmacology.org; run manually to verify the version metadata line")
def test_get_latest_version_live():
    version = get_latest_version()
    major, _, minor = version.partition(".")
    assert major.isdigit() and minor.isdigit()
