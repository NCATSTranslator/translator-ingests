import pytest
from biolink_model.datamodel.pydanticmodel_v2 import (
    AgentTypeEnum,
    Gene,
    KnowledgeLevelEnum,
    PairwiseMolecularInteraction,
    RetrievalSource,
    ResourceRoleEnum,
)
from koza.runner import KozaRunner, KozaTransformHooks

from tests.unit.ingests import MockKozaWriter
from translator_ingest.ingests.intact.intact import (
    extract_curie,
    get_primary_identifier,
    parse_psi_mi_field,
    transform_record,
)

INTACT_SOURCES = [
    RetrievalSource(
        id="infores:intact",
        resource_id="infores:intact",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]

# ── Fixtures: one per edge type declared in intact_rig.yaml / intact.py ────
EDGE_FIXTURES = [
    {
        "association_class": PairwiseMolecularInteraction,
        "params": {
            "id": "uuid:intact-ppi",
            "subject": "UniProtKB:P04637",
            "predicate": "biolink:physically_interacts_with",
            "object": "UniProtKB:Q00987",
            "knowledge_level": KnowledgeLevelEnum.knowledge_assertion,
            "agent_type": AgentTypeEnum.manual_agent,
            "sources": INTACT_SOURCES,
            "publications": ["PMID:9029145"],
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


@pytest.mark.parametrize("database", ["entrezgene/locuslink", "entrez gene/locuslink"])
def test_extract_curie_normalizes_entrez_gene_aliases(database: str) -> None:
    """Both PSI-MI Entrez namespace spellings normalize to the Biolink prefix."""
    parsed = parse_psi_mi_field(f"{database}:5359(identity)")

    assert extract_curie(parsed) == "NCBIGene:5359"


@pytest.mark.parametrize("database", ["entrezgene/locuslink", "entrez gene/locuslink"])
@pytest.mark.parametrize("identifier_field", ["primary", "alternative"])
def test_get_primary_identifier_classifies_entrez_gene_aliases(database: str, identifier_field: str) -> None:
    """Entrez identifiers are genes whether they occur in primary or alternative IDs."""
    id_field = f"{database}:5359(identity)" if identifier_field == "primary" else "-"
    alt_ids_field = f"{database}:5359(identity)" if identifier_field == "alternative" else "-"

    assert get_primary_identifier(id_field, alt_ids_field) == ("NCBIGene:5359", "gene")


@pytest.mark.parametrize(
    ("id_field", "alt_ids_field", "expected"),
    [
        ("-", "-", (None, None)),
        ("uniprotkb:P04637", "entrezgene/locuslink:7157", ("UniProtKB:P04637", "protein")),
        ("chebi:15377", "entrezgene/locuslink:5359", ("CHEBI:15377", "small_molecule")),
        ("entrezgene/locuslink:7157", "uniprotkb:P04637", ("UniProtKB:P04637", "protein")),
        ("entrezgene/locuslink:5359", "chebi:15377", ("CHEBI:15377", "small_molecule")),
    ],
)
def test_get_primary_identifier_preserves_missing_and_preferred_identifiers(
    id_field: str,
    alt_ids_field: str,
    expected: tuple[str | None, str | None],
) -> None:
    """Adding an Entrez alias does not change missing-ID or protein/chemical behavior."""
    assert get_primary_identifier(id_field, alt_ids_field) == expected


def test_transform_record_emits_ncbigene_node_for_intact_entrez_identifier() -> None:
    """Exercise the Koza transform with the compact namespace emitted by IntAct.

    The compact identifier occurs in the xrefs of IntAct record EBI-10379036;
    placing it in the identifier field reproduces the reported output path.
    """
    record = {
        "idA": "entrezgene/locuslink:5359(identity)",
        "idB": "uniprotkb:O15162",
        "altIdsA": "-",
        "altIdsB": "-",
        "aliasesA": "uniprotkb:PLSCR1(gene name)",
        "aliasesB": "uniprotkb:PLSCR3(gene name)",
        "interactionDetectionMethod": 'psi-mi:"MI:0006"(anti bait coimmunoprecipitation)',
        "publicationIDs": "pubmed:11877448",
        "taxidA": "taxid:9606(human)",
        "taxidB": "taxid:9606(human)",
        "interactionTypes": 'psi-mi:"MI:0915"(physical association)',
    }
    writer = MockKozaWriter()
    runner = KozaRunner(
        data=iter([record]),
        writer=writer,
        hooks=KozaTransformHooks(transform_record=[transform_record]),
    )

    runner.run()

    genes = [entity for entity in writer.items if isinstance(entity, Gene)]
    assert len(writer.items) == 3
    assert len(genes) == 1
    assert genes[0].id == "NCBIGene:5359"
    assert genes[0].category == ["biolink:Gene"]
