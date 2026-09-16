import json
from collections.abc import Iterator
from dataclasses import FrozenInstanceError
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

import pytest
import pandas as pd
from requests import HTTPError

from translator_ingest.ingests.gtopdb.gtopdb import (
    GTOPDB_VERSION_PATTERN,
    TargetClassification,
    TargetDescriptor,
    _load_ligand_mapping,
    _publication_list,
    get_latest_version,
    multi_species_source_target_ids,
    prepare,
    source_target_species_descriptors,
    transform_ingest_all,
)
from translator_ingest.ingests.gtopdb.rules import (
    ACTIVATION,
    AGONISM,
    ANTAGONISM,
    INHIBITION,
    NEUTRAL_PHYSICAL,
    RELATED,
    RULES,
    SKIP,
    TYPE_FALLBACKS,
    resolve_rule,
)

from biolink_model.datamodel.pydanticmodel_v2 import (
    Association,
    ChemicalAffectsGeneAssociation,
    MacromolecularComplex,
    PairwiseMolecularInteraction,
    Protein,
    GeneOrGeneProductOrChemicalEntityAspectEnum,
    DirectionQualifierEnum,
    CausalMechanismQualifierEnum,
    KnowledgeLevelEnum,
    AgentTypeEnum,
    RetrievalSource,
    ResourceRoleEnum,
)

GTOPDB_SOURCES = [
    RetrievalSource(
        id="infores:gtopdb",
        resource_id="infores:gtopdb",
        resource_role=ResourceRoleEnum.primary_knowledge_source,
    )
]


def test_target_descriptor_preserves_source_identity_and_components():
    target = TargetDescriptor.from_record(
        {
            "target_id": "378",
            "target_name": "5-HT3AB",
            "target_species": "Human",
            "target_subunit_ids": "373|374",
            "target_gene_symbols": "HTR3A|HTR3B",
            "target_uniprot_ids": "P46098|O95264",
        }
    )

    assert target.source_id == "378"
    assert target.species == "Human"
    assert target.subunit_ids == ("373", "374")
    assert target.gene_symbols == ("HTR3A", "HTR3B")
    assert target.uniprot_ids == ("P46098", "O95264")
    assert target.classification is TargetClassification.MACROMOLECULAR_COMPLEX
    assert target.complex_curie == "IUPHARobj:378"


@pytest.mark.parametrize(
    ("record", "classification"),
    [
        (
            {
                "target_id": "378",
                "target_name": "5-HT3AB",
                "target_species": "Human",
                "target_subunit_ids": "373|374",
                "target_gene_symbols": "HTR3A|HTR3B",
                "target_uniprot_ids": "P46098|O95264",
            },
            TargetClassification.MACROMOLECULAR_COMPLEX,
        ),
        (
            {
                "target_id": "2903",
                "target_name": "claudin 18",
                "target_species": "Human",
                "target_subunit_ids": "",
                "target_gene_symbols": "CLDN18",
                "target_uniprot_ids": "P56856|P56856-2",
            },
            TargetClassification.SINGLE_PROTEIN,
        ),
        (
            {
                "target_id": "34",
                "target_name": "AT1 receptor",
                "target_species": "Rat",
                "target_subunit_ids": "",
                "target_gene_symbols": "Agtr1b|Agtr1a",
                "target_uniprot_ids": "P29089|P25095",
            },
            TargetClassification.UNRESOLVED_MULTI_PROTEIN_GROUP,
        ),
        (
            {
                "target_id": "1287",
                "target_name": "Guanylyl cyclase",
                "target_species": "Bovine",
                "target_subunit_ids": "1288|1290",
                "target_gene_symbols": "",
                "target_uniprot_ids": "",
            },
            TargetClassification.MACROMOLECULAR_COMPLEX,
        ),
    ],
)
def test_target_descriptor_classifies_components_before_accession_cardinality(record, classification):
    """Classify complexes independently of multi-species and UniProt evidence."""
    target = TargetDescriptor.from_record(record)

    assert target.key == (record["target_id"], record["target_species"])
    assert target.classification is classification


def test_multi_species_target_detection_is_independent_of_complex_classification():
    """Identify source targets with distinct known species without flattening complexes."""
    human_complex = TargetDescriptor.from_record(
        {
            "target_id": "378",
            "target_name": "5-HT3AB",
            "target_species": "Human",
            "target_subunit_ids": "373|374",
            "target_gene_symbols": "HTR3A|HTR3B",
            "target_uniprot_ids": "P46098|O95264",
        }
    )
    mouse_complex = TargetDescriptor.from_record(
        {
            "target_id": "378",
            "target_name": "5-HT3AB",
            "target_species": "Mouse",
            "target_subunit_ids": "373|374",
            "target_gene_symbols": "Htr3a|Htr3b",
            "target_uniprot_ids": "P23979|Q9JHJ5",
        }
    )
    unknown_species = TargetDescriptor.from_record(
        {
            "target_id": "378",
            "target_name": "5-HT3AB",
            "target_species": "Unknown",
            "target_subunit_ids": "373|374",
            "target_gene_symbols": "",
            "target_uniprot_ids": "",
        }
    )

    assert human_complex.classification is TargetClassification.MACROMOLECULAR_COMPLEX
    assert mouse_complex.classification is TargetClassification.MACROMOLECULAR_COMPLEX
    assert multi_species_source_target_ids((human_complex, mouse_complex, unknown_species)) == frozenset({"378"})
    assert source_target_species_descriptors((human_complex, mouse_complex, unknown_species)) == {
        "378": {"human": human_complex, "mouse": mouse_complex}
    }


@pytest.mark.parametrize(
    ("type_value", "action_value", "polarity"),
    [
        ("Activator", "future action", "positive"),
        ("Inhibitor", "future action", "negative"),
    ],
)
def test_legacy_type_fallbacks_remain_explicit(type_value, action_value, polarity):
    rule = resolve_rule(type_value, action_value)

    assert rule is not None
    assert rule.polarity == polarity
    assert rule.mechanism is None
    assert not rule.physical_interaction
    assert not rule.skip


def test_unknown_type_action_pair_has_no_rule():
    assert resolve_rule("unknown type", "unknown action") is None


def test_canonical_rules_and_nested_lookup_are_the_registration_source():
    assert RULES["Activator"]["Activation"] is ACTIVATION
    assert RULES["Agonist"]["Agonist"] is AGONISM
    assert RULES["Agonist"]["Inverse agonist"] == resolve_rule("Agonist", "Inverse agonist")


def test_canonical_rules_are_immutable_and_mapping_inventory_is_explicit():
    canonical_rules = (
        ACTIVATION,
        AGONISM,
        ANTAGONISM,
        INHIBITION,
        SKIP,
        RELATED,
        NEUTRAL_PHYSICAL,
    )
    for rule in canonical_rules:
        with pytest.raises(FrozenInstanceError):
            rule.skip = True

    assert sum(len(actions) for actions in RULES.values()) == 77
    assert set(TYPE_FALLBACKS) == {"Activator", "Inhibitor"}


def test_prepare_preserves_source_target_fields(tmp_path):
    (tmp_path / "ligands.csv").write_text('"# GtoPdb Version: test"\n"Ligand ID","PubChem CID"\n"1","2244"\n')
    context = RecordingContext()
    context.input_files_dir = tmp_path
    source_record = {
        "Target": "5-HT3AB",
        "Target ID": "378",
        "Target Subunit IDs": "373|374",
        "Target Gene Symbol": "HTR3A|HTR3B",
        "Target UniProt ID": "P46098|O95264",
        "Target Species": "Human",
        "Ligand ID": "1",
        "Ligand": "example ligand",
        "Type": "Agonist",
        "Action": "Agonist",
        "Endogenous": "FALSE",
        "Ligand Context": "",
        "PubMed ID": "11489465",
    }

    prepared = list(prepare(context, [source_record]))

    assert len(prepared) == 1
    assert prepared[0]["target_id"] == "378"
    assert prepared[0]["target_name"] == "5-HT3AB"
    assert prepared[0]["target_species"] == "Human"
    assert prepared[0]["target_subunit_ids"] == "373|374"
    assert prepared[0]["target_gene_symbols"] == "HTR3A|HTR3B"
    assert prepared[0]["target_uniprot_ids"] == "P46098|O95264"


def test_prepare_aggregates_duplicate_rows_and_retains_null_qualifiers(tmp_path):
    (tmp_path / "ligands.csv").write_text('"# GtoPdb Version: test"\n"Ligand ID","PubChem CID"\n"1","2244"\n')
    context = RecordingContext()
    context.input_files_dir = tmp_path
    base = {
        "Target": "example target",
        "Target ID": "1",
        "Target Subunit IDs": "",
        "Target Gene Symbol": "GENE",
        "Target UniProt ID": "P08588",
        "Target Species": "Human",
        "Ligand ID": "1",
        "Ligand": "example ligand",
        "Type": None,
        "Action": None,
        "Endogenous": "FALSE",
        "Ligand Context": "",
        "PubMed ID": "123",
    }
    duplicate = base | {"PubMed ID": "123"}
    distinct_publication = base | {"PubMed ID": "456"}

    prepared = list(prepare(context, [base, duplicate, distinct_publication]))

    assert len(prepared) == 1
    assert prepared[0]["PubMed ID"] == "123|456"
    assert prepared[0]["target_id"] == "1"
    assert pd.isna(prepared[0]["Type"])
    assert pd.isna(prepared[0]["Action"])


def test_prepare_preserves_target_species_descriptors(tmp_path):
    """Keep source target descriptors separate when their species differ."""
    (tmp_path / "ligands.csv").write_text('"# GtoPdb Version: test"\n"Ligand ID","PubChem CID"\n"1","2244"\n')
    context = RecordingContext()
    context.input_files_dir = tmp_path
    base = {
        "Target": "5-HT3AB",
        "Target ID": "378",
        "Target Subunit IDs": "373|374",
        "Target Gene Symbol": "HTR3A|HTR3B",
        "Target UniProt ID": "P46098|O95264",
        "Target Species": "Human",
        "Ligand ID": "1",
        "Ligand": "example ligand",
        "Type": "Agonist",
        "Action": "Agonist",
        "Endogenous": "FALSE",
        "Ligand Context": "",
        "PubMed ID": "11489465",
    }
    mouse = base | {
        "Target Species": "Mouse",
        "Target Gene Symbol": "Htr3a|Htr3b",
        "Target UniProt ID": "P23979|Q9JHJ5",
    }

    prepared = list(prepare(context, [base, mouse]))

    assert {(record["target_id"], record["target_species"], record["target_uniprot_ids"]) for record in prepared} == {
        ("378", "Human", "P46098|O95264"),
        ("378", "Mouse", "P23979|Q9JHJ5"),
    }


def test_load_ligand_mapping_and_publication_list(tmp_path):
    (tmp_path / "ligands.csv").write_text('"# GtoPdb Version: test"\n"Ligand ID","PubChem CID"\n"1","2244"\n')

    assert _load_ligand_mapping(tmp_path) == {"1": "2244"}
    assert _publication_list("123|456") == ["PMID:123", "PMID:456"]
    assert _publication_list("") is None


@pytest.fixture
def interactions_url(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    """Serve a local interactions file through real HTTP, overriding only its URL."""
    handler = partial(SimpleHTTPRequestHandler, directory=str(tmp_path))
    with ThreadingHTTPServer(("127.0.0.1", 0), handler) as server:
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        url = f"http://127.0.0.1:{server.server_port}/interactions.csv"
        monkeypatch.setattr("translator_ingest.ingests.gtopdb.gtopdb.GTOPDB_INTERACTIONS_URL", url)
        try:
            yield url
        finally:
            server.shutdown()
            thread.join()


@pytest.mark.parametrize(
    "content,expected",
    [
        ('"# GtoPdb Version: 2026.2 - published: 2026-06-15"\nTarget,Target ID\n', "2026.2"),
        ("# GtoPdb Version:2026.10 - published: 2026-11-02\n", "2026.10"),
    ],
)
def test_get_latest_version_reads_interactions_header(
    tmp_path: Path, interactions_url: str, content: str, expected: str
) -> None:
    """Read the metadata from the first line using the actual requests client."""
    (tmp_path / "interactions.csv").write_text(content)
    assert get_latest_version() == expected


@pytest.mark.parametrize(
    "content", ["", "Target,Target ID\n# GtoPdb Version: 2026.2\n", "<b>Downloads are from the 2026.2 version.</b>"]
)
def test_get_latest_version_rejects_missing_header(tmp_path: Path, interactions_url: str, content: str) -> None:
    """A missing first-line version cannot fall back to old HTML or later lines."""
    (tmp_path / "interactions.csv").write_text(content)
    with pytest.raises(RuntimeError, match="Could not parse the GtoPdb version from the first line"):
        get_latest_version()


def test_get_latest_version_rejects_http_error(interactions_url: str) -> None:
    """An unsuccessful download retains the upstream HTTP error behavior."""
    with pytest.raises(HTTPError, match="404"):
        get_latest_version()


class RecordingContext:
    def __init__(self):
        self.messages = []

    def log(self, message: str, level: str = "INFO") -> None:
        self.messages.append((level, message))


def _edge_signature(edge):
    return {
        "class": type(edge).__name__,
        "subject": edge.subject,
        "predicate": edge.predicate,
        "object": edge.object,
        "qualified_predicate": getattr(edge, "qualified_predicate", None),
        "object_aspect_qualifier": getattr(edge, "object_aspect_qualifier", None),
        "object_direction_qualifier": getattr(edge, "object_direction_qualifier", None),
        "causal_mechanism_qualifier": getattr(edge, "causal_mechanism_qualifier", None),
        "publications": getattr(edge, "publications", None),
    }


INTERACTION_RULE_GOLDEN = json.loads((Path(__file__).parent / "interaction_rule_golden.json").read_text())


@pytest.mark.parametrize(
    "case",
    INTERACTION_RULE_GOLDEN,
    ids=lambda case: f"{case['type']}:{case['action']}:{case['endogenous']}",
)
def test_transform_matches_current_source_rule_behavior(case):
    """Freeze 2026.2 behavior before replacing the Type/Action conditional forest."""
    record = {
        "subject_id": "2244",
        "subject_name": "example ligand",
        "target_id": "1",
        "target_name": "example target",
        "target_species": "Human",
        "target_subunit_ids": "",
        "target_gene_symbols": "GENE",
        "target_uniprot_ids": "P08588",
        "Type": case["type"],
        "Action": case["action"],
        "Endogenous": case["endogenous"],
        "PubMed ID": "123|456",
    }

    graph = transform_ingest_all(RecordingContext(), [record])[0]

    assert [_edge_signature(edge) for edge in graph.edges] == case["edges"]


def test_transform_emits_source_defined_complex_targets():
    context = RecordingContext()
    record = {
        "subject_id": "2244",
        "subject_name": "example ligand",
        "object_id": "P46098|O95264",
        "object_name": "5-HT3AB",
        "target_id": "378",
        "target_name": "5-HT3AB",
        "target_species": "Human",
        "target_subunit_ids": "373|374",
        "target_gene_symbols": "HTR3A|HTR3B",
        "target_uniprot_ids": "P46098|O95264",
        "Type": "Agonist",
        "Action": "Agonist",
        "Endogenous": "FALSE",
        "PubMed ID": "11489465",
    }

    graph = transform_ingest_all(context, [record])[0]

    assert {node.id for node in graph.nodes} == {
        "PUBCHEM.COMPOUND:2244",
        "IUPHARobj:378",
        "UniProtKB:P46098",
        "UniProtKB:O95264",
    }
    assert isinstance(
        next(node for node in graph.nodes if node.id == "IUPHARobj:378"),
        MacromolecularComplex,
    )
    assert all(isinstance(node, Protein) for node in graph.nodes if node.id in {"UniProtKB:P46098", "UniProtKB:O95264"})
    assert {(edge.subject, edge.predicate, edge.object) for edge in graph.edges} == {
        ("PUBCHEM.COMPOUND:2244", "biolink:affects", "IUPHARobj:378"),
        (
            "PUBCHEM.COMPOUND:2244",
            "biolink:directly_physically_interacts_with",
            "IUPHARobj:378",
        ),
        ("IUPHARobj:378", "biolink:has_part", "UniProtKB:P46098"),
        ("IUPHARobj:378", "biolink:has_part", "UniProtKB:O95264"),
    }
    assert {
        edge.predicate: edge.species_context_qualifier for edge in graph.edges if edge.predicate != "biolink:has_part"
    } == {
        "biolink:affects": "NCBITaxon:9606",
        "biolink:directly_physically_interacts_with": "NCBITaxon:9606",
    }
    assert all(node.in_taxon == ["NCBITaxon:9606"] for node in graph.nodes if node.id != "PUBCHEM.COMPOUND:2244")
    assert context.messages == []


def test_transform_omits_components_for_multi_species_source_complexes():
    """Avoid merging human and mouse target 378 components under IUPHARobj:378."""
    human = {
        "subject_id": "2244",
        "subject_name": "example ligand",
        "target_id": "378",
        "target_name": "5-HT3AB",
        "target_species": "Human",
        "target_subunit_ids": "373|374",
        "target_gene_symbols": "HTR3A|HTR3B",
        "target_uniprot_ids": "P46098|O95264",
        "Type": "Agonist",
        "Action": "Agonist",
        "Endogenous": "FALSE",
        "PubMed ID": "11489465",
    }
    mouse = human | {
        "target_species": "Mouse",
        "target_gene_symbols": "Htr3a|Htr3b",
        "target_uniprot_ids": "P23979|Q9JHJ5",
    }

    graph = transform_ingest_all(RecordingContext(), [human, mouse])[0]

    assert {node.id for node in graph.nodes} == {
        "PUBCHEM.COMPOUND:2244",
        "IUPHARobj:378",
    }
    assert all(edge.predicate != "biolink:has_part" for edge in graph.edges)
    assert {edge.species_context_qualifier for edge in graph.edges if edge.predicate != "biolink:has_part"} == {
        "NCBITaxon:9606",
        "NCBITaxon:10090",
    }


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
