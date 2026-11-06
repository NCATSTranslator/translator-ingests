"""Interaction behavior for single proteins and source-defined complexes.

Expectations use the checked-in inventory, never production rules. Reviewed
corrections cover physical-only interactions and independent edge qualifiers.
Full agonism intentionally maps to agonism, voltage-dependent inhibition to
gating_inhibition, and both Activator + Activation and Agonist + Activation to
agonism with a physical edge. Inhibitor + Antagonist maps to inhibition. The
review decisions for Activator + Activation and Inhibitor + Antagonist supersede
the older mapping-sheet entries.
Non-competitive inhibition uses Biolink's noncompetitive_inhibition term;
Antibody + Agonist follows the reviewed decreased/downregulated direction.
Irreversible agonism deliberately retains the original ingest's agonism fallback
because Biolink has no irreversible_agonism mechanism.

Source-defined complexes retain these rules on the complex target. Unresolved
multi-protein groups and unmapped targets remain excluded.
"""

import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
from koza import KozaTransform
from koza.io.writer.jsonl_writer import JSONLWriter
from koza.model.graphs import KnowledgeGraph
from koza.model.writer import WriterConfig
from pydantic import ValidationError

from translator_ingest.ingests.gtopdb.gtopdb import transform_ingest_all

FIXTURES = Path(__file__).parent
GOLDEN_CASES = json.loads((FIXTURES / "interaction_rule_golden.json").read_text())
EXPECTED_EDGES = {(case["type"], case["action"], case["endogenous"]): case["edges"] for case in GOLDEN_CASES}
TYPE_LABELS = (
    "Activator",
    "Agonist",
    "Allosteric modulator",
    "Antagonist",
    "Antibody",
    "Channel blocker",
    "Fusion protein",
    "Gating inhibitor",
    "Inhibitor",
    "None",
    "Subunit-specific",
)
ACTION_LABELS = (
    "Activation",
    "Agonist",
    "Antagonist",
    "Biased agonist",
    "Binding",
    "Biphasic",
    "Competitive",
    "Feedback inhibition",
    "Full agonist",
    "Inhibition",
    "Inverse agonist",
    "Irreversible agonist",
    "Irreversible inhibition",
    "Mixed",
    "Negative",
    "Neutral",
    "Non-competitive",
    "None",
    "Partial agonist",
    "Pore blocker",
    "Positive",
    "Potentiation",
    "Slows inactivation",
    "Unknown",
    "Voltage-dependent inhibition",
    "future action",
    "",
    "agonist",
    " Agonist ",
    None,
    float("nan"),
    pd.NA,
)
ENDOGENOUS_VALUES = ("TRUE", "FALSE", "", None, True, False, "true", "TRUE ", float("nan"))
RECORD: dict[str, Any] = {
    "subject_id": "2244",
    "subject_name": "example ligand",
    "target_id": "1",
    "target_name": "example target",
    "target_species": "Human",
    "target_subunit_ids": "",
    "target_gene_symbols": "GENE",
    "target_uniprot_ids": "P08588",
    "Type": "Agonist",
    "Action": "Agonist",
    "Endogenous": "FALSE",
    "PubMed ID": "123|456",
}
SOURCES = [
    {
        "id": "infores:gtopdb",
        "category": ["biolink:RetrievalSource"],
        "resource_id": "infores:gtopdb",
        "resource_role": "primary_knowledge_source",
    }
]


@pytest.fixture
def context(tmp_path: Path) -> Iterator[KozaTransform]:
    """Use real Koza state and a local JSONL writer for every contract test."""
    writer = JSONLWriter(str(tmp_path / "output"), "gtopdb", WriterConfig())
    yield KozaTransform(extra_fields={}, writer=writer, mappings={}, input_files_dir=tmp_path)
    writer.finalize()


def expected_edges(type_value: str, action: Any, endogenous: Any) -> list[dict[str, Any]]:
    """Look up literal expectations, with the two independently specified defaults."""
    context_label = "TRUE" if endogenous == "TRUE" else "FALSE"
    key = (type_value, action, context_label)
    if key in EXPECTED_EDGES:
        return EXPECTED_EDGES[key]
    if type_value not in {"Activator", "Inhibitor"}:
        return []
    positive = type_value == "Activator"
    direction = (
        ("upregulated" if positive else "downregulated")
        if context_label == "TRUE"
        else ("increased" if positive else "decreased")
    )
    return [
        {
            "class": "ChemicalAffectsGeneAssociation",
            "subject": "PUBCHEM.COMPOUND:2244",
            "predicate": "biolink:regulates" if context_label == "TRUE" else "biolink:affects",
            "object": "UniProtKB:P08588",
            "qualified_predicate": "biolink:causes",
            "object_aspect_qualifier": "activity",
            "object_direction_qualifier": direction,
            "causal_mechanism_qualifier": None,
            "publications": ["PMID:123", "PMID:456"],
        }
    ]


def graph_signature(graph: KnowledgeGraph) -> dict[str, list[dict[str, Any]]]:
    """Compare all serialized fields and model classes, excluding edge UUIDs only."""
    return {
        "nodes": [{"class": type(node).__name__, "data": node.model_dump(mode="json")} for node in graph.nodes],
        "edges": [
            {"class": type(edge).__name__, "data": edge.model_dump(mode="json", exclude={"id"})} for edge in graph.edges
        ],
    }


@pytest.mark.parametrize("type_value", TYPE_LABELS)
@pytest.mark.parametrize("action", ACTION_LABELS)
@pytest.mark.parametrize("endogenous", ENDOGENOUS_VALUES)
def test_type_action_context_matrix(context: KozaTransform, type_value: str, action: Any, endogenous: Any) -> None:
    """Characterize exact pairs, skips, fallbacks, and non-TRUE endogenous values."""
    graphs = list(
        transform_ingest_all(
            context,
            iter(
                [
                    RECORD
                    | {
                        "Type": type_value,
                        "Action": action,
                        "Endogenous": endogenous,
                    }
                ]
            ),
        )
    )
    assert len(graphs) == 1
    graph = graphs[0]
    expected = expected_edges(type_value, action, endogenous)
    assert len(graph.edges) == len(expected)
    assert [(type(node).__name__, node.id, node.name) for node in graph.nodes] == (
        [
            ("ChemicalEntity", "PUBCHEM.COMPOUND:2244", "example ligand"),
            ("Protein", "UniProtKB:P08588", "example target"),
        ]
        if expected
        else []
    )
    assert len({edge.id for edge in graph.edges}) == len(graph.edges)
    for edge, fields in zip(graph.edges, expected, strict=True):
        assert {key: type(edge).__name__ if key == "class" else getattr(edge, key, None) for key in fields} == fields
        assert [source.model_dump(mode="json", exclude_none=True) for source in edge.sources] == SOURCES
        assert edge.knowledge_level == "knowledge_assertion"
        assert edge.agent_type == "manual_agent"
        if edge.predicate == "biolink:related_to":
            # Biolink's base Association has no species-context slot.
            assert "species_context_qualifier" not in type(edge).model_fields
            assert not hasattr(edge, "species_context_qualifier")
        else:
            assert "species_context_qualifier" in type(edge).model_fields
            assert edge.species_context_qualifier == "NCBITaxon:9606"
    if expected:
        assert graph.nodes[1].in_taxon == ["NCBITaxon:9606"]


@pytest.mark.parametrize(
    "type_value,action,mechanism",
    [
        ("Inhibitor", "Binding", "inhibition"),
        ("Inhibitor", "Antagonist", "inhibition"),
        ("Antagonist", "Binding", "antagonism"),
        ("Inhibitor", "Non-competitive", "noncompetitive_inhibition"),
        ("Antagonist", "Non-competitive", "non_competitive_antagonism"),
        ("Antibody", "Agonist", "antibody_agonism"),
        ("Antibody", "Antagonist", "antibody_inhibition"),
    ],
)
@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [
        ("FALSE", "biolink:affects", "decreased"),
        ("TRUE", "biolink:regulates", "downregulated"),
    ],
)
def test_inhibitory_mappings_match_reviewed_specification(
    context: KozaTransform,
    type_value: str,
    action: str,
    mechanism: str,
    endogenous: str,
    predicate: str,
    direction: str,
) -> None:
    """Apply the reviewed mechanisms and negative directions in both contexts.

    Non-competitive inhibition uses Biolink's spelling, without an underscore
    between non and competitive. Antibody + Agonist has a negative direction
    in the reviewed specification despite retaining its antibody_agonism mechanism:
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142
    """
    record = RECORD | {"Type": type_value, "Action": action, "Endogenous": endogenous}
    graph = list(transform_ingest_all(context, [record]))[0]

    assert len(graph.edges) == 2
    effect, physical = graph.edges
    assert effect.predicate == predicate
    assert effect.qualified_predicate == "biolink:causes"
    assert effect.object_aspect_qualifier == "activity"
    assert effect.object_direction_qualifier == direction
    assert effect.causal_mechanism_qualifier == mechanism
    assert physical.predicate == "biolink:directly_physically_interacts_with"
    assert effect.subject == physical.subject == "PUBCHEM.COMPOUND:2244"
    assert effect.object == physical.object == "UniProtKB:P08588"
    assert effect.publications == physical.publications == ["PMID:123", "PMID:456"]
    assert type(effect).model_validate(effect.model_dump()) == effect
    assert type(physical).model_validate(physical.model_dump()) == physical


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [
        ("FALSE", "biolink:affects", "increased"),
        ("TRUE", "biolink:regulates", "upregulated"),
    ],
)
def test_irreversible_agonist_preserves_original_fallback(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """Retain general agonism and physical interaction without claiming irreversibility."""
    record = RECORD | {"Action": "Irreversible agonist", "Endogenous": endogenous}
    graph = list(transform_ingest_all(context, [record]))[0]

    assert len(graph.edges) == 2
    effect, physical = graph.edges
    assert effect.predicate == predicate
    assert effect.causal_mechanism_qualifier == "agonism"
    assert effect.qualified_predicate == "biolink:causes"
    assert effect.object_aspect_qualifier == "activity"
    assert effect.object_direction_qualifier == direction
    assert physical.predicate == "biolink:directly_physically_interacts_with"
    assert physical.causal_mechanism_qualifier is None
    assert physical.object_aspect_qualifier is None
    assert physical.object_direction_qualifier is None
    assert type(effect).model_validate(effect.model_dump()) == effect


@pytest.mark.parametrize(
    "type_value,action,mechanism",
    [
        ("Allosteric modulator", "Neutral", "allosteric_modulation"),
        ("Allosteric modulator", "None", "allosteric_modulation"),
        ("Antagonist", "Partial agonist", None),
        ("Fusion protein", "Binding", "binding"),
        ("None", "Binding", "binding"),
        ("None", "Competitive", None),
    ],
)
@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_physical_only_interactions(
    context: KozaTransform, type_value: str, action: str, mechanism: str | None, endogenous: str
) -> None:
    """Mapping rows 32, 33, 44, 54, 71, and 72 assert only physical interaction."""
    record = RECORD | {"Type": type_value, "Action": action, "Endogenous": endogenous}
    graph = list(transform_ingest_all(context, [record]))[0]

    assert [node.id for node in graph.nodes] == ["PUBCHEM.COMPOUND:2244", "UniProtKB:P08588"]
    assert len(graph.edges) == 1
    edge = graph.edges[0]
    assert type(edge).__name__ == "PairwiseMolecularInteraction"
    assert edge.predicate == "biolink:directly_physically_interacts_with"
    assert edge.causal_mechanism_qualifier == mechanism
    assert edge.qualified_predicate is None
    assert edge.object_aspect_qualifier is None
    assert edge.object_direction_qualifier is None
    assert edge.subject == "PUBCHEM.COMPOUND:2244"
    assert edge.object == "UniProtKB:P08588"
    assert edge.publications == ["PMID:123", "PMID:456"]
    assert edge.species_context_qualifier == "NCBITaxon:9606"
    assert [source.model_dump(mode="json", exclude_none=True) for source in edge.sources] == SOURCES
    assert type(edge).model_validate(edge.model_dump()) == edge


def _assert_explicit_none_edges(
    context: KozaTransform,
    *,
    type_value: str | None,
    action: str | None,
    endogenous: str,
    expected_edges: list[dict[str, str | None]],
) -> None:
    """Compare emitted edges with literal expectations; never infer a mapping.

    Qi's aspect review excerpt checked 2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/pull/504#discussion_r4138697143
    Qi's direction review excerpt checked 2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/pull/504#discussion_r4138807786

    Null qualifier expectations must also be omitted from serialized output.
    """
    record = RECORD | {"Type": type_value, "Action": action, "Endogenous": endogenous}
    graph = list(transform_ingest_all(context, [record]))[0]
    actual_edges = [
        {
            "class": type(edge).__name__,
            "predicate": edge.predicate,
            "qualified_predicate": getattr(edge, "qualified_predicate", None),
            "object_aspect_qualifier": getattr(edge, "object_aspect_qualifier", None),
            "object_direction_qualifier": getattr(edge, "object_direction_qualifier", None),
            "causal_mechanism_qualifier": getattr(edge, "causal_mechanism_qualifier", None),
        }
        for edge in graph.edges
    ]
    assert actual_edges == expected_edges
    if not expected_edges:
        assert graph.nodes == []
    for edge, expected in zip(graph.edges, expected_edges, strict=True):
        serialized = edge.model_dump(mode="json", exclude_none=True)
        for qualifier, value in expected.items():
            if value is None:
                assert qualifier not in serialized


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [("FALSE", "biolink:affects", "increased"), ("TRUE", "biolink:regulates", "upregulated")],
)
def test_activator_literal_none_has_positive_effect_without_mechanism(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """Activator + "None" retains a positive effect and no mechanism or physical edge.

    Source row 6, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E6:M6
    """
    _assert_explicit_none_edges(
        context,
        type_value="Activator",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": direction,
                "causal_mechanism_qualifier": None,
            }
        ],
    )


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [("FALSE", "biolink:affects", "increased"), ("TRUE", "biolink:regulates", "upregulated")],
)
def test_agonist_literal_none_emits_agonism_and_physical_interaction(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """Agonist + "None" emits agonism and a physical edge without a mechanism.

    Source row 18, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E18:M18
    """
    _assert_explicit_none_edges(
        context,
        type_value="Agonist",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": direction,
                "causal_mechanism_qualifier": "agonism",
            },
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            },
        ],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_allosteric_modulator_literal_none_emits_only_allosteric_interaction(
    context: KozaTransform, endogenous: str
) -> None:
    """Allosteric modulator + "None" emits only an allosteric physical edge.

    Source row 33, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E33:M33
    """
    _assert_explicit_none_edges(
        context,
        type_value="Allosteric modulator",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": "allosteric_modulation",
            }
        ],
    )


@pytest.mark.parametrize("endogenous,predicate", [("FALSE", "biolink:affects"), ("TRUE", "biolink:regulates")])
def test_antibody_literal_none_has_undirected_activity_and_physical_interaction(
    context: KozaTransform, endogenous: str, predicate: str
) -> None:
    """Antibody + "None" has unqualified, undirected activity and a physical edge.

    Source row 49, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E49:M49
    """
    _assert_explicit_none_edges(
        context,
        type_value="Antibody",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": None,
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            },
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            },
        ],
    )


@pytest.mark.parametrize("endogenous,predicate", [("FALSE", "biolink:affects"), ("TRUE", "biolink:regulates")])
def test_channel_blocker_literal_none_has_undirected_channel_blockage(
    context: KozaTransform, endogenous: str, predicate: str
) -> None:
    """Channel blocker + "None" has undirected channel blockage and a physical edge.

    Source row 52, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E52:M52
    """
    _assert_explicit_none_edges(
        context,
        type_value="Channel blocker",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": None,
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": "molecular_channel_blockage",
            },
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            },
        ],
    )


@pytest.mark.parametrize("endogenous,predicate", [("FALSE", "biolink:affects"), ("TRUE", "biolink:regulates")])
def test_gating_inhibitor_literal_none_has_undirected_gating_inhibition(
    context: KozaTransform, endogenous: str, predicate: str
) -> None:
    """Gating inhibitor + "None" has undirected gating inhibition and a physical edge.

    Source row 58, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E58:M58
    """
    _assert_explicit_none_edges(
        context,
        type_value="Gating inhibitor",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": None,
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": "gating_inhibition",
            },
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            },
        ],
    )


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [("FALSE", "biolink:affects", "decreased"), ("TRUE", "biolink:regulates", "downregulated")],
)
def test_inhibitor_literal_none_emits_inhibition_and_physical_interaction(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """Inhibitor + "None" emits inhibition and a physical edge.

    Source row 69, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E69:M69
    """
    _assert_explicit_none_edges(
        context,
        type_value="Inhibitor",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": direction,
                "causal_mechanism_qualifier": "inhibition",
            },
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            },
        ],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_literal_none_binding_emits_only_physical_binding(context: KozaTransform, endogenous: str) -> None:
    """ "None" + Binding emits only a physical binding edge.

    Source row 71, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E71:M71
    """
    _assert_explicit_none_edges(
        context,
        type_value="None",
        action="Binding",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": "binding",
            }
        ],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_literal_none_competitive_emits_only_physical_interaction(context: KozaTransform, endogenous: str) -> None:
    """ "None" + Competitive emits only a physical edge without a mechanism.

    Source row 72, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E72:M72
    """
    _assert_explicit_none_edges(
        context,
        type_value="None",
        action="Competitive",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            }
        ],
    )


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [("FALSE", "biolink:affects", "decreased"), ("TRUE", "biolink:regulates", "downregulated")],
)
def test_literal_none_inhibition_emits_inhibition_and_physical_interaction(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """ "None" + Inhibition emits inhibition and a physical edge.

    Source row 73, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E73:M73
    """
    _assert_explicit_none_edges(
        context,
        type_value="None",
        action="Inhibition",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": direction,
                "causal_mechanism_qualifier": "inhibition",
            },
            {
                "class": "PairwiseMolecularInteraction",
                "predicate": "biolink:directly_physically_interacts_with",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            },
        ],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_literal_none_literal_none_emits_only_related_to(context: KozaTransform, endogenous: str) -> None:
    """ "None" + "None" emits only an unqualified related_to edge.

    Source row 74, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E74:M74
    """
    _assert_explicit_none_edges(
        context,
        type_value="None",
        action="None",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "Association",
                "predicate": "biolink:related_to",
                "qualified_predicate": None,
                "object_aspect_qualifier": None,
                "object_direction_qualifier": None,
                "causal_mechanism_qualifier": None,
            }
        ],
    )


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [("FALSE", "biolink:affects", "increased"), ("TRUE", "biolink:regulates", "upregulated")],
)
def test_literal_none_potentiation_emits_only_potentiation(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """ "None" + Potentiation emits only a positive potentiation edge.

    Source row 75, checked against the supplied mapping snapshot at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142&range=E75:M75
    """
    _assert_explicit_none_edges(
        context,
        type_value="None",
        action="Potentiation",
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": direction,
                "causal_mechanism_qualifier": "potentiation",
            }
        ],
    )


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [("FALSE", "biolink:affects", "increased"), ("TRUE", "biolink:regulates", "upregulated")],
)
def test_activator_python_none_action_preserves_positive_fallback(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """Activator + Python None uses its effect-only fallback with no mechanism.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Activator",
        action=None,
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": direction,
                "causal_mechanism_qualifier": None,
            }
        ],
    )


@pytest.mark.parametrize(
    "endogenous,predicate,direction",
    [("FALSE", "biolink:affects", "decreased"), ("TRUE", "biolink:regulates", "downregulated")],
)
def test_inhibitor_python_none_action_preserves_negative_fallback(
    context: KozaTransform, endogenous: str, predicate: str, direction: str
) -> None:
    """Inhibitor + Python None uses its effect-only fallback with no mechanism.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Inhibitor",
        action=None,
        endogenous=endogenous,
        expected_edges=[
            {
                "class": "ChemicalAffectsGeneAssociation",
                "predicate": predicate,
                "qualified_predicate": "biolink:causes",
                "object_aspect_qualifier": "activity",
                "object_direction_qualifier": direction,
                "causal_mechanism_qualifier": None,
            }
        ],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_agonist_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Agonist' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Agonist",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_allosteric_modulator_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Allosteric modulator' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Allosteric modulator",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_antagonist_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Antagonist' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Antagonist",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_antibody_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Antibody' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Antibody",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_channel_blocker_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Channel blocker' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Channel blocker",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_fusion_protein_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Fusion protein' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Fusion protein",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_gating_inhibitor_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Gating inhibitor' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Gating inhibitor",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_subunit_specific_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'Subunit-specific' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="Subunit-specific",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_literal_none_type_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """'None' + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value="None",
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_python_none_type_python_none_action_emits_nothing(context: KozaTransform, endogenous: str) -> None:
    """None + Python None emits no nodes or edges.

    Python-null fallback contract (not a spreadsheet claim), verified at
    2026-10-24T12:29:55-07:00 (America/Vancouver):
    https://github.com/NCATSTranslator/translator-ingests/blob/6633abb64ecd86a5a32a8e238459f38d3ff86621/src/translator_ingest/ingests/gtopdb/rules.py#L278-L305
    """
    _assert_explicit_none_edges(
        context,
        type_value=None,
        action=None,
        endogenous=endogenous,
        expected_edges=[],
    )


@pytest.mark.parametrize(
    "uniprot_ids",
    [
        "P46098|O95264",
        "",
    ],
)
@pytest.mark.parametrize(
    "type_value,action",
    [("Agonist", "Agonist"), ("Fusion protein", "Binding"), ("Allosteric modulator", "Neutral")],
)
def test_unsupported_targets_remain_excluded(
    context: KozaTransform, uniprot_ids: str, type_value: str, action: str
) -> None:
    """Exclude unmapped targets and multiple proteins without source subunit evidence."""
    record = RECORD | {"target_uniprot_ids": uniprot_ids, "Type": type_value, "Action": action}
    graph = list(transform_ingest_all(context, [record]))[0]
    assert graph.nodes == []
    assert graph.edges == []


@pytest.mark.parametrize(
    "type_value,action,primary_mechanism,physical_mechanism",
    [
        ("Agonist", "Binding", "agonism", "binding"),
        ("Fusion protein", "Binding", None, "binding"),
        ("Allosteric modulator", "Neutral", None, "allosteric_modulation"),
    ],
)
@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
@pytest.mark.parametrize(
    "species,taxon,accessions",
    [
        ("Human", "NCBITaxon:9606", ("P46098", "O95264")),
        ("Mouse", "NCBITaxon:10090", ("P23979", "Q9JHJ5")),
    ],
)
def test_complex_targets_preserve_independent_interaction_rules(
    context: KozaTransform,
    type_value: str,
    action: str,
    primary_mechanism: str | None,
    physical_mechanism: str,
    endogenous: str,
    species: str,
    taxon: str,
    accessions: tuple[str, str],
) -> None:
    """Retain binding-only rules and separate mechanisms on a source-defined complex."""
    record = RECORD | {
        "target_id": "378",
        "target_name": "5-HT3AB",
        "target_species": species,
        "target_subunit_ids": "373|374",
        "target_gene_symbols": "HTR3A|HTR3B",
        "target_uniprot_ids": "|".join(accessions),
        "Type": type_value,
        "Action": action,
        "Endogenous": endogenous,
    }
    graph = list(transform_ingest_all(context, [record]))[0]

    assert {(type(node).__name__, node.id) for node in graph.nodes} == {
        ("ChemicalEntity", "PUBCHEM.COMPOUND:2244"),
        ("MacromolecularComplex", "IUPHARobj:378"),
        *(("Protein", f"UniProtKB:{accession}") for accession in accessions),
    }
    assert all(node.in_taxon == [taxon] for node in graph.nodes if node.id != "PUBCHEM.COMPOUND:2244")
    interactions = [edge for edge in graph.edges if edge.subject == "PUBCHEM.COMPOUND:2244"]
    assert len(interactions) == (2 if primary_mechanism else 1)
    assert all(edge.object == "IUPHARobj:378" for edge in interactions)
    assert all(edge.species_context_qualifier == taxon for edge in interactions)
    if primary_mechanism:
        primary = interactions[0]
        assert primary.predicate == ("biolink:regulates" if endogenous == "TRUE" else "biolink:affects")
        assert primary.causal_mechanism_qualifier == primary_mechanism
        assert primary.qualified_predicate == "biolink:causes"
        assert primary.object_aspect_qualifier == "activity"
        assert primary.object_direction_qualifier == ("upregulated" if endogenous == "TRUE" else "increased")
    physical = interactions[-1]
    assert physical.predicate == "biolink:directly_physically_interacts_with"
    assert physical.causal_mechanism_qualifier == physical_mechanism
    assert physical.qualified_predicate is None
    assert physical.object_aspect_qualifier is None
    assert physical.object_direction_qualifier is None
    component_edges = [edge for edge in graph.edges if edge.predicate == "biolink:has_part"]
    assert {(edge.subject, edge.object) for edge in component_edges} == {
        ("IUPHARobj:378", f"UniProtKB:{accession}") for accession in accessions
    }
    assert len(graph.edges) == len(interactions) + len(component_edges)
    for edge in graph.edges:
        assert edge.publications == ["PMID:123", "PMID:456"]
        assert type(edge).model_validate(edge.model_dump()) == edge


@pytest.mark.parametrize(
    "type_value,action,primary_mechanism,physical_mechanism",
    [
        ("Activator", "Binding", "binding", "binding"),
        ("Agonist", "Binding", "agonism", "binding"),
        ("Antagonist", "Binding", "antagonism", "binding"),
        ("Antibody", "Binding", "binding", "binding"),
        ("Inhibitor", "Binding", "inhibition", "binding"),
        ("Allosteric modulator", "Agonist", "agonism", "allosteric_modulation"),
        ("Allosteric modulator", "Antagonist", "antagonism", "allosteric_modulation"),
        ("Allosteric modulator", "Biased agonist", "biased_agonism", "allosteric_modulation"),
        ("Allosteric modulator", "Binding", "allosteric_modulation", "allosteric_modulation"),
        ("Allosteric modulator", "Biphasic", "biphasic_allosteric_modulation", "allosteric_modulation"),
        ("Allosteric modulator", "Full agonist", "agonism", "allosteric_modulation"),
        ("Allosteric modulator", "Inhibition", "inhibition", "allosteric_modulation"),
        ("Allosteric modulator", "Inverse agonist", "inverse_agonism", "allosteric_modulation"),
        ("Allosteric modulator", "Mixed", "mixed_allosteric_modulation", "allosteric_modulation"),
        ("Allosteric modulator", "Negative", "negative_allosteric_modulation", "allosteric_modulation"),
        ("Allosteric modulator", "Partial agonist", "partial_agonism", "allosteric_modulation"),
        ("Allosteric modulator", "Positive", "positive_allosteric_modulation", "allosteric_modulation"),
        ("Allosteric modulator", "Potentiation", "potentiation", "allosteric_modulation"),
    ],
)
@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_primary_and_physical_mechanisms_are_independent(
    context: KozaTransform,
    type_value: str,
    action: str,
    primary_mechanism: str,
    physical_mechanism: str,
    endogenous: str,
) -> None:
    """Apply mapping columns K and M to their own edges without leaking qualifiers.

    Full agonism intentionally uses agonism on the primary edge.
    https://docs.google.com/spreadsheets/d/1DeAE04O1mz3R9s3dCZpG2hQp9hwif5WjdkUMsBci-u8/edit?gid=461277142
    """
    record = RECORD | {"Type": type_value, "Action": action, "Endogenous": endogenous}
    graph = list(transform_ingest_all(context, [record]))[0]

    assert len(graph.edges) == 2
    primary, physical = graph.edges
    assert primary.predicate == ("biolink:regulates" if endogenous == "TRUE" else "biolink:affects")
    assert primary.object_aspect_qualifier == "activity"
    assert primary.causal_mechanism_qualifier == primary_mechanism
    assert physical.predicate == "biolink:directly_physically_interacts_with"
    assert physical.causal_mechanism_qualifier == physical_mechanism
    assert physical.qualified_predicate is None
    assert physical.object_aspect_qualifier is None
    assert physical.object_direction_qualifier is None
    assert primary.publications == physical.publications == ["PMID:123", "PMID:456"]
    assert primary.species_context_qualifier == physical.species_context_qualifier == "NCBITaxon:9606"
    assert type(physical).model_validate(physical.model_dump()) == physical


@pytest.mark.parametrize(
    "type_value,action,mechanism,physical_count",
    [
        ("Activator", "None", None, 0),
        ("Activator", "Positive", None, 0),
        ("Fusion protein", "Inhibition", "inhibition", 1),
    ],
)
@pytest.mark.parametrize("endogenous", ["FALSE", "TRUE"])
def test_primary_mechanism_corrections(
    context: KozaTransform,
    type_value: str,
    action: str,
    mechanism: str | None,
    physical_count: int,
    endogenous: str,
) -> None:
    """Mapping rows 6, 8, and 55 specify absent or inhibitory primary mechanisms."""
    record = RECORD | {"Type": type_value, "Action": action, "Endogenous": endogenous}
    graph = list(transform_ingest_all(context, [record]))[0]

    assert len(graph.edges) == 1 + physical_count
    primary = graph.edges[0]
    assert primary.causal_mechanism_qualifier == mechanism
    assert primary.qualified_predicate == "biolink:causes"
    assert primary.object_aspect_qualifier == "activity"
    assert primary.predicate == ("biolink:regulates" if endogenous == "TRUE" else "biolink:affects")
    directions = ("decreased", "downregulated") if type_value == "Fusion protein" else ("increased", "upregulated")
    assert primary.object_direction_qualifier == directions[endogenous == "TRUE"]


@pytest.mark.parametrize("type_value", [None, float("nan"), pd.NA, "", "Unknown", "agonist", " Agonist ", True])
def test_unknown_type_skips_before_required_emission_fields(context: KozaTransform, type_value: Any) -> None:
    """The current baseline resolves unsupported types before constructing nodes."""
    record = {key: value for key, value in RECORD.items() if key not in {"subject_id", "Endogenous", "PubMed ID"}}
    graph = list(transform_ingest_all(context, [record | {"Type": type_value}]))[0]
    assert graph.nodes == []
    assert graph.edges == []


@pytest.mark.parametrize(
    "type_value,action",
    [
        ("Allosteric modulator", None),
        ("Subunit-specific", "future action"),
        ("None", "future action"),
    ],
)
@pytest.mark.parametrize("publications", [None, "123"])
def test_old_exceptional_paths_now_skip(
    context: KozaTransform, type_value: str, action: Any, publications: Any
) -> None:
    """Preserve current skips rather than reintroducing failures from 72f65887."""
    record = RECORD | {"Type": type_value, "Action": action, "PubMed ID": publications}
    assert graph_signature(list(transform_ingest_all(context, [record]))[0]) == {"nodes": [], "edges": []}


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, None),
        ("", None),
        (False, None),
        (0, None),
        ("123|123", ["PMID:123", "PMID:123"]),
        (" 123 |456 ", ["PMID: 123 ", "PMID:456 "]),
        ("123||", ["PMID:123", "PMID:", "PMID:"]),
    ],
)
def test_publication_policy(context: KozaTransform, value: Any, expected: list[str] | None) -> None:
    """Attach literal tokens to both edges without cleaning or evidence leakage."""
    graph = list(transform_ingest_all(context, [RECORD | {"PubMed ID": value}, RECORD | {"PubMed ID": ""}]))[0]
    assert [edge.publications for edge in graph.edges] == [expected, expected, None, None]


@pytest.mark.parametrize("missing", ["Type", "Action", "subject_id", "subject_name", "Endogenous", "PubMed ID"])
def test_missing_required_key(context: KozaTransform, missing: str) -> None:
    """Retain the missing-key exception on an otherwise supported record."""
    record = {key: value for key, value in RECORD.items() if key != missing}
    with pytest.raises(KeyError) as error:
        list(transform_ingest_all(context, [record]))
    assert error.value.args == (missing,)


@pytest.mark.parametrize(
    "updates,error_type,detail",
    [
        ({"Type": []}, TypeError, "unhashable type"),
        ({"Action": []}, TypeError, "unhashable type"),
        ({"subject_name": {}}, ValidationError, "name"),
        ({"PubMed ID": 123}, AttributeError, "split"),
        ({"PubMed ID": float("nan")}, AttributeError, "split"),
        ({"PubMed ID": pd.NA}, TypeError, "boolean value of NA is ambiguous"),
        ({"Endogenous": pd.NA}, TypeError, "boolean value of NA is ambiguous"),
    ],
)
def test_malformed_emitted_record(
    context: KozaTransform, updates: dict[str, Any], error_type: type[Exception], detail: str
) -> None:
    """Characterize current failures with real model validation and scalar values."""
    with pytest.raises(error_type, match=detail):
        list(transform_ingest_all(context, [RECORD | updates]))


def test_emission_access_order(context: KozaTransform) -> None:
    """Node validation precedes evidence parsing; evidence precedes context projection."""
    with pytest.raises(ValidationError, match="name"):
        list(transform_ingest_all(context, [RECORD | {"subject_name": {}, "PubMed ID": 123}]))
    with pytest.raises(AttributeError, match="split"):
        list(transform_ingest_all(context, [RECORD | {"PubMed ID": 123, "Endogenous": pd.NA}]))
    record = {key: value for key, value in RECORD.items() if key != "Endogenous"}
    with pytest.raises(KeyError, match="Endogenous"):
        list(transform_ingest_all(context, [record | {"PubMed ID": 123}]))


@pytest.mark.parametrize("records", [[], [RECORD | {"Type": "unknown"}]])
def test_empty_and_all_filtered_batches(context: KozaTransform, records: list[dict[str, Any]]) -> None:
    """Always return one aggregate graph, including when nothing is emitted."""
    graphs = list(transform_ingest_all(context, iter(records)))
    assert len(graphs) == 1
    assert graph_signature(graphs[0]) == {"nodes": [], "edges": []}


def test_mixed_batch_order_duplicates_and_evidence(context: KozaTransform) -> None:
    """Retain record order and multiplicity across different rules and evidence."""
    records = [
        RECORD,
        RECORD | {"Type": "None", "Action": "None", "PubMed ID": ""},
        RECORD | {"Type": "unknown"},
        RECORD,
        RECORD | {"Type": "Inhibitor", "Action": "future action", "Endogenous": "TRUE"},
    ]
    individual = [graph_signature(list(transform_ingest_all(context, [record]))[0]) for record in records]
    graph = list(transform_ingest_all(context, iter(records)))[0]
    assert graph_signature(graph) == {
        name: [entity for part in individual for entity in part[name]] for name in ("nodes", "edges")
    }
    assert len(graph.nodes) == 8
    assert len(graph.edges) == 6
    assert len({edge.id for edge in graph.edges}) == 6
