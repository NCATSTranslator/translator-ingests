"""GtoPdb record behavior, revalidated against the main merge at aac620a4.

Initially characterized at 0d1e745c, these tests also preserve the merged
baseline's complex targets and skips for previously exceptional unknown mappings.
The older 72f65887 specification must not undo those changes.
Expectations use the checked-in interaction inventory, never production rules.
"""

import json
from collections.abc import Iterator
from pathlib import Path
from shutil import copyfile
from typing import Any

import pandas as pd
import pytest
from koza import KozaTransform
from koza.io.writer.jsonl_writer import JSONLWriter
from koza.model.graphs import KnowledgeGraph
from koza.model.writer import WriterConfig
from pydantic import ValidationError

from translator_ingest.ingests.gtopdb.gtopdb import prepare, transform_ingest_all

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
        assert getattr(edge, "species_context_qualifier", None) == (
            None if edge.predicate == "biolink:related_to" else "NCBITaxon:9606"
        )
    if expected:
        assert graph.nodes[1].in_taxon == ["NCBITaxon:9606"]


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


def test_all_target_descriptors_are_validated_before_emission(context: KozaTransform) -> None:
    """A later malformed descriptor still fails before an earlier missing Type."""
    first = {key: value for key, value in RECORD.items() if key != "Type"}
    second = RECORD | {"target_name": ["a", "b"]}
    with pytest.raises(ValueError, match="truth value of an array"):
        list(transform_ingest_all(context, [first, second]))


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


def test_complex_order_and_filtered_species_context(context: KozaTransform) -> None:
    """Even a skipped second-species record prevents unqualified component edges."""
    human = RECORD | {"target_id": "378", "target_subunit_ids": "373|374", "target_uniprot_ids": "P46098|O95264"}
    single = list(transform_ingest_all(context, [human]))[0]
    assert [node.id for node in single.nodes] == [
        "PUBCHEM.COMPOUND:2244",
        "IUPHARobj:378",
        "UniProtKB:P46098",
        "UniProtKB:O95264",
    ]
    assert [edge.predicate for edge in single.edges] == [
        "biolink:affects",
        "biolink:directly_physically_interacts_with",
        "biolink:has_part",
        "biolink:has_part",
    ]
    assert all(edge.publications == ["PMID:123", "PMID:456"] for edge in single.edges)
    assert len({edge.id for edge in single.edges}) == 4
    mouse = human | {"target_species": "Mouse", "Type": "unknown"}
    combined = list(transform_ingest_all(context, [human, mouse]))[0]
    assert [node.id for node in combined.nodes] == ["PUBCHEM.COMPOUND:2244", "IUPHARobj:378"]
    assert len(combined.edges) == 2


def test_local_preparation_and_jsonl_output(context: KozaTransform, tmp_path: Path) -> None:
    """Exercise CSV mapping, grouping, missing scalars, and real graph serialization."""
    copyfile(FIXTURES / "fixtures" / "ligands.csv", tmp_path / "ligands.csv")
    source = pd.read_csv(FIXTURES / "fixtures" / "interactions.csv", dtype={"Ligand ID": str, "Target ID": str})
    records = prepare(context, source.to_dict(orient="records"))
    assert [record["target_name"] for record in records] == [
        "A target",
        "B missing labels",
        "C blank CID",
        "D missing CID",
        "G isoform",
    ]
    assert [record["subject_id"] for record in records] == ["2244", "2244", "", "nan", "0055"]
    assert records[0]["PubMed ID"] == "123|123|456"
    assert all(pd.isna(records[1][key]) for key in ("Type", "Action", "Endogenous"))
    assert isinstance(records[1]["Type"], float)
    graph = list(transform_ingest_all(context, records))[0]
    assert [edge.subject for edge in graph.edges] == [
        "PUBCHEM.COMPOUND:2244",
        "PUBCHEM.COMPOUND:2244",
        "PUBCHEM.COMPOUND:",
        "PUBCHEM.COMPOUND:",
        "PUBCHEM.COMPOUND:nan",
        "PUBCHEM.COMPOUND:nan",
        "PUBCHEM.COMPOUND:0055",
        "PUBCHEM.COMPOUND:0055",
    ]
    assert graph.edges[0].publications == ["PMID:123", "PMID:123", "PMID:456"]
    assert graph.edges[-1].object == "UniProtKB:P56856"
    context.write(*graph.nodes, *graph.edges)
    context.writer.finalize()
    written_edges = [json.loads(line) for line in (tmp_path / "output" / "gtopdb_edges.jsonl").read_text().splitlines()]
    assert written_edges == [edge.model_dump(mode="json", exclude_none=True) for edge in graph.edges]
    written_nodes = [json.loads(line) for line in (tmp_path / "output" / "gtopdb_nodes.jsonl").read_text().splitlines()]
    assert [node["id"] for node in written_nodes] == [
        "PUBCHEM.COMPOUND:2244",
        "UniProtKB:P08588",
        "PUBCHEM.COMPOUND:",
        "PUBCHEM.COMPOUND:nan",
        "PUBCHEM.COMPOUND:0055",
        "UniProtKB:P56856",
    ]
