"""Exercise the real Koza configuration with small synthetic source archives."""

import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from koza.model.writer import OutputFormat
from koza.runner import KozaRunner

from translator_ingest.ingests.clinpgx.clinpgx import (
    ACCESSION,
    RELATIONSHIP_COLUMNS,
    load_entities,
    numeric_curie,
    parse_publications,
)
from tests.util import get_ingest_config_yaml_path


def write_archive(directory: Path, name: str, columns: list[str], rows: list[list[str]]) -> None:
    """Create a TSV ZIP with the upstream filename and creation marker convention."""
    text = io.StringIO()
    writer = csv.writer(text, delimiter="\t")
    writer.writerow(columns)
    writer.writerows(rows)
    with ZipFile(directory / f"{name}.zip", "w") as archive:
        archive.writestr(f"{name}.tsv", text.getvalue())
        archive.writestr("CREATED_2026-10-05.txt", "")


@pytest.fixture
def input_dir(tmp_path: Path) -> Path:
    """Include unmapped, ambiguous, reciprocal, and overlapping entity examples."""
    directory = tmp_path / "input"
    directory.mkdir()
    write_archive(
        directory,
        "genes",
        [ACCESSION, "Name", "NCBI Gene ID"],
        [
            ["PAG1", "Example gene", "101"],
            ["PAG2", "Missing gene", ""],
            ["PAG3", "Ambiguous gene", "102, 103"],
            ["PAG4", "Another accession for gene", "101"],
        ],
    )
    columns = [ACCESSION, "Name", "PubChem Compound Identifiers"]
    write_archive(
        directory,
        "chemicals",
        columns,
        [
            ["PAC1", "Example chemical", "201"],
            ["PAC2", "Missing chemical", ""],
            ["PAC3", "Another chemical", "203"],
        ],
    )
    write_archive(directory, "drugs", columns, [["PAC1", "Example chemical", "201"]])
    write_archive(
        directory,
        "relationships",
        RELATIONSHIP_COLUMNS,
        [
            ["PAG1", "Gene", "PAC1", "Chemical", "VariantAnnotation", "associated", "123;456"],
            ["PAC1", "Chemical", "PAG1", "Gene", "Guideline", "associated", "456;789"],
            ["PAG4", "Gene", "PAC1", "Chemical", "SummaryAnnotation", "associated", "123"],
            ["PAG1", "Gene", "PAC3", "Chemical", "Guideline", "associated", ""],
            ["PAG1", "Gene", "PAC1", "Chemical", "VariantAnnotation", "not associated", "999"],
            ["PAG1", "Gene", "PAC1", "Chemical", "VariantAnnotation", "ambiguous", "888"],
            ["PAV1", "SNV", "PAC1", "Chemical", "VariantAnnotation", "associated", "777"],
            ["PAG1", "Gene", "PAP1", "Phenotype", "VariantAnnotation", "associated", "666"],
            ["PAG2", "Gene", "PAC1", "Chemical", "VariantAnnotation", "associated", "555"],
            ["PAG3", "Gene", "PAC1", "Chemical", "VariantAnnotation", "associated", "444"],
            ["PAG1", "Gene", "PAC2", "Chemical", "VariantAnnotation", "associated", "333"],
            ["PAG9", "Gene", "PAC1", "Chemical", "VariantAnnotation", "associated", "222"],
        ],
    )
    return directory


def run_ingest(input_dir: Path, output_dir: Path) -> tuple[list[dict], list[dict], dict]:
    """Run the production YAML and JSONL writer, with no mocked components."""
    output_dir.mkdir()
    _, runner = KozaRunner.from_config_file(
        str(get_ingest_config_yaml_path("clinpgx")),
        input_files_dir=str(input_dir),
        output_dir=str(output_dir),
        output_format=OutputFormat.jsonl,
    )
    runner.run()
    nodes = [json.loads(line) for line in (output_dir / "clinpgx_nodes.jsonl").read_text().splitlines()]
    edges = [json.loads(line) for line in (output_dir / "clinpgx_edges.jsonl").read_text().splitlines()]
    return nodes, edges, runner.transform_metadata["clinpgx"]


def test_zip_to_kgx(input_dir: Path, tmp_path: Path) -> None:
    """Keep only positive mapped pairs, union reciprocal evidence, and retain xrefs."""
    nodes, edges, metadata = run_ingest(input_dir, tmp_path / "output")
    assert {node["id"] for node in nodes} == {"NCBIGene:101", "PUBCHEM.COMPOUND:201", "PUBCHEM.COMPOUND:203"}
    assert len(nodes) == 3
    gene = next(node for node in nodes if node["id"] == "NCBIGene:101")
    assert gene["category"] == ["biolink:Gene"]
    assert gene["xref"] == ["PharmGKB:PAG1", "PharmGKB:PAG4"]
    assert len(edges) == 2
    edge = next(edge for edge in edges if edge["object"] == "PUBCHEM.COMPOUND:201")
    assert edge["subject"] == "NCBIGene:101"
    assert edge["predicate"] == "biolink:associated_with"
    assert edge["publications"] == ["PMID:123", "PMID:456", "PMID:789"]
    assert edge["agent_type"] == "automated_agent"
    assert edge["knowledge_level"] == "knowledge_assertion"
    assert any(
        source["resource_id"] == "infores:clinpgx" and source["resource_role"] == "primary_knowledge_source"
        for source in edge["sources"]
    )
    no_pubs = next(edge for edge in edges if edge["object"] == "PUBCHEM.COMPOUND:203")
    assert not no_pubs.get("publications")
    assert metadata["counts"] == {
        "input_rows": 12,
        "included_rows": 4,
        "excluded_not_positive": 2,
        "excluded_outside_initial_scope": 2,
        "excluded_unmapped_or_ambiguous_identifier": 4,
        "output_edges": 2,
        "output_nodes": 3,
        "merged_rows": 2,
    }
    assert metadata["included_evidence_labels_by_source_row"] == {
        "Guideline": 2,
        "SummaryAnnotation": 1,
        "VariantAnnotation": 1,
    }
    assert set(tuple(dates) for dates in metadata["archive_creation_dates"].values()) == {("2026-10-05",)}
    _, repeated_edges, _ = run_ingest(input_dir, tmp_path / "repeated")
    assert repeated_edges == edges


def test_empty_scope_produces_empty_graph(input_dir: Path, tmp_path: Path) -> None:
    """A source with no supported associations should complete without StopIteration."""
    write_archive(input_dir, "relationships", RELATIONSHIP_COLUMNS, [])
    nodes, edges, metadata = run_ingest(input_dir, tmp_path / "empty")
    assert nodes == []
    assert edges == []
    assert metadata["counts"]["output_edges"] == 0


@pytest.mark.parametrize("value", ["", "nan", "<NA>", "0", "-1", "101.0", "101,102", "101;102"])
def test_reject_invalid_or_ambiguous_ids(value: str) -> None:
    """Never silently create a malformed or multiply mapped identifier."""
    assert numeric_curie(value, "NCBIGene") is None


def test_publication_normalization() -> None:
    """Handle whitespace, existing prefixes, empty values, and duplicate PMIDs."""
    assert parse_publications("123; PMID:456;123; ") == ["PMID:123", "PMID:456"]
    assert parse_publications("") == []
    with pytest.raises(ValueError, match="Invalid ClinPGx PMID"):
        parse_publications("123;not-a-pmid")


def test_conflicting_entity_mappings_fail(input_dir: Path) -> None:
    """A drug/chemical disagreement must not depend on table processing order."""
    write_archive(
        input_dir, "drugs", [ACCESSION, "Name", "PubChem Compound Identifiers"], [["PAC1", "Example chemical", "999"]]
    )
    with pytest.raises(ValueError, match="Conflicting ClinPGx identifier mappings"):
        load_entities(input_dir)


def test_missing_source_column_fails(input_dir: Path) -> None:
    """An upstream schema change must not masquerade as an empty mapping table."""
    write_archive(input_dir, "genes", [ACCESSION, "Name"], [["PAG1", "Example gene"]])
    with pytest.raises(ValueError, match="Usecols"):
        load_entities(input_dir)
