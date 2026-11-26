"""Test CPIC through the production Koza reader/transform/writer configuration."""

import csv
import io
import json
from pathlib import Path
from zipfile import ZipFile

import pytest
from koza.model.writer import OutputFormat
from koza.runner import KozaRunner

from tests.util import get_ingest_config_yaml_path
from translator_ingest.ingests.cpic.cpic import ACCESSION, load_entities, numeric_curie, publications, read_pairs


def archive(directory: Path, name: str, columns: list[str], rows: list[list[str]]) -> None:
    """Write a synthetic entity table using the real ZIP member convention."""
    stream = io.StringIO()
    writer = csv.writer(stream, delimiter="\t")
    writer.writerow(columns)
    writer.writerows(rows)
    with ZipFile(directory / f"{name}.zip", "w") as zipped:
        zipped.writestr(f"{name}.tsv", stream.getvalue())


def pair(**overrides: object) -> dict:
    """Build a synthetic API pair; names deliberately differ from entity-table names."""
    return {
        "pairid": 1,
        "cpgxgeneid": "PAG1",
        "cpgxdrugid": "PAD1",
        "clinpgxid": "PA123",
        "genesymbol": "Example symbol",
        "drugname": "Example drug alias",
        "cpiclevel": "A",
        "pgkbcalevel": "1A",
        "pgxtesting": None,
        "pmids": ["123", "456"],
        "provisional": False,
        "usedforrecommendation": "Yes",
        **overrides,
    }


def write_pairs(directory: Path, rows: list[dict]) -> None:
    """Save a successful API envelope for an offline transform."""
    (directory / "pairs.json").write_text(json.dumps({"status": "success", "data": rows}))


@pytest.fixture
def input_dir(tmp_path: Path) -> Path:
    """Cover scope filters, ID ambiguity, and evidence aggregation with no mocks."""
    directory = tmp_path / "input"
    directory.mkdir()
    archive(
        directory,
        "genes",
        [ACCESSION, "Name", "NCBI Gene ID"],
        [
            ["PAG1", "Example gene", "101"],
            ["PAG2", "Ambiguous gene", "102,103"],
        ],
    )
    columns = [ACCESSION, "Name", "PubChem Compound Identifiers"]
    archive(
        directory,
        "chemicals",
        columns,
        [
            ["PAD1", "Example drug", "201"],
            ["PAD2", "Another drug", "202"],
            ["PAD3", "Missing identifier", ""],
        ],
    )
    archive(directory, "drugs", columns, [["PAD1", "Example drug", "201"]])
    write_pairs(
        directory,
        [
            pair(),
            pair(pairid=2, pmids=["456", "789"], clinpgxid="PA456"),
            pair(pairid=3, cpgxdrugid="PAD2", cpiclevel="C", pmids=None, usedforrecommendation="No"),
            pair(pairid=4, provisional=True),
            pair(pairid=5, cpiclevel="Retired"),
            pair(pairid=6, pgxtesting="No Clinical PGx "),
            pair(pairid=7, cpgxgeneid="PAG2"),
            pair(pairid=8, cpgxdrugid="PAD3"),
            pair(pairid=9, cpgxdrugid="PAD_UNKNOWN"),
        ],
    )
    return directory


def run_ingest(input_dir: Path, output_dir: Path) -> tuple[list[dict], list[dict], dict]:
    """Load the actual YAML and write JSONL using the real Koza runner."""
    output_dir.mkdir()
    _, runner = KozaRunner.from_config_file(
        str(get_ingest_config_yaml_path("cpic")),
        input_files_dir=str(input_dir),
        output_dir=str(output_dir),
        output_format=OutputFormat.jsonl,
    )
    runner.run()
    nodes = [json.loads(line) for line in (output_dir / "cpic_nodes.jsonl").read_text().splitlines()]
    edges = [json.loads(line) for line in (output_dir / "cpic_edges.jsonl").read_text().splitlines()]
    return nodes, edges, runner.transform_metadata["cpic"]


def test_complete_transform(input_dir: Path, tmp_path: Path) -> None:
    """Resolve by accession, union evidence, preserve provenance, and audit filtering."""
    nodes, edges, metadata = run_ingest(input_dir, tmp_path / "output")
    assert len(nodes) == 3
    assert {node["id"] for node in nodes} == {"NCBIGene:101", "PUBCHEM.COMPOUND:201", "PUBCHEM.COMPOUND:202"}
    assert next(node for node in nodes if node["id"] == "PUBCHEM.COMPOUND:201")["category"] == ["biolink:Drug"]
    assert len(edges) == 2
    edge = next(edge for edge in edges if edge["object"] == "PUBCHEM.COMPOUND:201")
    assert edge["subject"] == "NCBIGene:101"
    assert edge["predicate"] == "biolink:associated_with_response_to"
    assert edge["publications"] == ["PMID:123", "PMID:456", "PMID:789"]
    assert edge["knowledge_level"] == "knowledge_assertion"
    assert edge["agent_type"] == "text_mining_agent"
    sources = {source["resource_id"]: source for source in edge["sources"]}
    assert sources["infores:cpic"]["resource_role"] == "primary_knowledge_source"
    assert sources["infores:cpic"]["source_record_urls"] == [
        "https://www.clinpgx.org/guideline/PA123",
        "https://www.clinpgx.org/guideline/PA456",
    ]
    assert sources["infores:clinpgx"]["resource_role"] == "aggregator_knowledge_source"
    assert sources["infores:clinpgx"]["upstream_resource_ids"] == ["infores:cpic"]
    assert metadata["counts"] == {
        "input_rows": 9,
        "included_rows": 3,
        "excluded_provisional": 1,
        "excluded_retired": 1,
        "excluded_no_clinical_pgx": 1,
        "excluded_unmapped_or_ambiguous_identifier": 3,
        "output_nodes": 3,
        "output_edges": 2,
        "merged_rows": 1,
    }
    assert metadata["included_cpic_levels"] == {"A": 2, "C": 1}
    assert [row["pairid"] for row in metadata["pair_annotations"][edge["id"]]] == [1, 2]
    no_pubs = next(edge for edge in edges if edge["object"] == "PUBCHEM.COMPOUND:202")
    assert not no_pubs.get("publications")
    assert metadata["pair_annotations"][no_pubs["id"]][0]["usedforrecommendation"] == "No"
    _, repeat, _ = run_ingest(input_dir, tmp_path / "repeat")
    assert repeat == edges


@pytest.mark.parametrize("rows", [[], [pair(provisional=True)], [pair(pgxtesting="No Clinical PGx")]])
def test_empty_result(input_dir: Path, tmp_path: Path, rows: list[dict]) -> None:
    """An empty or wholly filtered input still writes valid empty graph files."""
    write_pairs(input_dir, rows)
    nodes, edges, metadata = run_ingest(input_dir, tmp_path / "empty")
    assert nodes == edges == []
    assert metadata["counts"]["output_edges"] == 0


@pytest.mark.parametrize(
    "payload",
    [
        {"status": "error", "data": []},
        {"status": "success", "data": {}},
        {"status": "success", "data": [{}]},
        {"status": "success", "data": [pair(provisional="false")]},
    ],
)
def test_bad_api_response_fails(tmp_path: Path, payload: dict) -> None:
    """Reject API errors and schema changes instead of silently emitting the wrong scope."""
    path = tmp_path / "pairs.json"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError):
        read_pairs(path)


@pytest.mark.parametrize("identifier", ["", "nan", "<NA>", "0", "-2", "12.0", "12,13"])
def test_invalid_identifiers(identifier: str) -> None:
    """Do not fabricate CURIEs for invalid or ambiguous numeric IDs."""
    assert numeric_curie(identifier, "NCBIGene") is None


def test_publication_validation() -> None:
    """Normalize numeric PMID values but reject unknown formats and scalar arrays."""
    assert publications(["123", "PMID:456", "123"]) == ["PMID:123", "PMID:456"]
    with pytest.raises(ValueError, match="Invalid CPIC PMID"):
        publications(["bad"])
    with pytest.raises(ValueError, match="must be a list"):
        publications("123")


def test_conflicting_mappings_fail(input_dir: Path) -> None:
    """Overlapping entity tables must agree on an accession's numeric mapping."""
    archive(input_dir, "drugs", [ACCESSION, "Name", "PubChem Compound Identifiers"], [["PAD1", "Example drug", "999"]])
    with pytest.raises(ValueError, match="Conflicting CPIC entity mapping"):
        load_entities(input_dir)
