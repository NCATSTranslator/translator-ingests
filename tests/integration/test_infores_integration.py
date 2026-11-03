"""Integration tests for infores validation against the real Information Resource Registry release."""

import json
import warnings
from pathlib import Path

import pytest

from translator_ingest.util import biolink
from translator_ingest.util.infores import InforesStatus, get_infores_catalog
from translator_ingest.util.storage.local import IngestFileName
from translator_ingest.util.validate_biolink_kgx import ValidationStatus, validate_kgx

NODES = [
    {"id": "NCBIGene:1017", "category": ["biolink:Gene"], "name": "CDK2"},
    {"id": "UBERON:0002107", "category": ["biolink:AnatomicalEntity"], "name": "liver"},
]


def make_edge(edge_id: str, sources: list[dict]) -> dict:
    """An expressed_in edge between the two test nodes with the given sources."""
    return {
        "id": edge_id,
        "category": ["biolink:GeneToExpressionSiteAssociation"],
        "subject": "NCBIGene:1017",
        "predicate": "biolink:expressed_in",
        "object": "UBERON:0002107",
        "sources": sources,
        "knowledge_level": "knowledge_assertion",
        "agent_type": "automated_agent",
    }


def write_jsonl(path: Path, records: list[dict]) -> Path:
    path.write_text("".join(json.dumps(record) + "\n" for record in records))
    return path


def run_validation(tmp_path: Path, edges: list[dict]) -> tuple[bool, dict]:
    """Validate the test nodes with the given edges; return the pass flag and the source report."""
    nodes_file = write_jsonl(tmp_path / "nodes.jsonl", NODES)
    edges_file = write_jsonl(tmp_path / "edges.jsonl", edges)
    passed = validate_kgx(nodes_file=nodes_file, edges_file=edges_file, output_dir=tmp_path)
    report = json.loads((tmp_path / IngestFileName.VALIDATION_REPORT_FILE).read_text())
    return passed, report["sources"]["single_validation"]


def test_get_infores_catalog_reads_a_release():
    catalog = get_infores_catalog()
    assert catalog.version[0].isdigit()
    assert f"/v{catalog.version}/" in catalog.url
    assert len(catalog.statuses) > 100


@pytest.mark.parametrize(
    "infores_id,expected",
    [
        ("infores:bgee", InforesStatus.VALID),
        ("infores:automat-cord19", InforesStatus.DEPRECATED),
        ("infores:not-a-real-source", InforesStatus.INVALID),
    ],
)
def test_real_catalog_statuses(infores_id: str, expected: InforesStatus):
    assert get_infores_catalog().get_infores_status(infores_id) == expected


def test_infores_constants_are_in_catalog() -> None:
    """Warn about infores constants in biolink.py that are not in the registry catalog.

    This warns rather than fails: an unregistered infores may live in the repo during development,
    validation still blocks them from being included in released graphs.
    """
    catalog = get_infores_catalog()
    constants = {name: value for name, value in vars(biolink).items() if name.startswith("INFORES_")}
    assert constants
    invalid = {
        name: value
        for name, value in constants.items()
        if catalog.get_infores_status(value) == InforesStatus.INVALID
    }
    if invalid:
        warnings.warn(
            f"Invalid infores constants, not found in the catalog (v{catalog.version}): {invalid}. "
            "These must be added to the infores catalog before graphs can be released with them.",
            stacklevel=1,
        )


def test_validate_kgx_passes_with_valid_infores(tmp_path: Path):
    edges = [
        make_edge("e1", [
            {"resource_id": "infores:medlineplus", "resource_role": "primary_knowledge_source"},
            {"resource_id": "infores:diseases", "resource_role": "aggregator_knowledge_source",
             "upstream_resource_ids": ["infores:medlineplus"]},
        ]),
    ]
    passed, report = run_validation(tmp_path, edges)
    assert passed
    assert report["validation_status"] == ValidationStatus.PASSED
    assert report["issues"]["invalid_infores"] == {}
    assert report["statistics"]["distinct_infores_ids"] == 2
    assert report["infores_catalog"] == get_infores_catalog().provenance()


def test_validate_kgx_fails_with_invalid_infores(tmp_path: Path):
    edges = [
        make_edge("e1", [{"resource_id": "infores:not-a-real-source", "resource_role": "primary_knowledge_source"}]),
        make_edge("e2", [
            {"resource_id": "infores:bgee", "resource_role": "primary_knowledge_source",
             "upstream_resource_ids": ["infores:not-a-real-source"]},
        ]),
    ]
    passed, report = run_validation(tmp_path, edges)
    assert not passed
    assert report["validation_status"] == ValidationStatus.FAILED
    assert report["issues"]["invalid_infores"] == {"infores:not-a-real-source": 2}
    assert report["statistics"]["invalid_infores_count"] == 1
    infores_errors = [error for error in report["issues"]["errors"] if error["type"] == "infores-validation"]
    assert len(infores_errors) == 1
    assert "infores:not-a-real-source" in infores_errors[0]["message"]


def test_validate_kgx_warns_on_deprecated_infores(tmp_path: Path):
    edges = [
        make_edge("e1", [{"resource_id": "infores:automat-cord19", "resource_role": "primary_knowledge_source"}]),
    ]
    passed, report = run_validation(tmp_path, edges)
    assert passed
    assert report["issues"]["deprecated_infores"] == {"infores:automat-cord19": 1}
    assert report["statistics"]["deprecated_infores_count"] == 1
    assert any(
        warning["type"] == "infores-validation" and "infores:automat-cord19" in warning["message"]
        for warning in report["issues"]["warnings"]
    )
