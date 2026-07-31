"""Tests for ingest_config.is_nodes_only_source / get_nodes_only_sources."""

from pathlib import Path

import pytest

from translator_ingest.ingest_config import (
    get_nodes_only_sources,
    is_nodes_only_source,
)


def _write_yaml(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body)


@pytest.fixture
def ingests_dir(tmp_path):
    """Build a fake ingests/ tree with several sources for parametrized checks."""
    _write_yaml(tmp_path / "nodes_only/nodes_only.yaml", "writer:\n  max_edge_count: 0\n")
    _write_yaml(tmp_path / "has_edges/has_edges.yaml", "writer:\n  max_edge_count: 100\n")
    _write_yaml(tmp_path / "no_writer/no_writer.yaml", "reader:\n  format: csv\n")
    _write_yaml(tmp_path / "empty_writer/empty_writer.yaml", "writer:\n")
    return tmp_path


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("nodes_only", True),
        ("has_edges", False),
        ("no_writer", False),
        ("empty_writer", False),
        ("missing_source", False),
    ],
)
def test_is_nodes_only_source(ingests_dir, source, expected):
    assert is_nodes_only_source(source, ingests_dir) is expected


def test_get_nodes_only_sources_filters_and_preserves_order(ingests_dir):
    sources = ["has_edges", "nodes_only", "no_writer", "missing_source"]
    assert get_nodes_only_sources(sources, ingests_dir) == ["nodes_only"]


def test_get_nodes_only_sources_empty_input(ingests_dir):
    assert get_nodes_only_sources([], ingests_dir) == []


# ── Integration against the real repo ingests/ tree ──────────────────────────

def test_ncbi_gene_is_nodes_only():
    assert is_nodes_only_source("ncbi_gene") is True


def test_known_edge_source_is_not_nodes_only():
    assert is_nodes_only_source("go_cam") is False