"""Tests for the graphs.yaml resolver."""

from pathlib import Path

import pytest
import yaml

from translator_ingest.graphs import (
    DEFAULT_GRAPHS_YAML,
    GraphConfigError,
    list_graph_ids,
    load_graphs,
    resolve_sources,
)


def _write_yaml(path: Path, data: dict) -> Path:
    path.write_text(yaml.safe_dump(data))
    return path


@pytest.fixture
def graphs_file(tmp_path):
    def _make(entries):
        return _write_yaml(tmp_path / "graphs.yaml", {"graphs": entries})
    return _make


# ── resolution ────────────────────────────────────────────────────────────────

def test_sources_only_returns_sorted_list(graphs_file):
    path = graphs_file([{"graph_id": "g", "sources": ["c", "a", "b"]}])
    assert resolve_sources("g", path) == ["a", "b", "c"]


def test_base_inherits_sources(graphs_file):
    path = graphs_file([
        {"graph_id": "parent", "sources": ["a", "b", "c"]},
        {"graph_id": "child", "base": "parent"},
    ])
    assert resolve_sources("child", path) == ["a", "b", "c"]


def test_excludes_removes_from_base(graphs_file):
    path = graphs_file([
        {"graph_id": "parent", "sources": ["a", "b", "c"]},
        {"graph_id": "child", "base": "parent", "excludes": ["b"]},
    ])
    assert resolve_sources("child", path) == ["a", "c"]


def test_includes_adds_to_base(graphs_file):
    path = graphs_file([
        {"graph_id": "parent", "sources": ["a", "b"]},
        {"graph_id": "child", "base": "parent", "includes": ["c"]},
    ])
    assert resolve_sources("child", path) == ["a", "b", "c"]


def test_excludes_then_includes(graphs_file):
    # Includes should win over excludes when a name appears in both.
    path = graphs_file([
        {"graph_id": "parent", "sources": ["a", "b", "c"]},
        {"graph_id": "child", "base": "parent", "excludes": ["b"], "includes": ["b", "d"]},
    ])
    assert resolve_sources("child", path) == ["a", "b", "c", "d"]


def test_multi_level_base(graphs_file):
    path = graphs_file([
        {"graph_id": "g1", "sources": ["a", "b", "c", "d"]},
        {"graph_id": "g2", "base": "g1", "excludes": ["d"]},
        {"graph_id": "g3", "base": "g2", "excludes": ["a"]},
    ])
    assert resolve_sources("g3", path) == ["b", "c"]


# ── translator_kg_open against real graphs.yaml ──────────────────────────────

def test_translator_kg_open_excludes_closed_sources():
    resolved = resolve_sources("translator_kg_open", DEFAULT_GRAPHS_YAML)
    parent = resolve_sources("translator_kg", DEFAULT_GRAPHS_YAML)
    assert "ctd" in parent and "semmeddb" in parent
    assert "ctd" not in resolved
    assert "semmeddb" not in resolved


def test_list_graph_ids_preserves_declaration_order():
    ids = list_graph_ids(DEFAULT_GRAPHS_YAML)
    assert ids[0] == "translator_kg"
    assert "translator_kg_open" in ids


# ── validation errors ────────────────────────────────────────────────────────

def test_missing_graph_id_raises(graphs_file):
    path = graphs_file([{"sources": ["a"]}])
    with pytest.raises(GraphConfigError, match="graph_id"):
        load_graphs(path)


def test_duplicate_graph_id_raises(graphs_file):
    path = graphs_file([
        {"graph_id": "g", "sources": ["a"]},
        {"graph_id": "g", "sources": ["b"]},
    ])
    with pytest.raises(GraphConfigError, match="Duplicate"):
        load_graphs(path)


def test_must_declare_sources_or_base(graphs_file):
    path = graphs_file([{"graph_id": "g"}])
    with pytest.raises(GraphConfigError, match="sources.*base"):
        load_graphs(path)


def test_cannot_declare_both_sources_and_base(graphs_file):
    path = graphs_file([
        {"graph_id": "parent", "sources": ["a"]},
        {"graph_id": "g", "sources": ["a"], "base": "parent"},
    ])
    with pytest.raises(GraphConfigError, match="sources.*base"):
        load_graphs(path)


def test_includes_excludes_require_base(graphs_file):
    path = graphs_file([{"graph_id": "g", "sources": ["a"], "excludes": ["a"]}])
    with pytest.raises(GraphConfigError, match="require 'base'"):
        load_graphs(path)


def test_unknown_base_raises(graphs_file):
    path = graphs_file([{"graph_id": "child", "base": "nope"}])
    with pytest.raises(GraphConfigError, match="Unknown graph_id"):
        resolve_sources("child", path)


def test_cycle_detection(graphs_file):
    path = graphs_file([
        {"graph_id": "a", "base": "b"},
        {"graph_id": "b", "base": "a"},
    ])
    with pytest.raises(GraphConfigError, match="Circular"):
        resolve_sources("a", path)


def test_missing_file_raises(tmp_path):
    with pytest.raises(GraphConfigError, match="not found"):
        load_graphs(tmp_path / "does-not-exist.yaml")