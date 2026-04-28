"""Resolver for multi-source knowledge-graph definitions declared in graphs.yaml.

A graph entry has a ``graph_id`` and either an explicit ``sources`` list or a
``base`` reference to another graph (optionally adjusted with ``includes`` and
``excludes``).  Resolution flattens ``base`` chains, applies excludes, then
applies includes, returning a source list.

CLI ::

    get the list of all available graph ids
    uv run python -m translator_ingest.graphs list
    
    get the list of sources for a particular graph_id
    uv run python -m translator_ingest.graphs sources <graph_id>
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import click
import yaml


DEFAULT_GRAPHS_YAML = Path(__file__).resolve().parents[2] / "graphs.yaml"


class GraphConfigError(ValueError):
    """Raised when graphs.yaml is malformed or a graph_id cannot be resolved."""


def load_graphs(path: Path = DEFAULT_GRAPHS_YAML) -> dict[str, dict[str, Any]]:
    """Load graphs.yaml and return a ``{graph_id: entry}`` mapping.

    Validates structural invariants:
      - Top-level ``graphs`` must be a list.
      - Each entry must have a ``graph_id``.
      - ``graph_id`` must be unique.
      - An entry must declare exactly one of ``sources`` or ``base``.
      - ``includes`` / ``excludes`` require ``base``.

    Raises:
        GraphConfigError: if the file is missing or malformed.
    """
    if not path.exists():
        raise GraphConfigError(f"graphs.yaml not found at {path}")

    with path.open() as f:
        data = yaml.safe_load(f) or {}

    entries = data.get("graphs")
    if not isinstance(entries, list):
        raise GraphConfigError("graphs.yaml must have a top-level 'graphs:' list")

    by_id: dict[str, dict[str, Any]] = {}
    for entry in entries:
        if not isinstance(entry, dict):
            raise GraphConfigError(f"Each graphs entry must be a mapping, got: {entry!r}")

        graph_id = entry.get("graph_id")
        if not graph_id or not isinstance(graph_id, str):
            raise GraphConfigError(f"Entry missing string 'graph_id': {entry!r}")

        if graph_id in by_id:
            raise GraphConfigError(f"Duplicate graph_id: {graph_id}")

        has_sources = "sources" in entry
        has_base = "base" in entry
        if has_sources == has_base:
            raise GraphConfigError(
                f"Graph {graph_id!r} must declare exactly one of 'sources' or 'base'"
            )

        if ("includes" in entry or "excludes" in entry) and not has_base:
            raise GraphConfigError(
                f"Graph {graph_id!r}: 'includes'/'excludes' require 'base'"
            )

        by_id[graph_id] = entry

    return by_id


def _resolve(
    graph_id: str,
    by_id: dict[str, dict[str, Any]],
    _visiting: frozenset[str] = frozenset(),
) -> list[str]:
    """Resolve a graph_id into its sorted list of source names."""
    if graph_id not in by_id:
        raise GraphConfigError(f"Unknown graph_id: {graph_id!r}")
    if graph_id in _visiting:
        chain = " -> ".join([*_visiting, graph_id])
        raise GraphConfigError(f"Circular 'base' reference: {chain}")

    entry = by_id[graph_id]

    if "sources" in entry:
        sources = set(entry["sources"])
    else:
        sources = set(_resolve(entry["base"], by_id, _visiting | {graph_id}))

    sources -= set(entry.get("excludes") or [])
    sources |= set(entry.get("includes") or [])
    return sorted(sources)


def resolve_sources(graph_id: str, path: Path = DEFAULT_GRAPHS_YAML) -> list[str]:
    """Return the resolved, sorted list of sources for ``graph_id``."""
    return _resolve(graph_id, load_graphs(path))


def list_graph_ids(path: Path = DEFAULT_GRAPHS_YAML) -> list[str]:
    """Return all declared graph_ids in declaration order."""
    return list(load_graphs(path).keys())


@click.group()
def cli() -> None:
    """Inspect graph definitions declared in graphs.yaml."""


@cli.command("list")
@click.option("--path", type=click.Path(path_type=Path), default=DEFAULT_GRAPHS_YAML,
              help="Path to graphs.yaml")
def _list(path: Path) -> None:
    """Print all declared graph_ids, one per line."""
    for gid in list_graph_ids(path):
        click.echo(gid)


@cli.command("sources")
@click.argument("graph_id")
@click.option("--path", type=click.Path(path_type=Path), default=DEFAULT_GRAPHS_YAML,
              help="Path to graphs.yaml")
def _sources(graph_id: str, path: Path) -> None:
    """Print the space-separated list of sources for GRAPH_ID."""
    try:
        click.echo(" ".join(resolve_sources(graph_id, path)))
    except GraphConfigError as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    cli()