"""Ingest-level metadata helpers that read ``ingests/{source}/{source}.yaml``.

Used by Makefile targets and build tooling that need to introspect koza
configuration *without* spinning up the full pipeline. The only reader in
this module is ``yaml.safe_load`` — we deliberately mirror the schema
rather than instantiating koza's config parser so these helpers stay cheap
to call for every source during a ``make`` invocation.

CLI (used by the Makefile)::

    uv run python -m translator_ingest.ingest_config nodes-only ctd go_cam ncbi_gene
"""

from collections.abc import Iterable
from pathlib import Path

import click
import yaml

from translator_ingest import INGESTS_PARSER_PATH


def _ingest_yaml_path(source: str, ingests_dir: Path = INGESTS_PARSER_PATH) -> Path:
    """Location of the koza config yaml for ``source``."""
    return Path(ingests_dir) / source / f"{source}.yaml"


def is_nodes_only_source(source: str, ingests_dir: Path = INGESTS_PARSER_PATH) -> bool:
    """Return True if ``source`` is a nodes-only ingest.

    A source is nodes-only when its koza config has ``writer.max_edge_count: 0``.
    This matches what ``pipeline.load_koza_config`` reads into
    ``pipeline_metadata.koza_config['max_edge_count']``.

    Missing yaml, missing ``writer`` block, or a non-zero ``max_edge_count``
    all return False.

    Examples:
        >>> # ncbi_gene.yaml has `writer.max_edge_count: 0`
        >>> is_nodes_only_source("ncbi_gene")
        True
        >>> is_nodes_only_source("go_cam")
        False
    """
    path = _ingest_yaml_path(source, ingests_dir)
    if not path.exists():
        return False
    with path.open() as f:
        config = yaml.safe_load(f) or {}
    writer = config.get("writer") or {}
    return writer.get("max_edge_count") == 0


def get_nodes_only_sources(
    sources: Iterable[str],
    ingests_dir: Path = INGESTS_PARSER_PATH,
) -> list[str]:
    """Return the subset of ``sources`` that are nodes-only ingests.

    Order is preserved to mirror the caller's list (Makefile ``$(SOURCES)``).

    Examples:
        >>> get_nodes_only_sources(["ctd", "ncbi_gene", "go_cam"])
        ['ncbi_gene']
    """
    return [s for s in sources if is_nodes_only_source(s, ingests_dir)]


@click.group()
def cli() -> None:
    """Inspect ingest-level koza configuration."""


@cli.command("nodes-only")
@click.argument("sources", nargs=-1)
def _nodes_only(sources: tuple[str, ...]) -> None:
    """Print the subset of nodes-only SOURCES``.

    Output is a space-separated list on a single line, suitable for Make's
    ``$(shell ...)``. Prints nothing (exit 0) when no sources qualify.
    """
    click.echo(" ".join(get_nodes_only_sources(sources)))


if __name__ == "__main__":
    cli()