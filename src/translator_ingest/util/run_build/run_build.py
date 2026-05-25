
import json
import sys
from pathlib import Path

import click

from translator_ingest import INGESTS_PARSER_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.run_build.orchestrator import run_full_build
from translator_ingest.util.run_build.utils import MEMORY_CRITICAL_THRESHOLD_PERCENT

logger = get_logger(__name__)


# ── CLI ───────────────────────────────────────────────────────────────────────

# Private prefixes to exclude from auto-discovery
_INGEST_EXCLUDE_PREFIXES = ("_", ".")


def discover_ingest_sources() -> list[str]:
    ingests_dir = Path(INGESTS_PARSER_PATH)
    sources = []
    for item in sorted(ingests_dir.iterdir()):
        if not item.is_dir():
            continue
        if any(item.name.startswith(p) for p in _INGEST_EXCLUDE_PREFIXES):
            continue
        if item.name == "__pycache__":
            continue
        # Verify it has a transform module (not just a stray directory)
        if (item / f"{item.name}.py").exists():
            sources.append(item.name)
    return sources


@click.command()
@click.option("--sources", type=str, default=None, help="Space-separated list of sources (default: all)")
@click.option("--graph-id", type=str, default="translator_kg", help="Merged graph ID")
@click.option("--node-properties", type=str, default="ncbi_gene", help="Space-separated node-property-only sources")
@click.option("--overwrite", is_flag=True, help="Overwrite previously generated files")
@click.option("--no-upload", is_flag=True, help="Skip S3 upload stage")
@click.option("--max-workers", type=int, default=None, help="Max parallel workers for parallel RUN phase")
@click.option(
    "--memory-threshold",
    type=float,
    default=None,
    help=(
        f"System memory critical threshold in percent "
        f"(default: {MEMORY_CRITICAL_THRESHOLD_PERCENT:.0f}). "
        f"Build aborts gracefully when exceeded."
    ),
)
@click.option(
    "--sequential-sources",
    type=str,
    default="ctd semmeddb",
    help="Space-separated sources to run one-at-a-time before the parallel batch (default: 'ctd semmeddb')",
)
def main(
    sources: str | None,
    graph_id: str,
    node_properties: str,
    overwrite: bool,
    no_upload: bool,
    max_workers: int | None,
    memory_threshold: float | None,
    sequential_sources: str,
) -> None:
    """Run the full translator-ingests pipeline build end-to-end.

    Stages: RUN (sequential-first then parallel) -> MERGE -> RELEASE -> UPLOAD

    Per-stage logs are written live to logs/{stage}/{timestamp}/.
    JSON artifacts and build reports go to reports/{timestamp}/.
    """
    if sources and sources.strip():
        source_list = sources.split()
    else:
        source_list = discover_ingest_sources()
        logger.info("Auto-discovered %d sources from ingests/ directory", len(source_list))
    node_props = node_properties.split() if node_properties else ["ncbi_gene"]
    seq_sources = sequential_sources.split() if sequential_sources and sequential_sources.strip() else []

    _report_dir, _error_log_path, memory_aborted = run_full_build(
        sources=source_list,
        graph_id=graph_id,
        node_properties=node_props,
        overwrite=overwrite,
        upload=not no_upload,
        max_workers=max_workers,
        memory_critical_percent=memory_threshold,
        sequential_sources=seq_sources,
    )

    # Only exit(2) if stages were skipped because memory was still critical
    # at stage start. A spike during RUN that recovered before MERGE is not
    # an abort — it's a warning that appears in the report notes.
    stages_skipped_for_memory = [
        s for s in ("MERGE", "RELEASE")  # UPLOAD skip due to --no-upload is normal
        if (_report_dir / "stages" / s.lower() / "_summary.json").exists() is False
        and memory_aborted
    ]
    if stages_skipped_for_memory:
        logger.error(
            "BUILD INCOMPLETE: Memory was still critical at stage start — "
            "%s were skipped. See report for details.",
            ", ".join(stages_skipped_for_memory),
        )
        sys.exit(2)

    # Exit non-zero if any source failed or any stage failed
    run_summary_path = _report_dir / "stages" / "run" / "_summary.json"
    has_failures = False
    if run_summary_path.exists():
        with run_summary_path.open() as f:
            run_summary = json.load(f)
        has_failures = run_summary.get("failed", 0) > 0

    if not has_failures:
        for stage in ("merge", "release", "upload"):
            stage_summary_path = _report_dir / "stages" / stage / "_summary.json"
            if stage_summary_path.exists():
                with stage_summary_path.open() as f:
                    stage_data = json.load(f)
                if stage_data.get("status") == "failed":
                    has_failures = True
                    break

    sys.exit(1 if has_failures else 0)


if __name__ == "__main__":
    main()
