
from dataclasses import dataclass, field
from pathlib import Path

from translator_ingest import INGESTS_DATA_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.storage.local import IngestFileName

logger = get_logger(__name__)


@dataclass(frozen=True)
class SourcePartition:
    # sources that succeeded this run — merge uses the freshly built data
    fresh: list[str] = field(default_factory=list)
    # sources that failed this run but have a prior LATEST_BUILD_FILE on disk —
    # merge uses the prior build (the file is only updated on success, so it
    # still points at the last good version)
    fallback: list[str] = field(default_factory=list)
    # sources that failed this run AND have no prior LATEST_BUILD_FILE —
    # the build cannot proceed; the orchestrator must hard-stop
    missing: list[str] = field(default_factory=list)

    @property
    def available(self) -> list[str]:
        # sources merge can include — fresh first, then fallback, both have a
        # LATEST_BUILD_FILE pointing at a valid build
        return self.fresh + self.fallback

    @property
    def has_missing(self) -> bool:
        return bool(self.missing)


def partition_sources_after_run(
    sources: list[str],
    failed_this_run: list[str],
    *,
    base_path: Path | None = None,
) -> SourcePartition:
    if base_path is None:
        base_path = Path(INGESTS_DATA_PATH)
    failed_set = set(failed_this_run)
    fresh: list[str] = []
    fallback: list[str] = []
    missing: list[str] = []
    for source in sources:
        if source not in failed_set:
            fresh.append(source)
            continue
        # source failed this run — decide between fallback and missing by
        # checking whether a prior LATEST_BUILD_FILE survived on disk
        latest_path = base_path / source / IngestFileName.LATEST_BUILD_FILE
        if latest_path.exists():
            fallback.append(source)
        else:
            missing.append(source)
    return SourcePartition(fresh=fresh, fallback=fallback, missing=missing)


def format_missing_sources_error(
    partition: SourcePartition,
    errors_by_source: dict[str, str] | None = None,
) -> str:
    errors_by_source = errors_by_source or {}
    lines = [
        "Cannot proceed to merge. The following sources failed this run AND",
        "have no prior successful build on disk:",
    ]
    for source in partition.missing:
        err = errors_by_source.get(source, "no error captured")
        lines.append(f"  - {source}: {err}")
    lines.append("")
    lines.append(
        "These sources have never produced a LATEST_BUILD_FILE, so no "
        "fallback data is available. Investigate the ingest, or check that "
        "persistent storage is mounted correctly (Kubernetes PVC, Jenkins "
        "workspace path)."
    )
    return "\n".join(lines)
