"""Support for the Tablassert ingest kind.

A Tablassert ingest is declared by the presence of a `tablassert.yaml` file in the ingest
directory. The declared upstream files (nodes, edges, and the Resource Ingest Guide) are
downloaded like any other source data, then the Tablassert CLI validates the graph and prunes
non-compliant edges inside an isolated `uvx` environment. No local transform code runs: whatever
the upstream CLI publishes is what this pipeline publishes.

The CLI selection defaults to `latest` so Tablassert releases keep flowing. The complete identity
of the isolated environment is resolved exactly once per build, recorded in the transform metadata,
and folded into the build version, so a newer CLI or changed input bytes produce a new build
instead of silently reusing a previous one.
"""

import hashlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from importlib.metadata import version as installed_package_version
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml
from pydantic import BaseModel, ConfigDict, ValidationError, field_validator, model_validator

from translator_ingest import INGESTS_PARSER_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.storage.local import (
    IngestFileType,
    get_source_data_directory,
    get_transform_directory,
    get_versioned_file_paths,
    write_ingest_file,
)

logger = get_logger(__name__)

TABLASSERT_CONFIG_FILENAME = "tablassert.yaml"
# Filenames an ingest directory or its declared paths may not use.
RESERVED_FILENAMES = frozenset({"download.yaml", TABLASSERT_CONFIG_FILENAME})
NODES_PATH_SUFFIX = "nodes.jsonl"
EDGES_PATH_SUFFIX = "edges.jsonl"
DEFAULT_CLI_VERSION = "latest"
CLI_VERSION_PATTERN = re.compile(r"^(latest|(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)$)")
DATASET_VERSION_PLACEHOLDER = "{version}"
# Pruning a real graph takes a while; give the isolated CLI an hour before giving up on it.
PRUNE_TIMEOUT_SECONDS = 3600

RESOLVER_SNIPPET = (
    "import json; from importlib.metadata import version; "
    'print(json.dumps({"tablassert": version("tablassert"), "biolink-model": version("biolink-model")}))'
)


class TablassertConfigError(ValueError):
    """Raised when a source's Tablassert declaration is missing, ambiguous, or invalid."""


class RemoteFile(BaseModel):
    """A remote file an ingest needs: where to fetch it and the bare local filename to store it under."""

    model_config = ConfigDict(extra="forbid")

    # A plain validated string, not HttpUrl: declared URLs carry `{version}` placeholders for the
    # dataset release, which strict URL parsers percent-encode.
    url: str
    path: str

    @field_validator("url")
    @classmethod
    def url_must_be_absolute_http(cls, value: str) -> str:
        parsed = urlparse(value)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError(f"url must be an absolute http(s) URL, got: {value!r}")
        return value

    @field_validator("path")
    @classmethod
    def path_must_be_bare_filename(cls, value: str) -> str:
        if not value or value in (".", ".."):
            raise ValueError(f"path must be a non-empty filename, got: {value!r}")
        if "/" in value or "\\" in value or Path(value).name != value:
            raise ValueError(f"path must be a bare filename without directory components, got: {value!r}")
        if value in RESERVED_FILENAMES:
            raise ValueError(f"path {value!r} is reserved and may not be used by a declared file")
        return value


class TablassertIngest(BaseModel):
    """The parsed and validated contents of a `tablassert.yaml` file.

    `version` selects the Tablassert CLI: `latest` by default, or an exact `X.Y.Z` release for
    replay. `{version}` placeholders in the declared URLs are the dataset release, never the CLI
    version.
    """

    model_config = ConfigDict(extra="forbid")

    version: str = DEFAULT_CLI_VERSION
    nodes: RemoteFile
    edges: RemoteFile
    rig: RemoteFile

    @field_validator("version")
    @classmethod
    def version_must_be_latest_or_semver(cls, value: str) -> str:
        if not CLI_VERSION_PATTERN.match(value):
            raise ValueError(f"version must be 'latest' or an exact X.Y.Z release, got: {value!r}")
        return value

    @model_validator(mode="after")
    def paths_must_be_distinct_and_typed(self) -> "TablassertIngest":
        paths = [self.nodes.path, self.edges.path, self.rig.path]
        if len(set(paths)) != len(paths):
            raise ValueError(f"declared file paths must be distinct, got: {paths}")
        if not self.nodes.path.endswith(NODES_PATH_SUFFIX):
            raise ValueError(f"nodes path must end in {NODES_PATH_SUFFIX}, got: {self.nodes.path!r}")
        if not self.edges.path.endswith(EDGES_PATH_SUFFIX):
            raise ValueError(f"edges path must end in {EDGES_PATH_SUFFIX}, got: {self.edges.path!r}")
        return self


@dataclass
class CliIdentity:
    """The resolved identity of the isolated Tablassert CLI environment for one build."""

    requested: str
    tablassert_version: str
    biolink_version: str


def config_path(source: str) -> Path:
    return INGESTS_PARSER_PATH / source / TABLASSERT_CONFIG_FILENAME


def declares(source: str) -> bool:
    """Return True when the source explicitly declares the Tablassert kind via `tablassert.yaml`."""
    return config_path(source).exists()


def load(source: str) -> TablassertIngest:
    """Load and validate a source's Tablassert declaration.

    Args:
        source: id of the source whose ingest directory holds `tablassert.yaml`.

    Returns:
        The validated TablassertIngest declaration.

    Raises:
        TablassertConfigError: when the declaration is missing, ambiguous with a Koza config or
            legacy download manifest, or fails validation.
    """
    path = config_path(source)
    if not path.exists():
        raise TablassertConfigError(f"{path} not found; a Tablassert ingest must declare {TABLASSERT_CONFIG_FILENAME}")
    koza_config_path = INGESTS_PARSER_PATH / source / f"{source}.yaml"
    if koza_config_path.exists():
        raise TablassertConfigError(
            f"{source} declares both {TABLASSERT_CONFIG_FILENAME} and {koza_config_path.name}; "
            "an ingest must declare exactly one kind."
        )
    legacy_download_path = INGESTS_PARSER_PATH / source / "download.yaml"
    if legacy_download_path.exists():
        raise TablassertConfigError(
            f"{source} declares a legacy {legacy_download_path.name}; a Tablassert ingest declares its "
            "files in tablassert.yaml instead."
        )
    try:
        with path.open("r", encoding="utf-8") as config_file:
            return TablassertIngest(**yaml.safe_load(config_file))
    except (yaml.YAMLError, ValidationError, TypeError) as error:
        raise TablassertConfigError(f"Invalid Tablassert declaration in {path}: {error}") from error


def resolve_dataset_version_placeholder(url: str, dataset_version: str) -> str:
    """Substitute the dataset release version for `{version}` placeholders in a declared URL."""
    resolved = url.replace(DATASET_VERSION_PLACEHOLDER, dataset_version)
    if "{" in resolved or "}" in resolved:
        raise TablassertConfigError(f"URL still contains a placeholder after substitution: {resolved}")
    return resolved


def write_download_yaml(source: str, config: TablassertIngest, dataset_version: str, output_dir: Path) -> Path:
    """Write the source's download manifest for the existing kghub-downloader.

    The manifest is durable (it stays in the source's data directory as a record of exactly which
    URLs were used for this version) and its URLs already carry the dataset version, so the
    downloader's own version substitution is a no-op.

    Args:
        source: id of the source, recorded in the manifest as a comment header entry.
        config: the validated Tablassert declaration.
        dataset_version: the dataset release to substitute into `{version}` placeholders.
        output_dir: directory to write `download.yaml` into.

    Returns:
        Path to the written manifest.
    """
    entries = []
    for declared in (config.nodes, config.edges, config.rig):
        entries.append({
            "url": resolve_dataset_version_placeholder(declared.url, dataset_version),
            "local_name": declared.path,
        })
    manifest_path = output_dir / "download.yaml"
    with manifest_path.open("w", encoding="utf-8") as manifest_file:
        yaml.safe_dump(entries, manifest_file, sort_keys=False)
    logger.info(f"Wrote download manifest for {source} ({dataset_version}) to {manifest_path}")
    return manifest_path


def biolink_mismatch_message(repo_biolink_version: str, resolved_biolink_version: str) -> str | None:
    """Describe a biolink-model version difference between the repo pin and the isolated CLI, or None."""
    if repo_biolink_version == resolved_biolink_version:
        return None
    return (
        f"The isolated Tablassert environment validated against biolink-model {resolved_biolink_version}, "
        f"but this repo pins biolink-model {repo_biolink_version}. Continuing with the mismatch; later "
        "repo-side validation uses the repo's model and may disagree with the CLI's validation."
    )


def resolve_cli_identity(cli_version: str) -> CliIdentity:
    """Resolve the complete identity of the isolated Tablassert CLI environment, exactly once per build.

    Args:
        cli_version: the configured CLI selection, `latest` or an exact `X.Y.Z`.

    Returns:
        The resolved CliIdentity.

    Raises:
        RuntimeError: when uvx or the resolver fails, or reports unusable output.
    """
    from_spec = f"tablassert[cli]=={cli_version}" if cli_version != DEFAULT_CLI_VERSION else "tablassert[cli]@latest"
    command = [
        "uvx", "--python", f"{sys.version_info.major}.{sys.version_info.minor}",
        "--from", from_spec, "python", "-c", RESOLVER_SNIPPET,
    ]
    started = time.perf_counter()
    result = subprocess.run(command, capture_output=True, text=True, shell=False, timeout=PRUNE_TIMEOUT_SECONDS)
    duration = time.perf_counter() - started
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to resolve the Tablassert CLI environment ({from_spec}) after {duration:.1f}s. "
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    try:
        resolved = json.loads(result.stdout.strip().splitlines()[-1])
        identity = CliIdentity(
            requested=cli_version, tablassert_version=resolved["tablassert"], biolink_version=resolved["biolink-model"]
        )
    except (ValueError, KeyError) as error:
        raise RuntimeError(
            f"Could not parse the Tablassert CLI resolver output ({from_spec}): {result.stdout!r}"
        ) from error
    logger.info(
        f"Resolved Tablassert CLI environment ({from_spec}) in {duration:.1f}s: "
        f"tablassert {identity.tablassert_version}, biolink-model {identity.biolink_version}"
    )
    return identity


def build_prune_command(identity: CliIdentity, nodes_path: Path, edges_path: Path) -> list[str]:
    """Build the fixed argv for the isolated, pinned Tablassert prune of staged graph files."""
    return [
        "uvx", "--python", f"{sys.version_info.major}.{sys.version_info.minor}",
        "--from", f"tablassert[cli]=={identity.tablassert_version}",
        "--with", f"biolink-model=={identity.biolink_version}",
        "tablassert", "validate-kgx", "--nodes", str(nodes_path), "--edges", str(edges_path), "--prune",
    ]


def sha256_files(*paths: Path) -> str:
    """Compute one digest over the bytes of the given files, in the given order."""
    digest = hashlib.sha256()
    for path in paths:
        digest.update(path.read_bytes())
    return digest.hexdigest()


def count_records(path: Path) -> int:
    """Count the non-empty lines (records) in an NDJSON file."""
    with path.open("r", encoding="utf-8") as records_file:
        return sum(1 for line in records_file if line.strip())


def run_prune(command: list[str], working_directory: Path) -> tuple[int, str, str, float]:
    """Run the prune command in the staging directory and return (returncode, stdout, stderr, duration)."""
    started = time.perf_counter()
    result = subprocess.run(
        command, capture_output=True, text=True, shell=False, cwd=str(working_directory),
        timeout=PRUNE_TIMEOUT_SECONDS,
    )
    duration = time.perf_counter() - started
    return result.returncode, result.stdout, result.stderr, duration


def _declared_paths(source_data_dir: Path, config: TablassertIngest) -> tuple[Path, Path, Path]:
    nodes_path = source_data_dir / config.nodes.path
    edges_path = source_data_dir / config.edges.path
    rig_path = source_data_dir / config.rig.path
    missing = [str(path) for path in (nodes_path, edges_path, rig_path) if not path.is_file()]
    if missing:
        raise FileNotFoundError(f"Declared Tablassert input(s) missing for the build: {missing}")
    return nodes_path, edges_path, rig_path


def _publish(staging_dir: Path, names: list[str], transform_output_dir: Path) -> None:
    for name in names:
        shutil.move(str(staging_dir / name), str(transform_output_dir / name))


def transform(pipeline_metadata) -> dict[str, Any]:
    """Run the Tablassert ingest kind for one source: verify, stage, prune, and publish.

    Downloads are never modified: the CLI prunes a staged copy of the edges inside a temporary
    directory, and the staged graph pair and RIG snapshot are only moved into the transform
    directory after a successful run. A failed run leaves the downloaded inputs untouched and no
    completion marker behind.

    Args:
        pipeline_metadata: metadata identifying the source, source_version, and transform_version.

    Returns:
        The transform metadata dictionary, also written to the transform metadata file and reflected
        onto pipeline_metadata's Tablassert identity fields.

    Raises:
        TablassertConfigError: when the source has no valid declaration.
        FileNotFoundError: when declared inputs are missing.
        RuntimeError: when the isolated CLI fails or publishes an empty graph.
    """
    source = pipeline_metadata.source
    config = load(source)
    # A rebuild starts by clearing the transform directory: it holds only derived, regenerable
    # artifacts, and wiping it up front guarantees that a failure anywhere in resolution, pruning,
    # or publication cannot leave a previous complete generation behind for a later non-overwrite
    # run to silently reuse.
    transform_output_dir = get_transform_directory(pipeline_metadata)
    if transform_output_dir.exists():
        shutil.rmtree(transform_output_dir)
    transform_output_dir.mkdir(parents=True, exist_ok=True)
    source_data_dir = get_source_data_directory(pipeline_metadata)
    nodes_path, edges_path, rig_path = _declared_paths(source_data_dir, config)
    input_digest = sha256_files(nodes_path, edges_path, rig_path)

    identity = resolve_cli_identity(config.version)
    repo_biolink_version = installed_package_version("biolink-model")
    mismatch = biolink_mismatch_message(repo_biolink_version, identity.biolink_version)
    if mismatch:
        logger.warning(mismatch)

    transform_output_dir = get_transform_directory(pipeline_metadata)
    started = time.perf_counter()
    with tempfile.TemporaryDirectory(dir=transform_output_dir, prefix="tablassert_staging_") as staging:
        staging_dir = Path(staging)
        staged_nodes = staging_dir / config.nodes.path
        staged_edges = staging_dir / config.edges.path
        staged_rig = staging_dir / config.rig.path
        shutil.copy2(nodes_path, staged_nodes)
        shutil.copy2(edges_path, staged_edges)
        shutil.copy2(rig_path, staged_rig)

        command = build_prune_command(identity, staged_nodes, staged_edges)
        returncode, stdout, stderr, duration = run_prune(command, staging_dir)
        logger.info(f"Tablassert CLI finished for {source} in {duration:.1f}s with exit code {returncode}")
        if returncode != 0:
            raise RuntimeError(
                f"Tablassert validation failed for {source} (exit code {returncode}).\n"
                f"stdout:\n{stdout}\nstderr:\n{stderr}"
            )
        if not staged_edges.is_file() or not staged_nodes.is_file():
            raise RuntimeError(f"Tablassert CLI did not produce a complete graph pair for {source}")
        edge_count = count_records(staged_edges)
        node_count = count_records(staged_nodes)
        if edge_count == 0 or node_count == 0:
            raise RuntimeError(
                f"Tablassert pruning left an unusable graph for {source} "
                f"({node_count} nodes, {edge_count} edges); refusing to publish."
            )
        _publish(staging_dir, [config.nodes.path, config.edges.path, config.rig.path], transform_output_dir)

    pipeline_metadata.tablassert_version = identity.tablassert_version
    pipeline_metadata.tablassert_biolink_version = identity.biolink_version
    pipeline_metadata.input_digest = input_digest
    transform_metadata = {
        "source": source,
        "source_version": pipeline_metadata.source_version,
        "transform_version": pipeline_metadata.transform_version,
        "transform_duration": f"{time.perf_counter() - started:.1f}",
        "ingest_kind": "tablassert",
        "requested_cli_version": config.version,
        "tablassert_version": identity.tablassert_version,
        "tablassert_biolink_version": identity.biolink_version,
        "repo_biolink_version": repo_biolink_version,
        "biolink_mismatch": mismatch is not None,
        "input_digest": input_digest,
        "node_count": node_count,
        "edge_count": edge_count,
        "prune_report": stderr.strip(),
    }
    write_ingest_file(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE,
        pipeline_metadata=pipeline_metadata,
        data=transform_metadata,
    )
    logger.info(f"Published {node_count} nodes and {edge_count} edges for {source} from the Tablassert CLI.")
    return transform_metadata


def adapter_files() -> list[Path]:
    """The shared source files that implement this ingest kind's behavior.

    get_transform_version() hashes these alongside the ingest-local files, so changes to the
    adapter's validation, pruning, or publication semantics invalidate existing Tablassert
    transforms just like an ingest-local code change would.
    """
    from translator_ingest.util import hf_releases

    return [Path(__file__), Path(hf_releases.__file__)]


def rig_snapshot_path(pipeline_metadata) -> Path | None:
    """Return the RIG snapshot staged by this build's transform, or None for non-Tablassert sources."""
    if not declares(pipeline_metadata.source):
        return None
    config = load(pipeline_metadata.source)
    return get_transform_directory(pipeline_metadata) / config.rig.path


def load_execution_identity(pipeline_metadata) -> None:
    """Restore the recorded Tablassert identity onto pipeline_metadata from the transform metadata.

    Cached transforms skip re-running the CLI, so the identity that produced the existing outputs is
    read back from disk. That keeps the build version stable across cached reruns while still
    changing whenever the recorded identity or digest changes.
    """
    if not declares(pipeline_metadata.source):
        return
    transform_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    if not transform_metadata_path.exists():
        return
    with transform_metadata_path.open("r", encoding="utf-8") as transform_metadata_file:
        recorded = json.load(transform_metadata_file)
    pipeline_metadata.tablassert_version = recorded.get("tablassert_version")
    pipeline_metadata.tablassert_biolink_version = recorded.get("tablassert_biolink_version")
    pipeline_metadata.input_digest = recorded.get("input_digest")
