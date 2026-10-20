import json
import re
import yaml
from datetime import datetime, timezone
from dataclasses import dataclass, field, fields, asdict
from pathlib import Path
from typing import Any, Dict

from orion import KGXGraphMetadata, KGXKnowledgeSource

from translator_ingest import INGESTS_PARSER_PATH, INGESTS_RELEASES_PATH

# Matches a semantic version of the form MAJOR.MINOR.PATCH (e.g. "1.0.0").
SEMANTIC_VERSION_PATTERN = re.compile(r"^(\d+)\.(\d+)\.(\d+)$")

# ORION namespaces the graph metadata terms it defines as `orion:`. This pipeline computes the values it
# publishes, and they do not always mean what ORION's do - a build version here is an underscore-joined
# composite of pipeline versions, where ORION's is a hash of its own inputs. The terms are published under a
# translator-ingests vocabulary instead, so consumers can tell which vocabulary defines what they are reading.
ORION_VOCABULARY = "orion"
TRANSLATOR_VOCABULARY = "translator"
TRANSLATOR_VOCABULARY_URL = "https://github.com/NCATSTranslator/translator-ingests"

# Terms of the translator-ingests vocabulary that are referred to by name elsewhere.
TRANSLATOR_BUILD_VERSION = f"{TRANSLATOR_VOCABULARY}:buildVersion"
TRANSLATOR_NODE_COUNT = f"{TRANSLATOR_VOCABULARY}:nodeCount"
TRANSLATOR_EDGE_COUNT = f"{TRANSLATOR_VOCABULARY}:edgeCount"


@dataclass
class PipelineMetadata:
    # source: id of the source, corresponding to the ingest directory and file names
    source: str
    # source_version: version of the source data itself, as determined by a get_latest_version() function
    source_version: str | None = None
    # source_download_date: the timestamp (ISO 8601) the source data was actually fetched
    source_download_date: str | None = None
    # transform_version: version of the translator-ingests ingest code used to generate KGX files from the source data
    transform_version: str | None = None
    # The following are normalization versions (babel_version, node_normalizer_version, normalization_code_version)
    # These are managed by ORION, which can provide their current versions when a NormalizationScheme is initialized
    # without specifying them.
    # babel_version: version of upstream data backing the Node Normalizer and Name Resolver services
    babel_version: str | None = None
    # node_normalizer_version: version of the Node Normalizer API software
    node_normalizer_version: str | None = None
    # normalization_code_version: version of the ORION code that performs the normalization process
    normalization_code_version: str | None = None
    # conflation and strict are configurable settings which control their respective aspects
    # normalization_conflation: whether to conflate genes/proteins and drugs/chemicals
    normalization_conflation: bool = True
    # normalization_strict: enforce node normalization strictly and discard nodes that don't normalize and their edges
    normalization_strict: bool = True
    # merging_code_version: version of the ORION code that performs the merging process.
    merging_code_version: str | None = None
    biolink_version: str | None = None
    # build_version: a composite version representing all the dependencies which should be considered when determining
    # whether the pipeline needs to be run, or if an ingest was already fully completed
    build_version: str | None = None
    # build_date: the timestamp (ISO 8601) the build was generated
    build_date: str | None = None
    # release_version: a version assigned to a completed ingest when it is released
    # (moved to the releases directory, compressed, and distribution metadata generated)
    release_version: str | None = None
    # release_date: the timestamp (ISO 8601) the release was made
    release_date: str | None = None
    # data: a URL pointing to the pipeline stage artifacts used to process an entire ingest
    data: str | None = None
    # koza_config: the koza config yaml file associated with an ingest
    koza_config: Dict[str, Any] = field(default_factory=dict)
    # The following three fields are only set for Tablassert ingests. They record the complete
    # identity of the isolated Tablassert CLI that produced the outputs and one digest over the
    # downloaded inputs, so a newer CLI or changed upstream bytes produce a new build (and thus a
    # new release) instead of looking identical to the previous one.
    # tablassert_version: the exact Tablassert CLI version the isolated environment resolved to
    tablassert_version: str | None = None
    # tablassert_biolink_version: the biolink-model version inside that isolated environment
    tablassert_biolink_version: str | None = None
    # input_digest: sha256 over the downloaded nodes bytes, edge bytes, and RIG bytes, in that order
    input_digest: str | None = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineMetadata":
        """Construct PipelineMetadata from a dict (ie loaded from json),
         drop unknown keys for forward compatibility, so old metadata files with
         deprecated attributes won't cause errors."""
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    def get_composite_normalization_version(self) -> str:
        """get a string that represents a composite of the relevant versions and settings used in normalization,
        used to name the directories normalization results are stored in, and to determine if a specific normalization
        has already been processed or not"""
        version = f"{self.babel_version}_{self.node_normalizer_version}_{self.normalization_code_version}"
        if self.normalization_conflation:
            version += "_conflated"
        if self.normalization_strict:
            version += "_strict"
        return version

    def generate_build_version(self) -> str:
        versions = [
            self.source,
            self.source_version,
            self.transform_version,
            self.get_composite_normalization_version(),
            self.merging_code_version,
            self.biolink_version,
        ]
        # Tablassert identity and input digests are part of the build identity: without them, a
        # rebuild against changed upstream bytes or a newer latest CLI would hash identically and
        # release/merge would short-circuit on unchanged outputs that are no longer the same.
        # Koza ingests record none of them and keep their exact existing build version.
        versions += [version for version in
                     (self.tablassert_version, self.tablassert_biolink_version, self.input_digest)
                     if version is not None]
        return "_".join(versions)

    def get_release_metadata(self) -> Dict[str, Any]:
        pipeline_metadata_dict = asdict(self)
        del pipeline_metadata_dict["koza_config"]
        return pipeline_metadata_dict

def current_iso_date() -> str:
    # Current UTC timestamp in ISO 8601 format (e.g., "2026-07-09T14:23:51Z"), with seconds precision.
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def next_release_version(previous_release_version: str | None) -> str:
    """Compute the next semantic release version (MAJOR.MINOR.PATCH).

    The patch component is bumped on every release. If there is no previous
    release carrying a valid semantic version (``None``, or a legacy
    date-based version like ``"2026_06_10"``), versioning starts fresh at
    ``"1.0.0"``.

    >>> next_release_version(None)
    '1.0.0'
    >>> next_release_version("2026_06_10")
    '1.0.0'
    >>> next_release_version("1.0.0")
    '1.0.1'
    >>> next_release_version("1.2.9")
    '1.2.10'
    """
    if previous_release_version is None:
        return "1.0.0"
    match = SEMANTIC_VERSION_PATTERN.match(previous_release_version)
    if not match:
        return "1.0.0"
    major, minor, patch = (int(part) for part in match.groups())
    return f"{major}.{minor}.{patch + 1}"


def _to_translator_vocabulary(value: Any) -> Any:
    """Recursively rename every ORION vocabulary term in a graph metadata value to its translator term."""
    if isinstance(value, dict):
        return {_to_translator_term(key): _to_translator_vocabulary(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_to_translator_vocabulary(item) for item in value]
    return value


def _to_translator_term(key: str) -> str:
    """Rename a single ORION vocabulary term, leaving terms of every other vocabulary alone."""
    prefix, separator, term = key.partition(":")
    if separator and prefix == ORION_VOCABULARY:
        return f"{TRANSLATOR_VOCABULARY}:{term}"
    return key


def to_translator_graph_metadata(graph_metadata: KGXGraphMetadata) -> Dict[str, Any]:
    """Serialize graph metadata using the translator-ingests vocabulary in place of ORION's."""
    translator_metadata = _to_translator_vocabulary(graph_metadata.to_dict())
    # The prefix is declared in the context as well as used by the terms, and is not itself a term.
    context = translator_metadata["@context"]
    context.pop(ORION_VOCABULARY, None)
    context[TRANSLATOR_VOCABULARY] = TRANSLATOR_VOCABULARY_URL
    return translator_metadata


def get_kgx_source_from_rig(source: str, rig_path: Path | None = None) -> KGXKnowledgeSource:
    """Read a source's RIG YAML and create a KGXKnowledgeSource instance.

    Args:
        source: id of the source whose RIG describes the upstream knowledge source.
        rig_path: explicit RIG file to read (e.g. a Tablassert build's downloaded snapshot).
            When omitted, the source's committed `<source>_rig.yaml` is used.

    Raises:
        FileNotFoundError: when neither the explicit nor the committed RIG file exists.
    """
    rig_yaml_file = rig_path if rig_path is not None else INGESTS_PARSER_PATH / source / f"{source}_rig.yaml"
    if not rig_yaml_file.exists():
        raise FileNotFoundError(f"Rig YAML file not found for {source}")

    with rig_yaml_file.open("r", encoding="utf-8") as rig_file:
        rig_data = yaml.safe_load(rig_file)
        rig_name = rig_data.get("name", source)
        rig_source_info = rig_data["source_info"]

    return KGXKnowledgeSource(
        identifier=source,
        name=rig_name if rig_name else source,
        description=rig_source_info.get("description", ""),
        license=rig_source_info.get("terms_of_use_info", ""),
        url=rig_source_info.get("data_access_locations", "")
    )

def get_kgx_source_from_release_metadata(source: str, release_metadata: "PipelineMetadata") -> KGXKnowledgeSource:
    """Reconstruct a source's KGXKnowledgeSource from its released graph metadata.

    HuggingFace deletes historical dataset directories, so a released Tablassert source's RIG can
    no longer be re-fetched from upstream when a multisource merged graph is built. Releases
    already carry their provenance: the `isBasedOn` entries of the released
    `releases/<source>/<release_version>/graph-metadata.json` record exactly what the release was
    built from. This reads that released metadata instead of any cache or network source.

    Args:
        source: id of the source whose release provenance to read.
        release_metadata: the source's latest release metadata (provides the release version).

    Returns:
        The KGXKnowledgeSource recorded for the source in its release.

    Raises:
        FileNotFoundError: when the release's graph-metadata.json is missing.
        ValueError: when the release records no entry, several entries, or a wrong version for the
            source; released provenance must be unambiguous to be usable.
    """
    from translator_ingest.util.storage.local import IngestFileName  # local import avoids a storage/metadata cycle

    if not release_metadata.release_version:
        raise ValueError(f"Release metadata for {source} does not record a release version.")
    release_graph_metadata_path = (Path(INGESTS_RELEASES_PATH) / source / release_metadata.release_version /
                                   IngestFileName.GRAPH_METADATA_FILE)
    if not release_graph_metadata_path.exists():
        raise FileNotFoundError(f"Released graph metadata not found for {source}: {release_graph_metadata_path}")
    with release_graph_metadata_path.open("r", encoding="utf-8") as graph_metadata_file:
        graph_metadata = json.load(graph_metadata_file)
    matches = [entry for entry in graph_metadata.get("isBasedOn", [])
               if isinstance(entry, dict) and entry.get("identifier") == source]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one isBasedOn entry for {source} in {release_graph_metadata_path}, "
            f"found {len(matches)}."
        )
    entry = matches[0]
    if entry.get("version") != release_metadata.source_version:
        raise ValueError(
            f"Released provenance for {source} records version {entry.get('version')!r}, but the release "
            f"metadata records source version {release_metadata.source_version!r}."
        )
    return KGXKnowledgeSource.from_dict(entry)
