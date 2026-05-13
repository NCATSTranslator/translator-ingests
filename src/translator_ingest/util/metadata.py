import yaml
from dataclasses import dataclass, field, fields, asdict
from typing import Any, Dict

from orion import KGXKnowledgeSource

from translator_ingest import INGESTS_PARSER_PATH


@dataclass
class PipelineMetadata:
    # source: id of the source, corresponding to the ingest directory and file names
    source: str
    # source_version: version of the source data itself, as determined by a get_latest_version() function
    source_version: str | None = None
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
    # release_version: a version assigned to a completed ingest when it is released
    # (moved to the releases directory, compressed, and distribution metadata generated)
    release_version: str | None = None
    # data: a URL pointing to the pipeline stage artifacts used to process an entire ingest
    data: str | None = None
    # koza_config: the koza config yaml file associated with an ingest
    koza_config: Dict[str, Any] = field(default_factory=dict)

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
        return "_".join(versions)

    def get_release_metadata(self) -> Dict[str, Any]:
        pipeline_metadata_dict = asdict(self)
        del pipeline_metadata_dict["koza_config"]
        return pipeline_metadata_dict

def get_kgx_source_from_rig(source: str) -> KGXKnowledgeSource:
    """Read a source's rig YAML file and create a KGXSource instance."""
    rig_yaml_file = INGESTS_PARSER_PATH / source / f"{source}_rig.yaml"
    if not rig_yaml_file.exists():
        raise FileNotFoundError(f"Rig YAML file not found for {source}")

    with rig_yaml_file.open("r") as rig_file:
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
