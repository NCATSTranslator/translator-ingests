import yaml
from dataclasses import dataclass, field
from typing import Any, Dict

from orion.kgx_metadata import KGXSource

from translator_ingest import INGESTS_PARSER_PATH


@dataclass
class PipelineMetadata:
    source: str
    source_version: str | None = None
    transform_version: str | None = None
    node_norm_version: str | None = None
    biolink_version: str | None = None
    build_version: str | None = None
    release_version: str | None = None
    data: str | None = None
    koza_config: Dict[str, Any] = field(default_factory=dict)

    def generate_build_version(self):
        versions = [
            self.source,
            self.source_version,
            self.transform_version,
            self.node_norm_version,
            self.biolink_version,
        ]
        return "_".join(versions)

def get_kgx_source_from_rig(source: str) -> KGXSource:
    """Read a source's rig YAML file and create a KGXSource instance."""
    rig_yaml_file = INGESTS_PARSER_PATH / source / f"{source}_rig.yaml"
    if not rig_yaml_file.exists():
        raise FileNotFoundError(f"Rig YAML file not found for {source}")

    with rig_yaml_file.open("r") as rig_file:
        rig_data = yaml.safe_load(rig_file)
        rig_name = rig_data.get("name")
        rig_source_info = rig_data["source_info"]

    return KGXSource(
        id=source,
        name=rig_name if rig_name else source,
        description=rig_source_info["description"],
        license=rig_source_info.get("terms_of_use_info", ""),
        url=rig_source_info.get("data_access_locations", "")
    )
