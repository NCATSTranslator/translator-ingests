from dataclasses import dataclass

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

    def generate_build_version(self):
        versions = [
            self.source,
            self.source_version,
            self.transform_version,
            self.node_norm_version,
            self.biolink_version,
        ]
        return "_".join(versions)
