from dataclasses import dataclass


@dataclass
class PipelineMetadata:
    source: str
    source_version: str | None = None
    transform_version: str | None = None
    normalization_version: str | None = None
    biolink_version: str | None = None
