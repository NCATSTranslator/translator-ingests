from dataclasses import dataclass


@dataclass
class PipelineMetadata:
    source: str
    source_version: str | None = None
    transform_version: str | None = None
    normalization_version: str | None = None
    biolink_version: str | None = None

    def get_composite_version(self):
        versions = [
            self.source,
            self.source_version,
            self.transform_version,
            self.normalization_version,
            self.biolink_version,
        ]
        return "".join(str(versions) for v in versions if v)
