"""translator_ingest.ingests.hpoa package."""
import importlib_metadata

try:
    __version__ = importlib_metadata.version(__name__)
except importlib_metadata.PackageNotFoundError:
    # package is not installed
    __version__ = "0.0.0"  # pragma: no cover

# TODO: this "shared" versioning of HPOA is probably totally wrong
#       in that this versioning metric is a migrant relic from the
#       monarch-phenotype-profile-ingest Git repository package
def get_latest_version():
    return __version__
