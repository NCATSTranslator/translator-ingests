"""Transform versions: a content hash of each ingest's code, recorded in a file alongside the ingest.

The transform version of an ingest is a short hash of the files that define how it transforms source
data. It is recorded in ``ingests/<source>/transform_version`` and committed with the code, so the version
belonging to any commit can be read from that commit instead of recomputed. The pipeline refuses to run an
ingest whose recorded version does not match its code, and a unit test fails in CI when a recorded version
is missing or out of date.

CLI ::

    recompute and record the transform version of every ingest (or only of the given sources)
    uv run python -m translator_ingest.util.transform_version update [SOURCES...]

    exit non-zero if any recorded transform version is missing or out of date
    uv run python -m translator_ingest.util.transform_version check [SOURCES...]
"""

import hashlib
from pathlib import Path

import click

from translator_ingest import INGESTS_PARSER_PATH


TRANSFORM_VERSION_FILENAME = "transform_version"

UPDATE_COMMAND = "make update-transform-versions"


class TransformVersionError(RuntimeError):
    """Raised when an ingest's recorded transform version is missing or does not match its code."""


def get_ingest_sources() -> list[str]:
    """Return the sorted names of every ingest.

    An ingest is a directory in the ingests package holding a ``<source>.yaml`` koza config. 
    Exclude directories starting with an underscore, such as the ingest template.
    """
    return sorted(
        path.name
        for path in INGESTS_PARSER_PATH.iterdir()
        if path.is_dir() and not path.name.startswith("_") and (path / f"{path.name}.yaml").exists()
    )


def compute_transform_version(source: str) -> str:
    """Compute a content hash of the ingest's source files.

    Hashes all .py files, .json files, and the ingest YAML config in the ingest directory,
    producing a short hash that changes whenever the ingest changes.
    This automatically triggers a new build when the pipeline detects a new version.
    """
    ingest_dir = INGESTS_PARSER_PATH / source
    source_yaml = ingest_dir / f"{source}.yaml"

    files_to_hash: list[Path] = sorted(ingest_dir.glob("*.py")) + sorted(ingest_dir.glob("*.json"))
    if source_yaml.exists():
        files_to_hash.append(source_yaml)

    hasher = hashlib.sha256()
    for file_path in files_to_hash:
        hasher.update(file_path.read_bytes())
    return hasher.hexdigest()[:8]


def get_transform_version_path(source: str) -> Path:
    """Return the path of the file recording the ingest's transform version."""
    return INGESTS_PARSER_PATH / source / TRANSFORM_VERSION_FILENAME


def read_recorded_transform_version(source: str) -> str | None:
    """Return the ingest's recorded transform version, or None if none is recorded."""
    version_path = get_transform_version_path(source)
    if not version_path.exists():
        return None
    return version_path.read_text().strip()


def describe_stale_transform_version(source: str, recorded: str | None, computed: str) -> str:
    """Describe how an ingest's recorded transform version differs from the hash of its code."""
    return f"{source}: recorded transform version is {recorded or 'missing'}, but the ingest code hashes to {computed}"


def get_transform_version(source: str) -> str:
    """Return the ingest's recorded transform version, after verifying it matches the ingest code.

    Raises:
        TransformVersionError: if the recorded version is missing or does not match the ingest code, so
            outputs are never labeled with a version that does not describe the code that produced them.
    """
    recorded = read_recorded_transform_version(source)
    computed = compute_transform_version(source)
    if recorded != computed:
        raise TransformVersionError(
            f"{describe_stale_transform_version(source, recorded, computed)}. Run `{UPDATE_COMMAND}` to update it."
        )
    return computed


def write_transform_version(source: str) -> bool:
    """Record the ingest's current transform version in its transform version file.

    Returns:
        True if the recorded version was missing or changed, False if it was already current.
    """
    computed = compute_transform_version(source)
    if read_recorded_transform_version(source) == computed:
        return False
    get_transform_version_path(source).write_text(f"{computed}\n")
    return True


def resolve_sources(sources: tuple[str, ...]) -> list[str]:
    """Return the requested sources, or every ingest if none were requested.

    Raises:
        click.BadParameter: if a requested source is not an ingest.
    """
    ingest_sources = get_ingest_sources()
    unknown = sorted(set(sources) - set(ingest_sources))
    if unknown:
        raise click.BadParameter(f"Not ingests: {', '.join(unknown)}", param_hint="SOURCES")
    return list(sources) or ingest_sources


@click.group()
def cli():
    """Manage the recorded transform versions of ingests."""


@cli.command()
@click.argument("sources", nargs=-1)
def update(sources: tuple[str, ...]):
    """Recompute and record the transform version of each ingest (default: every ingest)."""
    for source in resolve_sources(sources):
        if write_transform_version(source):
            click.echo(f"{source}: transform version updated to {read_recorded_transform_version(source)}")


@cli.command()
@click.argument("sources", nargs=-1)
def check(sources: tuple[str, ...]):
    """Fail if the recorded transform version of any ingest is missing or out of date."""
    stale = []
    for source in resolve_sources(sources):
        recorded = read_recorded_transform_version(source)
        computed = compute_transform_version(source)
        if recorded != computed:
            stale.append(describe_stale_transform_version(source, recorded, computed))
    if stale:
        raise click.ClickException(
            "\n".join(stale) + f"\nRun `{UPDATE_COMMAND}` and commit the updated transform_version files."
        )


if __name__ == "__main__":
    cli()