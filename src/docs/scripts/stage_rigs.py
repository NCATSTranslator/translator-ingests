"""Stage every source's RIG into one directory for validation and documentation.

Committed RIGs (`src/translator_ingest/ingests/*/*_rig.yaml`) are copied byte-for-byte. Tablassert
sources have no committed RIG: their upstream RIG is fetched for the source's current latest dataset
release (the same `get_latest_version()` the pipeline uses), preserving the remote bytes exactly.
Fetch or declaration failures fail the whole staging run instead of silently omitting a source, so
`make validate-rigs` and the docs can never quietly lose DAKP.

The target directory must be empty or contain only files this script manages (the committed and
fetched `<source>_rig.yaml` set). Foreign files are refused so a caller's directory is never wiped
or clobbered by surprise.
"""

from importlib import import_module
from pathlib import Path

import click
import requests

from translator_ingest import INGESTS_PARSER_PATH
from translator_ingest.util.logging_utils import get_logger
from translator_ingest.util.tablassert import declares, load, resolve_dataset_version_placeholder

logger = get_logger(__name__)


def committed_rig_paths() -> list[Path]:
    """Return every committed RIG file, sorted by path."""
    return sorted(INGESTS_PARSER_PATH.glob("*/*_rig.yaml"))


def remote_rig_sources() -> list[str]:
    """Return the sorted names of Tablassert sources that have no committed RIG to copy."""
    sources = []
    for source_dir in sorted(path for path in INGESTS_PARSER_PATH.iterdir() if path.is_dir()):
        source = source_dir.name
        if declares(source) and not (source_dir / f"{source}_rig.yaml").exists():
            sources.append(source)
    return sources


def expected_staged_names() -> set[str]:
    """The set of filenames a complete staging run produces."""
    names = {path.name for path in committed_rig_paths()}
    names.update(f"{source}_rig.yaml" for source in remote_rig_sources())
    return names


def fetch_remote_rig(source: str) -> bytes:
    """Fetch a Tablassert source's upstream RIG for its current latest dataset release."""
    config = load(source)
    ingest_module = import_module(f"translator_ingest.ingests.{source}.{source}")
    dataset_version = ingest_module.get_latest_version()
    url = resolve_dataset_version_placeholder(config.rig.url, dataset_version)
    logger.info(f"Fetching the {source} RIG ({dataset_version}) from {url}")
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response.content


def stage_rigs(target_dir: Path) -> list[Path]:
    """Copy committed RIGs and fetch remote (Tablassert) RIGs into target_dir.

    Args:
        target_dir: the directory to stage into; must exist, and must contain no files outside the
            expected staged set.

    Returns:
        The sorted list of staged file paths.

    Raises:
        click.ClickException: when the target is unusable or contains foreign files.
        requests.HTTPError: when a remote RIG cannot be fetched; staging fails loudly rather than
            dropping the source from validation or docs.
    """
    target = Path(target_dir)
    if not target.is_dir():
        raise click.ClickException(f"Staging target {target} is not a directory.")
    expected = expected_staged_names()
    # Refuse anything that is not a plain regular file, symlinks included: write_bytes() follows
    # symlinks, so a managed name that links outside the target must never be followed.
    foreign = [path.name for path in sorted(target.iterdir())
               if path.is_symlink() or not path.is_file() or path.name not in expected]
    if foreign:
        raise click.ClickException(
            f"Staging target {target} contains files this script does not manage: {foreign}. "
            "Remove them or stage into a fresh directory."
        )

    staged = []
    for rig_path in committed_rig_paths():
        destination = target / rig_path.name
        destination.write_bytes(rig_path.read_bytes())
        staged.append(destination)

    for source in remote_rig_sources():
        destination = target / f"{source}_rig.yaml"
        destination.write_bytes(fetch_remote_rig(source))
        staged.append(destination)

    if not staged:
        raise click.ClickException("No RIG files were found to stage.")
    return sorted(staged)


@click.command()
@click.option("--target-dir", type=click.Path(file_okay=False, path_type=Path), required=True,
              help="Directory to stage RIG files into.")
def cli(target_dir: Path):
    """Stage committed and remote RIG files into one directory."""
    staged = stage_rigs(target_dir)
    click.echo(f"Staged {len(staged)} RIG files into {target_dir}")


if __name__ == "__main__":
    cli()
