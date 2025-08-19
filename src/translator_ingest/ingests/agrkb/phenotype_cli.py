"""Command line interface for alliance-phenotype-association-ingest."""

import logging
from pathlib import Path

import typer
from kghub_downloader.download_utils import download_from_yaml
from koza.cli_utils import transform_source

app = typer.Typer()
logger = logging.getLogger(__name__)


@app.callback()
def callback(
    version: bool = typer.Option(False, "--version", is_eager=True),
):
    """alliance-phenotype-association-ingest CLI."""
    if version:
        from alliance_phenotype_association_ingest import __version__

        typer.echo(f"alliance-phenotype-association-ingest version: {__version__}")
        raise typer.Exit()


@app.command()
def download(force: bool = typer.Option(False, help="Force download of data, even if it exists")):
    """Download data for alliance-phenotype-association-ingest."""
    typer.echo("Downloading data for alliance-phenotype-association-ingest...")
    download_config = Path(__file__).parent / "download.yaml"
    download_from_yaml(yaml_file=download_config, output_dir=".", ignore_cache=force)


@app.command()
def transform(
    output_dir: str = typer.Option("output", help="Output directory for transformed data"),
    row_limit: int = typer.Option(None, help="Number of rows to process"),
    verbose: int = typer.Option(False, help="Whether to be verbose"),
):
    """Run the Koza transform for alliance-phenotype-association-ingest."""
    typer.echo("Transforming data for alliance-phenotype-association-ingest...")
    transform_code = Path(__file__).parent / "transform.yaml"
    transform_source(
        source=transform_code,
        output_dir=output_dir,
        output_format="tsv",
        row_limit=row_limit,
        verbose=verbose,
    )


if __name__ == "__main__":
    app()
