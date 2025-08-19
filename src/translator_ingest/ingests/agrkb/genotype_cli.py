"""
Command line interface for alliance-genotype.
TODO: adapt this code for Translator Ingest pipeline
      management of data downloading from the AGRKB
"""
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
    """alliance-genotype CLI."""
    if version:
        from alliance_genotype import __version__

        typer.echo(f"alliance-genotype version: {__version__}")
        raise typer.Exit()


@app.command()
def download(force: bool = typer.Option(False, help="Force download of data, even if it exists")):
    """Download data for alliance-genotype."""
    typer.echo("Downloading data for alliance-genotype...")
    download_config = Path(__file__).parent / "download.yaml"
    download_from_yaml(yaml_file=download_config, output_dir=".", ignore_cache=force)


@app.command()
def transform(
    output_dir: str = typer.Option("output", help="Output directory for transformed data"),
    row_limit: int = typer.Option(None, help="Number of rows to process"),
    verbose: int = typer.Option(False, help="Whether to be verbose"),
    sources: list[str] = typer.Option(["genotype", "allele"], help="Sources to transform"),
):
    """Run the Koza transform for alliance-genotype."""
    for source in sources:
        typer.echo(f"Transforming data for alliance-{source}...")
        transform_code = Path(__file__).parent / f"{source}.yaml"
        if not transform_code.exists():
            typer.echo(f"Error: Transform configuration not found for {source} at {transform_code}")
            continue

        transform_source(
            source=transform_code,
            output_dir=output_dir,
            output_format="tsv",
            row_limit=row_limit,
            verbose=verbose,
        )


if __name__ == "__main__":
    app()
