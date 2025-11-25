import logging
import click
import json

from dataclasses import is_dataclass, asdict
from datetime import datetime
from importlib import import_module
from pathlib import Path

from translator_ingest.util.biolink import get_current_biolink_version

from kghub_downloader.main import main as kghub_download

from koza.runner import KozaRunner
from koza.model.formats import OutputFormat as KozaOutputFormat

from orion.meta_kg import MetaKnowledgeGraphBuilder
from orion.kgx_metadata import KGXGraphMetadata, analyze_graph

from translator_ingest import INGESTS_PARSER_PATH, INGESTS_STORAGE_URL
from translator_ingest.normalize import get_current_node_norm_version, normalize_kgx_files
from translator_ingest.util.metadata import PipelineMetadata, get_kgx_source_from_rig
from translator_ingest.util.storage.local import (
    get_output_directory,
    get_source_data_directory,
    get_transform_directory,
    get_normalization_directory,
    get_validation_directory,
    get_versioned_file_paths,
    IngestFileType,
    write_ingest_file,
)
from translator_ingest.util.validate_biolink_kgx import ValidationStatus, get_validation_status, validate_kgx

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Determine the latest available version for the source using the function from the ingest module
def get_latest_source_version(source):
    try:
        # Import the ingest module for this source
        ingest_module = import_module(f"translator_ingest.ingests.{source}.{source}")
    except ModuleNotFoundError:
        error_message = f"Python module for {source} was not found at translator_ingest.ingests.{source}.{source}.py"
        logger.error(error_message)
        raise NotImplementedError(error_message)

    try:
        # Get a reference to the get_latest_source_version function
        latest_version_fn = getattr(ingest_module, "get_latest_version")
        # Call it and return the latest version
        return latest_version_fn()
    except AttributeError:
        error_message = (
            f"Function get_latest_version() was not found for {source}. "
            f"There should be a function declared to retrieve the latest version of the source data in"
            f" translator_ingest.ingests.{source}.{source}.py"
        )
        logger.error(error_message)
        raise NotImplementedError(error_message)


# Download the source data for a source from the original location
def download(pipeline_metadata: PipelineMetadata):
    logger.info(f"Downloading source data for {pipeline_metadata.source}...")
    # Find the path to the source specific download yaml
    download_yaml_file = INGESTS_PARSER_PATH / pipeline_metadata.source / "download.yaml"
    # Get a path for the subdirectory for the source data
    source_data_output_dir = get_source_data_directory(pipeline_metadata)
    Path.mkdir(source_data_output_dir, exist_ok=True)
    # Download the data
    # Don't need to check if file(s) already downloaded, kg downloader handles that
    kghub_download(yaml_file=str(download_yaml_file), output_dir=str(source_data_output_dir))


# Check if the transform stage was already completed
def is_transform_complete(pipeline_metadata: PipelineMetadata):
    nodes_file_path, edges_file_path = get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_KGX_FILES, pipeline_metadata=pipeline_metadata
    )

    if not (nodes_file_path and nodes_file_path.exists() and edges_file_path and edges_file_path.exists()):
        return False
    transform_metadata = get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    if not (transform_metadata.exists()):
        logger.info(f"Transform {pipeline_metadata.source}: KGX files exist but transformation metadata was not found.")
        return False
    return True


# Transform original source data into KGX files using Koza and functions defined in the ingest module
def transform(pipeline_metadata: PipelineMetadata):
    source = pipeline_metadata.source
    logger.info(f"Starting transform for {source}")

    # the path to the source config yaml file for this specific ingest
    source_config_yaml_path = INGESTS_PARSER_PATH / source / f"{source}.yaml"

    # the path for the versioned output subdirectory for this transform
    transform_output_dir = get_transform_directory(pipeline_metadata)
    Path.mkdir(transform_output_dir, parents=True, exist_ok=True)

    # use Koza to load the config and run the transform
    config, runner = KozaRunner.from_config_file(
        str(source_config_yaml_path),
        output_dir=str(transform_output_dir),
        output_format=KozaOutputFormat.jsonl,
        input_files_dir=str(get_source_data_directory(pipeline_metadata)),
    )
    runner.run()
    logger.info(f"Finished transform for {source}")

    # retrieve source level metadata from the koza config
    # (this is currently populated from the metadata field of the source yaml but gets cast to a koza.DatasetDescription
    # object so you can't include arbitrary fields)
    # TODO bring koza.DatasetDescription up to date with the KGX metadata spec or allow passing arbitrary fields
    koza_source_metadata = config.metadata
    source_metadata = (
        asdict(koza_source_metadata)
        if is_dataclass(koza_source_metadata)
        else {"source_metadata": koza_source_metadata}
    )

    # collect and save some metadata about the transform
    transform_metadata = {
        "source": pipeline_metadata.source,
        **{k: v for k, v in source_metadata.items() if v is not None},
        "source_version": pipeline_metadata.source_version,
        "transform_version": "1.0",
        "transform_metadata": runner.transform_metadata,
    }
    # we probably still want to do more here, maybe stuff like:
    # transform_metadata.update(runner.writer.duplicate_node_count)
    write_ingest_file(file_type=IngestFileType.TRANSFORM_METADATA_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data=transform_metadata)


def is_normalization_complete(pipeline_metadata: PipelineMetadata):
    norm_nodes, norm_edges = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    norm_metadata = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZATION_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    norm_map = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZATION_MAP_FILE, pipeline_metadata=pipeline_metadata
    )
    return norm_nodes.exists() and norm_edges.exists() and norm_metadata.exists() and norm_map.exists()


def normalize(pipeline_metadata: PipelineMetadata):
    logger.info(f"Starting normalization for {pipeline_metadata.source}...")
    normalization_output_dir = get_normalization_directory(pipeline_metadata=pipeline_metadata)
    normalization_output_dir.mkdir(exist_ok=True)
    input_nodes_path, input_edges_path = get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    norm_node_path, norm_edge_path = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    norm_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZATION_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    norm_failures_path = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZATION_FAILURES_FILE, pipeline_metadata=pipeline_metadata
    )
    node_norm_map_path = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZATION_MAP_FILE, pipeline_metadata=pipeline_metadata
    )
    predicate_map_path = get_versioned_file_paths(
        file_type=IngestFileType.PREDICATE_NORMALIZATION_MAP_FILE, pipeline_metadata=pipeline_metadata
    )
    normalize_kgx_files(
        input_nodes_file_path=str(input_nodes_path),
        input_edges_file_path=str(input_edges_path),
        nodes_output_file_path=str(norm_node_path),
        node_norm_map_file_path=str(node_norm_map_path),
        node_norm_failures_file_path=str(norm_failures_path),
        edges_output_file_path=str(norm_edge_path),
        predicate_map_file_path=str(predicate_map_path),
        normalization_metadata_file_path=str(norm_metadata_path),
    )
    logger.info(f"Normalization complete for {pipeline_metadata.source}.")


def is_validation_complete(pipeline_metadata: PipelineMetadata):
    validation_report_file_path = get_versioned_file_paths(
        file_type=IngestFileType.VALIDATION_REPORT_FILE, pipeline_metadata=pipeline_metadata
    )
    return validation_report_file_path.exists()


def validate(pipeline_metadata: PipelineMetadata):
    logger.info(f"Starting validation for {pipeline_metadata.source}... biolink: {pipeline_metadata.biolink_version}")
    nodes_file, edges_file = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    validation_output_dir = get_validation_directory(pipeline_metadata=pipeline_metadata)
    validation_output_dir.mkdir(exist_ok=True)
    validate_kgx(nodes_file=nodes_file, edges_file=edges_file, output_dir=validation_output_dir)


def get_validation_result(pipeline_metadata: PipelineMetadata):
    if not is_validation_complete(pipeline_metadata):
        error_message = f"Validation report not found for {pipeline_metadata.source}."
        logger.error(error_message)
        raise FileNotFoundError(error_message)

    validation_file_path = get_versioned_file_paths(
        file_type=IngestFileType.VALIDATION_REPORT_FILE, pipeline_metadata=pipeline_metadata
    )
    validation_status = get_validation_status(validation_file_path)
    logger.info(f"Validation status for {pipeline_metadata.source}: {validation_status}")
    if validation_status == ValidationStatus.PASSED:
        return True
    return False


def test_data(pipeline_metadata: PipelineMetadata):
    # TODO It'd be more efficient to generate the test data and example edges at the same time as the graph summary.
    #  ORION currently generates test data and example edges while building a metakg, so we still use that, even though
    #  we're not saving the metakg anymore.
    logger.info(f"Generating test data and example edges for {pipeline_metadata.source}...")
    graph_nodes_file_path, graph_edges_file_path = get_versioned_file_paths(
        IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    # Generate the test data and example data
    mkgb = MetaKnowledgeGraphBuilder(
        nodes_file_path=graph_nodes_file_path, edges_file_path=graph_edges_file_path, logger=logger
    )
    # write test data to file
    write_ingest_file(file_type=IngestFileType.TEST_DATA_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data=mkgb.testing_data)
    # write example edges to file
    write_ingest_file(file_type=IngestFileType.EXAMPLE_EDGES_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data=mkgb.example_edges)
    logger.info(f"Test data and example edges complete for {pipeline_metadata.source}.")


def is_graph_metadata_complete(pipeline_metadata: PipelineMetadata):
    test_data_file_path = get_versioned_file_paths(
        file_type=IngestFileType.TEST_DATA_FILE, pipeline_metadata=pipeline_metadata
    )
    example_edges_file_path = get_versioned_file_paths(
        file_type=IngestFileType.EXAMPLE_EDGES_FILE, pipeline_metadata=pipeline_metadata
    )
    graph_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.GRAPH_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    return graph_metadata_file_path.exists() and test_data_file_path.exists() and example_edges_file_path.exists()


def generate_graph_metadata(pipeline_metadata: PipelineMetadata):
    logger.info(f"Generating Graph Metadata for {pipeline_metadata.source}...")

    # Generate test data and example edges
    test_data(pipeline_metadata)

    # Get KGXSource metadata from the rig file
    data_source_info = get_kgx_source_from_rig(pipeline_metadata.source)
    data_source_info.version = pipeline_metadata.source_version

    release_url = f"{INGESTS_STORAGE_URL}/{pipeline_metadata.source}/{pipeline_metadata.release_version}/"
    source_metadata = KGXGraphMetadata(
        id=release_url,
        name=pipeline_metadata.source,
        description="A knowledge graph built for the NCATS Biomedical Data Translator project using Translator-Ingests"
                    ", Biolink Model, and Node Normalizer.",
        license="MIT",
        url=release_url,
        version=pipeline_metadata.release_version,
        date_created=datetime.now().strftime("%Y_%m_%d"),
        biolink_version=pipeline_metadata.biolink_version,
        babel_version=pipeline_metadata.node_norm_version,
        kgx_sources=[data_source_info]
    )

    # get paths to the final nodes and edges files
    graph_nodes_file_path, graph_edges_file_path = get_versioned_file_paths(
        IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    # construct the full graph_metadata by combining source_metadata from translator-ingests with an ORION analysis
    graph_metadata = analyze_graph(
        nodes_file_path=graph_nodes_file_path,
        edges_file_path=graph_edges_file_path,
        graph_metadata=source_metadata,
    )
    write_ingest_file(file_type=IngestFileType.GRAPH_METADATA_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data=graph_metadata)
    logger.info(f"Graph metadata complete for {pipeline_metadata.source}. Preparing ingest metadata...")

    transform_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    if transform_metadata_file_path.exists():
        with transform_metadata_file_path.open("r") as transform_metadata_file:
            transform_metadata = json.load(transform_metadata_file)
    else:
        logger.error(f"Transform metadata not found for {pipeline_metadata.source}...")
        transform_metadata = {"Transform metadata not found."}
    normalization_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZATION_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    if normalization_metadata_path.exists():
        with normalization_metadata_path.open("r") as normalization_metadata_file:
            normalization_metadata = json.load(normalization_metadata_file)
    else:
        logger.error(f"Normalization metadata not found for {pipeline_metadata.source}...")
        normalization_metadata = {"Normalization metadata not found."}
    ingest_metadata = {
        "transform": transform_metadata,
        "normalization": normalization_metadata,
    }
    write_ingest_file(file_type=IngestFileType.INGEST_METADATA_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data=ingest_metadata)
    logger.info(f"Ingest metadata complete for {pipeline_metadata.source}.")


# Open the latest release metadata and compare build versions with the current pipeline run to see if a new release needs to
# be generated. build_version is used, intentionally ignoring the release version, because we don't need to make a
# new release if the build hasn't actually changed.
def is_latest_release_current(pipeline_metadata: PipelineMetadata):
    release_metadata_path = get_versioned_file_paths(IngestFileType.LATEST_RELEASE_FILE,
                                                     pipeline_metadata=pipeline_metadata)
    if not release_metadata_path.exists():
        return False
    with release_metadata_path.open("r") as latest_release_file:
        latest_release_metadata = PipelineMetadata(**json.load(latest_release_file))
    return pipeline_metadata.build_version == latest_release_metadata.build_version


def generate_latest_release_metadata(pipeline_metadata: PipelineMetadata):
    logger.info(f"Generating release metadata for {pipeline_metadata.source}... "
                f"release: {pipeline_metadata.release_version}")
    latest_release_metadata = {
        **asdict(pipeline_metadata),
        "data": f"{INGESTS_STORAGE_URL}/{pipeline_metadata.source}/{pipeline_metadata.release_version}/",
    }
    write_ingest_file(file_type=IngestFileType.LATEST_RELEASE_FILE,
                      pipeline_metadata=pipeline_metadata,
                      data=latest_release_metadata)


def run_pipeline(source: str, transform_only: bool = False, overwrite: bool = False):
    source_version = get_latest_source_version(source)
    pipeline_metadata: PipelineMetadata = PipelineMetadata(source, source_version=source_version)
    Path.mkdir(get_output_directory(pipeline_metadata), parents=True, exist_ok=True)

    # Download the source data
    download(pipeline_metadata)

    # Transform the source data into KGX files if needed
    # TODO we need a way to version the transform (see issue #97)
    pipeline_metadata.transform_version = "1.0"
    if is_transform_complete(pipeline_metadata) and not overwrite:
        logger.info(
            f"Transform already done for {pipeline_metadata.source} ({pipeline_metadata.source_version}), "
            f"transform: {pipeline_metadata.transform_version}"
        )
    else:
        transform(pipeline_metadata)
    if transform_only:
        return

    # Normalize the post-transform KGX files
    pipeline_metadata.node_norm_version = get_current_node_norm_version()
    if is_normalization_complete(pipeline_metadata) and not overwrite:
        logger.info(
            f"Normalization already done for {pipeline_metadata.source} ({pipeline_metadata.source_version}), "
            f"normalization: {pipeline_metadata.node_norm_version}"
        )
    else:
        normalize(pipeline_metadata)

    # Validate the post-normalization files
    # First retrieve and set the current biolink version to make sure validation is run using that version
    pipeline_metadata.biolink_version = get_current_biolink_version()
    if is_validation_complete(pipeline_metadata) and not overwrite:
        logger.info(f"Validation already done for {pipeline_metadata.source} ({pipeline_metadata.source_version}), "
                    f"biolink: {pipeline_metadata.biolink_version}")
    else:
        validate(pipeline_metadata)

    passed = get_validation_result(pipeline_metadata)
    if not passed:
        logger.warning(f"Validation did not pass for {pipeline_metadata.source}! Aborting...")
        return

    # The release version needs to be established before the graph metadata phase because it's used in the outputs
    release_version = datetime.now().strftime("%Y_%m_%d")
    pipeline_metadata.release_version = release_version

    pipeline_metadata.build_version = pipeline_metadata.generate_build_version()
    if is_graph_metadata_complete(pipeline_metadata) and not overwrite:
        logger.info(
            f"Graph metadata already completed for {pipeline_metadata.source} ({pipeline_metadata.source_version})."
        )
    else:
        generate_graph_metadata(pipeline_metadata)

    if is_latest_release_current(pipeline_metadata) and not overwrite:
        logger.info(f"Latest release metadata already up to date for {pipeline_metadata.source}, "
                    f"build: {pipeline_metadata.build_version}")
    else:
        generate_latest_release_metadata(pipeline_metadata)


@click.command()
@click.argument("source", type=str)
@click.option("--transform-only", is_flag=True, help="Only perform the transformation.")
@click.option("--overwrite", is_flag=True, help="Start fresh and overwrite previously generated files.")
def main(source, transform_only, overwrite):
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    run_pipeline(source, transform_only=transform_only, overwrite=overwrite)


if __name__ == "__main__":
    main()
