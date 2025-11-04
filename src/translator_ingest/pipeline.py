import logging
import click
import json
from dataclasses import is_dataclass, asdict
from importlib import import_module
from pathlib import Path

from kghub_downloader.main import main as kghub_download
from koza.runner import KozaRunner
from koza.model.formats import OutputFormat as KozaOutputFormat
from orion.meta_kg import MetaKnowledgeGraphBuilder
from orion.kgx_validation import validate_graph as generate_graph_summary

from translator_ingest import INGESTS_PARSER_PATH, INGESTS_DATA_PATH
from translator_ingest.normalize import get_current_node_norm_version, normalize_kgx_files
from translator_ingest.util.metadata import PipelineMetadata
from translator_ingest.util.storage.local import (
    get_output_directory,
    get_source_data_directory,
    get_transform_directory,
    get_normalization_directory,
    get_versioned_file_paths,
    IngestFileType,
)
from translator_ingest.util.validate_biolink_kgx import (
    ValidationStatus, get_validation_status, validate_kgx
)
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

    # the path for the versioned subdirectory for this transform
    transform_dir = get_transform_directory(pipeline_metadata)
    Path.mkdir(transform_dir, parents=True, exist_ok=True)

    # use Koza to load the config and run the transform
    config, runner = KozaRunner.from_config_file(
        str(source_config_yaml_path),
        output_dir=str(transform_dir),
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

    transform_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    with transform_metadata_file_path.open("w") as normalization_metadata_file:
        normalization_metadata_file.write(json.dumps(transform_metadata, indent=4))


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
    logger.info(f"Starting validation for {pipeline_metadata.source}...")
    nodes_file, edges_file = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    validation_output_dir = get_normalization_directory(pipeline_metadata=pipeline_metadata)
    validate_kgx(nodes_file=nodes_file, edges_file=edges_file, output_dir=validation_output_dir)


def get_validation_result(pipeline_metadata: PipelineMetadata):
    if not is_validation_complete(pipeline_metadata):
        error_message = f"Validation report not found for {pipeline_metadata.source}."
        logger.error(error_message)
        raise IOError(error_message)

    validation_file_path = get_versioned_file_paths(
        file_type=IngestFileType.VALIDATION_REPORT_FILE, pipeline_metadata=pipeline_metadata
    )
    validation_status = get_validation_status(validation_file_path)
    logger.info(f"Validation status for {pipeline_metadata.source}: {validation_status}")
    if validation_status == ValidationStatus.PASSED:
        return True
    return False


def is_meta_kg_complete(pipeline_metadata: PipelineMetadata):
    meta_kg_file_path = get_versioned_file_paths(
        file_type=IngestFileType.META_KG_FILE, pipeline_metadata=pipeline_metadata
    )
    return meta_kg_file_path.exists()


def meta_kg(pipeline_metadata: PipelineMetadata):
    logger.info(f"Generating Meta KG for {pipeline_metadata.source}...")
    graph_nodes_file_path, graph_edges_file_path = get_versioned_file_paths(
        IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    # Generate the Meta KG, test data and example data (holds them in memory)
    mkgb = MetaKnowledgeGraphBuilder(
        nodes_file_path=graph_nodes_file_path, edges_file_path=graph_edges_file_path, logger=logger
    )
    # Write them all to files
    meta_kg_file_path = get_versioned_file_paths(
        file_type=IngestFileType.META_KG_FILE, pipeline_metadata=pipeline_metadata
    )
    mkgb.write_meta_kg_to_file(str(meta_kg_file_path))
    test_data_file_path = get_versioned_file_paths(
        file_type=IngestFileType.TEST_DATA_FILE, pipeline_metadata=pipeline_metadata
    )
    mkgb.write_test_data_to_file(str(test_data_file_path))
    example_data_file_path = get_versioned_file_paths(
        file_type=IngestFileType.EXAMPLE_DATA_FILE, pipeline_metadata=pipeline_metadata
    )
    mkgb.write_example_data_to_file(str(example_data_file_path))
    logger.info(f"Meta KG complete for {pipeline_metadata.source}.")


def is_summary_complete(pipeline_metadata: PipelineMetadata):
    final_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.FINAL_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    return final_metadata_file_path.exists()


def summary(pipeline_metadata: PipelineMetadata):
    logger.info(f"Generating Graph Summary for {pipeline_metadata.source}...")
    graph_nodes_file_path, graph_edges_file_path = get_versioned_file_paths(
        IngestFileType.NORMALIZED_KGX_FILES, pipeline_metadata=pipeline_metadata
    )
    summary_results = generate_graph_summary(
        nodes_file_path=graph_nodes_file_path,
        edges_file_path=graph_edges_file_path,
        graph_id=pipeline_metadata.source,
        graph_version=pipeline_metadata.source_version,
        logger=logger,
    )

    logger.info(f"Graph Summary complete. Accumulating previous metadata for {pipeline_metadata.source}...")
    transform_metadata_file_path = get_versioned_file_paths(
        file_type=IngestFileType.TRANSFORM_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    if transform_metadata_file_path.exists():
        with transform_metadata_file_path.open() as transform_metadata_file:
            transform_metadata = json.load(transform_metadata_file)
    else:
        logger.error(f"Transform metadata not found for {pipeline_metadata.source}...")
        transform_metadata = {"Transform metadata not found."}
    normalization_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.NORMALIZATION_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    if normalization_metadata_path.exists():
        with normalization_metadata_path.open() as normalization_metadata_file:
            normalization_metadata = json.load(normalization_metadata_file)
    else:
        logger.error(f"Normalization metadata not found for {pipeline_metadata.source}...")
        normalization_metadata = {"Normalization metadata not found."}

    # get source_metadata here too when it's implemented
    all_metadata = {
        "transform": transform_metadata,
        "normalization": normalization_metadata,
        "summary": summary_results,
    }
    all_metadata_path = get_versioned_file_paths(
        file_type=IngestFileType.FINAL_METADATA_FILE, pipeline_metadata=pipeline_metadata
    )
    with all_metadata_path.open("w") as all_metadata_file:
        all_metadata_file.write(json.dumps(all_metadata, indent=4))
    logger.info(f"Final metadata complete for {pipeline_metadata.source}.")


def generate_latest_build_report(pipeline_metadata: PipelineMetadata):
    latest_build_report_path = Path(INGESTS_DATA_PATH) / pipeline_metadata.source / "latest_build.json"
    with latest_build_report_path.open("w") as latest_build_file:
        latest_build_file.write(json.dumps(asdict(pipeline_metadata), indent=4))


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
    pipeline_metadata.normalization_version = get_current_node_norm_version()
    if is_normalization_complete(pipeline_metadata) and not overwrite:
        logger.info(
            f"Normalization already done for {pipeline_metadata.source} ({pipeline_metadata.source_version}), "
            f"transform: {pipeline_metadata.transform_version}, "
            f"normalization: {pipeline_metadata.normalization_version}"
        )
    else:
        normalize(pipeline_metadata)

    # Validate the normalized files
    if is_validation_complete(pipeline_metadata) and not overwrite:
        logger.info(f"Validation already done for {pipeline_metadata.source}")
    else:
        validate(pipeline_metadata)

    passed = get_validation_result(pipeline_metadata)
    if not passed:
        logger.warning(f"Validation did not pass for {pipeline_metadata.source}! Aborting...")
        return

    # Generate a Meta KG, test data, example edges
    if is_meta_kg_complete(pipeline_metadata) and not overwrite:
        logger.info(
            f"Meta KG already done for {pipeline_metadata.source} ({pipeline_metadata.source_version}), "
            f"transform: {pipeline_metadata.transform_version}, "
            f"normalization: {pipeline_metadata.normalization_version}"
        )
    else:
        meta_kg(pipeline_metadata)

    if is_summary_complete(pipeline_metadata) and not overwrite:
        logger.info(
            f"Graph summary already done for {pipeline_metadata.source} ({pipeline_metadata.source_version}), "
            f"transform: {pipeline_metadata.transform_version}, "
            f"normalization: {pipeline_metadata.normalization_version}"
        )
    else:
        summary(pipeline_metadata)

    logger.info(f"Generating latest build file {pipeline_metadata.source}.")
    generate_latest_build_report(pipeline_metadata)
    logger.info(f"Pipeline finished for {pipeline_metadata.source}.")


@click.command()
@click.argument("source", type=str)
@click.option("--transform-only", is_flag=True, help="Only perform the transformation.")
@click.option("--overwrite", is_flag=True, help="Start fresh and overwrite previously generated files.")
def main(source, transform_only, overwrite):
    run_pipeline(source, transform_only=transform_only, overwrite=overwrite)


if __name__ == "__main__":
    main()
