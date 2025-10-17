#!/usr/bin/env python3
"""
Biolink KGX Validator using LinkML validation framework

Validates KGX files against Biolink Model requirements using LinkML validation plugins.
Provides the same CLI interface as the original validate_kgx.py but with enhanced
Biolink Model compliance checking.
"""

import json
import logging
import sys
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Optional, Dict, Any, List

import click
from translator_ingest.util.storage.local import IngestFileName

from linkml_runtime.utils.schemaview import SchemaView

try:
    from .biolink_validation_plugin import BiolinkValidationPlugin
except ImportError:
    # Handle direct script execution
    sys.path.append(str(Path(__file__).parent))
    from biolink_validation_plugin import BiolinkValidationPlugin
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Cache for Biolink schema to avoid repeated network calls
_BIOLINK_SCHEMA_CACHE = None


def get_biolink_schema() -> SchemaView:
    """Get cached Biolink schema, loading it if not already cached."""
    global _BIOLINK_SCHEMA_CACHE
    
    if _BIOLINK_SCHEMA_CACHE is not None:
        return _BIOLINK_SCHEMA_CACHE
    
    # Try to load from local biolink model first (same version as ingests)
    try:
        from biolink_model import BIOLINK_MODEL_YAML_PATH
        _BIOLINK_SCHEMA_CACHE = SchemaView(BIOLINK_MODEL_YAML_PATH)
        logger.debug("Successfully loaded Biolink schema from local file")
        return _BIOLINK_SCHEMA_CACHE
    except Exception as e:
        logger.warning(f"Failed to load local Biolink schema: {e}")
    
    # Fallback to loading from official URL
    try:
        _BIOLINK_SCHEMA_CACHE = SchemaView("https://w3id.org/biolink/biolink-model.yaml")
        logger.debug("Successfully loaded Biolink schema from URL")
        return _BIOLINK_SCHEMA_CACHE
    except Exception as e:
        logger.error(f"Failed to load Biolink schema from URL: {e}")
        raise RuntimeError(
            "Cannot proceed without Biolink schema. Please ensure biolink-model is properly installed."
        )


class ValidationStatus(StrEnum):
    PASSED = "PASSED"
    # maybe we want something like this
    # PASSED_WITH_WARNINGS = "PASSED_WITH_WARNINGS"
    FAILED = "FAILED"
    PENDING = "PENDING"


def load_jsonl(file_path: Path) -> List[Dict[str, Any]]:
    """Load JSONL file and return list of records."""
    records = []
    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def extract_node_ids(nodes: list[dict]) -> set[str]:
    """Extract all node IDs from nodes file."""
    return {node["id"] for node in nodes if "id" in node}


def extract_edge_node_refs(edges: list[dict]) -> set[str]:
    """Extract all node IDs referenced in edges (subject + object)."""
    node_refs = set()
    for edge in edges:
        if "subject" in edge:
            node_refs.add(edge["subject"])
        if "object" in edge:
            node_refs.add(edge["object"])
    return node_refs


def save_validation_report(report: Dict[str, Any], output_dir: Path) -> Path:
    """Save validation report to JSON file"""

    # Generate report filepath
    report_path = output_dir / IngestFileName.VALIDATION_REPORT_FILE

    # Save report
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2)

    logger.info(f"Validation report saved to: {report_path}")
    return report_path


def validate_kgx_consistency(nodes_file: Path, edges_file: Path) -> Dict[str, Any]:
    """
    Validate KGX files using LinkML Biolink validation.

    Returns comprehensive validation report.
    """
    logger.info(f"Loading nodes from: {nodes_file}")
    nodes = load_jsonl(nodes_file)
    logger.info(f"Found {len(nodes)} nodes")

    logger.info(f"Loading edges from: {edges_file}")
    edges = load_jsonl(edges_file)
    logger.info(f"Found {len(edges)} edges")

    # Create combined KGX structure for validation
    kgx_data = {"nodes": nodes, "edges": edges}

    # Get cached Biolink schema view - always required for proper validation
    biolink_schema = get_biolink_schema()

    # Perform validation using plugin directly
    validation_results = []
    try:
        # Use plugin directly for validation, passing schema_view during initialization
        plugin = BiolinkValidationPlugin(schema_view=biolink_schema)
        from linkml.validator.validation_context import ValidationContext

        # Create validation context with schema
        context = ValidationContext(target_class="KnowledgeGraph", schema=biolink_schema.schema)

        validation_results = list(plugin.process(kgx_data, context))
    except Exception as e:
        logger.error(f"Validation failed: {e}")
        validation_results = []

    # Process validation results
    errors = []
    warnings = []
    for result in validation_results:
        result_dict = {
            "type": result.type,
            "severity": result.severity.name,
            "message": result.message,
            "instance_path": getattr(result, "instance_path", "unknown"),
        }
        if result.severity.name == "ERROR":
            errors.append(result_dict)
        else:
            warnings.append(result_dict)

    # Check reference integrity (nodes referenced in edges)
    node_ids = {node["id"] for node in nodes if "id" in node}
    edge_node_refs = set()
    for edge in edges:
        if "subject" in edge:
            edge_node_refs.add(edge["subject"])
        if "object" in edge:
            edge_node_refs.add(edge["object"])

    missing_nodes = edge_node_refs - node_ids
    orphaned_nodes = node_ids - edge_node_refs

    # Add reference integrity errors
    for missing_node in missing_nodes:
        errors.append(
            {
                "type": "reference-integrity",
                "severity": "ERROR",
                "message": f"Edge references non-existent node: {missing_node}",
                "instance_path": "edges",
            }
        )

    validation_passed = len(errors) == 0 and len(missing_nodes) == 0

    # Create structured validation report
    report = {
        "timestamp": datetime.now().isoformat(),
        "files": {"nodes_file": str(nodes_file), "edges_file": str(edges_file)},
        "statistics": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "unique_nodes_in_edges": len(edge_node_refs),
            "missing_nodes_count": len(missing_nodes),
            "orphaned_nodes_count": len(orphaned_nodes),
            "validation_errors": len(errors),
            "validation_warnings": len(warnings),
        },
        "validation_status": ValidationStatus.PASSED if validation_passed else ValidationStatus.FAILED,
        "issues": {
            "errors": errors,
            "warnings": warnings,
            "missing_nodes": sorted(list(missing_nodes)),
            "orphaned_nodes": sorted(list(orphaned_nodes)),
        },
    }

    # Log summary
    if errors:
        logger.error(f"Found {len(errors)} validation errors")
    if warnings:
        logger.warning(f"Found {len(warnings)} validation warnings")
    if missing_nodes:
        logger.error(f"Found {len(missing_nodes)} missing node references")
    if orphaned_nodes:
        logger.info(f"Found {len(orphaned_nodes)} orphaned nodes")

    logger.info(f"Validation {ValidationStatus.PASSED if validation_passed else ValidationStatus.FAILED}")

    return report


def find_kgx_files(data_dir: Path) -> List[tuple]:
    """
    Find all KGX node/edge file pairs in data directory.

    Returns list of (source_name, nodes_file, edges_file) tuples.
    """
    kgx_pairs = []

    for subdir in data_dir.iterdir():
        if not subdir.is_dir():
            continue

        # Look for *nodes.jsonl and *edges.jsonl files
        nodes_files = list(subdir.glob("*nodes.jsonl"))
        edges_files = list(subdir.glob("*edges.jsonl"))

        if not nodes_files or not edges_files:
            continue

        if len(nodes_files) > 1:
            logger.warning(f"Multiple nodes files found in {subdir}: {[f.name for f in nodes_files]}")
        if len(edges_files) > 1:
            logger.warning(f"Multiple edges files found in {subdir}: {[f.name for f in edges_files]}")

        kgx_pairs.append((subdir.name, nodes_files[0], edges_files[0]))

    return kgx_pairs


def validate_kgx(nodes_file: Path, edges_file: Path, output_dir: Path, no_save: bool = False) -> bool:
    if not nodes_file.exists():
        error_message = f"Nodes file not found: {nodes_file}"
        logger.error(error_message)
        raise IOError(error_message)
    if not edges_file.exists():
        error_message = f"Edges file not found: {edges_file}"
        logger.error(error_message)
        raise IOError(error_message)

    single_report = validate_kgx_consistency(nodes_file, edges_file)
    validation_passed = single_report.get("validation_status") == ValidationStatus.PASSED

    # Save single file report if requested
    if not no_save:
        # Create a minimal report structure for single file validation
        validation_report = {
            "timestamp": datetime.now().isoformat(),
            "data_directory": "single_file_validation",
            "sources": {"single_validation": single_report},
            "summary": {
                "total_sources": 1,
                "passed": 1 if validation_passed else 0,
                "failed": 0 if validation_passed else 1,
                "overall_status": ValidationStatus.PASSED if validation_passed else ValidationStatus.FAILED,
            },
        }
        save_validation_report(validation_report, output_dir)
    return validation_passed


def validate_data_directory(data_dir: Path, output_dir: Optional[Path] = None) -> Dict[str, Any]:
    """
    Validate all KGX files in data directory using Biolink validation.

    Returns combined validation report for all sources.
    """
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        return {"error": f"Data directory not found: {data_dir}"}

    kgx_pairs = find_kgx_files(data_dir)

    if not kgx_pairs:
        logger.info(f"No KGX file pairs found in {data_dir}")
        return {
            "timestamp": datetime.now().isoformat(),
            "data_directory": str(data_dir),
            "sources": {},
            "summary": {"total_sources": 0, "passed": 0, "failed": 0, "overall_status": "NO_DATA"},
        }

    logger.info(f"Found {len(kgx_pairs)} KGX file pairs to validate")

    # Create validation report
    validation_report = {
        "timestamp": datetime.now().isoformat(),
        "data_directory": str(data_dir),
        "sources": {},
        "summary": {"total_sources": len(kgx_pairs), "passed": 0, "failed": 0, "overall_status": "PENDING"},
    }

    # Validate each source
    for source_name, nodes_file, edges_file in kgx_pairs:
        logger.info(f"Validating source: {source_name}")

        source_report = validate_kgx_consistency(nodes_file, edges_file)
        validation_report["sources"][source_name] = source_report

        if source_report.get("validation_status") == ValidationStatus.PASSED:
            validation_report["summary"]["passed"] += 1
        else:
            validation_report["summary"]["failed"] += 1

    # Set overall status
    if validation_report["summary"]["failed"] == 0:
        validation_report["summary"]["overall_status"] = ValidationStatus.PASSED
    else:
        validation_report["summary"]["overall_status"] = ValidationStatus.FAILED

    # Save report if output directory specified
    if output_dir:
        save_validation_report(validation_report, output_dir)

    logger.info(f"Overall validation: {validation_report['summary']['overall_status']}")
    logger.info(f"Passed: {validation_report['summary']['passed']}, Failed: {validation_report['summary']['failed']}")

    return validation_report


def get_validation_status(report_file_path: Path) -> Optional[str]:
    with report_file_path.open("r") as validation_report_file:
        validation_report = json.load(validation_report_file)
        try:
            return validation_report["summary"]["overall_status"]
        except KeyError:
            error_message = "Validation report file found but format was unexpected, validation status not found."
            logger.error(error_message)
            raise KeyError(error_message)


@click.command()
@click.option(
    "--data-dir",
    type=click.Path(exists=True, file_okay=False, dir_okay=True, path_type=Path),
    help="Path to data directory containing subdirectories with KGX files",
)
@click.option("--files", nargs=2, metavar="NODES_FILE EDGES_FILE", help="Specific nodes and edges files to validate")
@click.option(
    "--output-dir",
    type=click.Path(path_type=Path),
    default=Path("data"),
    help="Output directory for validation reports (default: data)",
)
@click.option("--no-save", is_flag=True, help="Don't save validation report to file")
def main(data_dir, files, output_dir, no_save):
    """Validate KGX files using Biolink Model compliance checks."""

    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # Validate that exactly one of data_dir or files is provided
    if not data_dir and not files:
        raise click.UsageError("Must specify either --data-dir or --files")
    if data_dir and files:
        raise click.UsageError("Cannot specify both --data-dir and --files")

    if data_dir:
        output_dir_to_use = None if no_save else output_dir
        validation_report = validate_data_directory(data_dir, output_dir_to_use)
        validation_passed = validation_report.get("summary", {}).get("overall_status") == ValidationStatus.PASSED
    else:
        nodes_file, edges_file = Path(files[0]), Path(files[1])
        validation_passed = validate_kgx(
            nodes_file=nodes_file, edges_file=edges_file, output_dir=output_dir, no_save=no_save
        )

    sys.exit(0 if validation_passed else 1)


if __name__ == "__main__":
    main()
