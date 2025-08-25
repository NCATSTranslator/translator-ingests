#!/usr/bin/env python3
"""
KGX Node/Edge Consistency Validator

Validates that:
1. All nodes referenced in edges exist in the nodes file at least one edge
2. Reports orphaned nodes and missing node references
"""

import json
import logging
import sys
from pathlib import Path
from typing import Set, Dict, List
import argparse

logger = logging.getLogger(__name__)


def load_jsonl(file_path: Path) -> List[Dict]:
    """Load JSONL file and return list of records."""
    records = []
    with open(file_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line:
                records.append(json.loads(line))
    return records


def extract_node_ids(nodes: List[Dict]) -> Set[str]:
    """Extract all node IDs from nodes file."""
    return {node['id'] for node in nodes if 'id' in node}


def extract_edge_node_refs(edges: List[Dict]) -> Set[str]:
    """Extract all node IDs referenced in edges (subject + object)."""
    node_refs = set()
    for edge in edges:
        if 'subject' in edge:
            node_refs.add(edge['subject'])
        if 'object' in edge:
            node_refs.add(edge['object'])
    return node_refs


def validate_kgx_consistency(nodes_file: Path, edges_file: Path) -> bool:
    """
    Validate KGX node/edge consistency.

    Returns True if validation passes, False otherwise.
    """
    logger.info(f"Loading nodes from: {nodes_file}")
    nodes = load_jsonl(nodes_file)
    node_ids = extract_node_ids(nodes)
    logger.info(f"Found {len(node_ids)} nodes")

    logger.info(f"Loading edges from: {edges_file}")
    edges = load_jsonl(edges_file)
    edge_node_refs = extract_edge_node_refs(edges)
    logger.info(f"Found {len(edges)} edges referencing {len(edge_node_refs)} unique nodes")

    # Check for missing nodes (referenced in edges but not in nodes file)
    missing_nodes = edge_node_refs - node_ids
    if missing_nodes:
        logger.info(f"\nERROR: {len(missing_nodes)} nodes referenced in edges but missing from nodes file:")
        for node_id in sorted(missing_nodes):
            logger.info(f"  - {node_id}")

    # Check for orphaned nodes (in nodes file but not referenced by any edge)
    orphaned_nodes = node_ids - edge_node_refs
    if orphaned_nodes:
        logger.info(f"\nWARNING: {len(orphaned_nodes)} nodes in nodes file but not referenced by any edge:")
        for node_id in sorted(orphaned_nodes):
            logger.info(f"  - {node_id}")

    # Summary
    logger.info(f"\n=== VALIDATION SUMMARY ===")
    logger.info(f"Nodes in file: {len(node_ids)}")
    logger.info(f"Nodes referenced by edges: {len(edge_node_refs)}")
    logger.info(f"Missing nodes: {len(missing_nodes)}")
    logger.info(f"Orphaned nodes: {len(orphaned_nodes)}")

    validation_passed = len(missing_nodes) == 0
    if validation_passed:
        logger.info("VALIDATION PASSED: No missing node references")
    else:
        logger.info("VALIDATION FAILED: Missing node references found")

    return validation_passed


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
            # Skip subdirectories without KGX files (not a warning since this is expected)
            continue

        if len(nodes_files) > 1:
            logger.info(f"Warning: Multiple nodes files found in {subdir}: {[f.name for f in nodes_files]}")
        if len(edges_files) > 1:
            logger.info(f"Warning: Multiple edges files found in {subdir}: {[f.name for f in edges_files]}")

        # Use the first found files
        kgx_pairs.append((subdir.name, nodes_files[0], edges_files[0]))

    return kgx_pairs


def validate_data_directory(data_dir: Path) -> bool:
    """
    Validate all KGX files in data directory.

    Returns True if all validations pass, False otherwise.
    """
    if not data_dir.exists():
        logger.info(f"Error: Data directory not found: {data_dir}")
        return False

    kgx_pairs = find_kgx_files(data_dir)

    if not kgx_pairs:
        logger.info(f"No KGX file pairs found in {data_dir}")
        return True

    logger.info(f"Found {len(kgx_pairs)} KGX file pairs to validate")

    all_passed = True
    for source_name, nodes_file, edges_file in kgx_pairs:
        logger.info(f"\n{'='*60}")
        logger.info(f"Validating source: {source_name}")
        logger.info(f"{'='*60}")

        passed = validate_kgx_consistency(nodes_file, edges_file)
        if not passed:
            all_passed = False

    logger.info(f"\n{'='*60}")
    logger.info("OVERALL VALIDATION SUMMARY")
    logger.info(f"{'='*60}")
    if all_passed:
        logger.info("ALL VALIDATIONS PASSED")
    else:
        logger.info("SOME VALIDATIONS FAILED")

    return all_passed


def main():
    parser = argparse.ArgumentParser(description="Validate KGX node/edge consistency")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--data-dir", type=Path, help="Path to data directory containing subdirectories with KGX files")
    group.add_argument("--files", nargs=2, metavar=("NODES_FILE", "EDGES_FILE"),
                      help="Specific nodes and edges files to validate")

    args = parser.parse_args()

    if args.data_dir:
        validation_passed = validate_data_directory(args.data_dir)
    else:
        nodes_file, edges_file = Path(args.files[0]), Path(args.files[1])
        if not nodes_file.exists():
            logger.info(f"Error: Nodes file not found: {nodes_file}")
            sys.exit(1)
        if not edges_file.exists():
            logger.info(f"Error: Edges file not found: {edges_file}")
            sys.exit(1)
        validation_passed = validate_kgx_consistency(nodes_file, edges_file)

    sys.exit(0 if validation_passed else 1)


if __name__ == "__main__":
    main()
