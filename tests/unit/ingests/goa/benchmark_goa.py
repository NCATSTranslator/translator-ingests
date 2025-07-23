#!/usr/bin/env python3
"""
Benchmark script for GOA ingest performance analysis.
Measures timing, memory usage, and other performance metrics.
"""

import time
import psutil
import os
import json
from pathlib import Path
import subprocess
import sys

def get_memory_usage():
    """Get current memory usage in MB."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024

def count_lines(file_path):
    """Count lines in a file."""
    with open(file_path, 'r') as f:
        return sum(1 for _ in f)

def analyze_file_stats(file_path):
    """Analyze file statistics."""
    if not os.path.exists(file_path):
        return None
    
    stats = os.stat(file_path)
    line_count = count_lines(file_path)
    
    return {
        'size_mb': stats.st_size / 1024 / 1024,
        'line_count': line_count,
        'avg_line_size': stats.st_size / line_count if line_count > 0 else 0
    }

def run_benchmark():
    """Run the GOA ingest benchmark."""
    print("GOA Ingest Performance Benchmark")
    print("=" * 50)
    
    # Initial memory usage
    initial_memory = get_memory_usage()
    print(f"Initial memory usage: {initial_memory:.2f} MB")
    
    # Start timing
    start_time = time.time()
    
    # Run the transform
    print("\nRunning GOA transform...")
    try:
        result = subprocess.run(
            ["make", "transform", "SOURCE_ID=goa"],
            capture_output=True,
            text=True,
            timeout=300  # 5 minute timeout
        )
        
        if result.returncode != 0:
            print(f"Transform failed with return code {result.returncode}")
            print(f"Error output: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("Transform timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"Transform failed with exception: {e}")
        return False
    
    # End timing
    end_time = time.time()
    total_time = end_time - start_time
    
    # Final memory usage
    final_memory = get_memory_usage()
    memory_delta = final_memory - initial_memory
    
    print(f"\nPerformance Results:")
    print(f"   Total time: {total_time:.2f} seconds")
    print(f"   Final memory usage: {final_memory:.2f} MB")
    print(f"   Memory delta: {memory_delta:.2f} MB")
    
    # Analyze output files
    print(f"\n Output File Analysis:")
    
    nodes_file = "data/goa/goa_nodes.jsonl"
    edges_file = "data/goa/goa_edges.jsonl"
    
    nodes_stats = analyze_file_stats(nodes_file)
    edges_stats = analyze_file_stats(edges_file)
    
    if nodes_stats:
        print(f"   Nodes file ({nodes_file}):")
        print(f"     Size: {nodes_stats['size_mb']:.2f} MB")
        print(f"     Lines: {nodes_stats['line_count']:,}")
        print(f"     Avg line size: {nodes_stats['avg_line_size']:.1f} bytes")
    
    if edges_stats:
        print(f"   Edges file ({edges_file}):")
        print(f"     Size: {edges_stats['size_mb']:.2f} MB")
        print(f"     Lines: {edges_stats['line_count']:,}")
        print(f"     Avg line size: {edges_stats['avg_line_size']:.1f} bytes")
    
    # Calculate processing rates
    if edges_stats:
        records_per_second = edges_stats['line_count'] / total_time
        mb_per_second = edges_stats['size_mb'] / total_time
        
        print(f"\n Processing Rates:")
        print(f"   Records per second: {records_per_second:.0f}")
        print(f"   MB per second: {mb_per_second:.2f}")
        print(f"   Records per MB: {records_per_second / mb_per_second:.0f}")
    
    # Check for input files
    print(f"\n Input File Analysis:")
    input_files = [
        "data/goa/goa_human.gaf.gz",
        "data/goa/mgi.gaf.gz"
    ]
    
    total_input_size = 0
    for input_file in input_files:
        if os.path.exists(input_file):
            size_mb = os.path.getsize(input_file) / 1024 / 1024
            total_input_size += size_mb
            print(f"   {input_file}: {size_mb:.2f} MB")
    
    if total_input_size > 0 and edges_stats:
        compression_ratio = edges_stats['size_mb'] / total_input_size
        print(f"   Output/Input size ratio: {compression_ratio:.2f}x")
    
    print(f"\n Benchmark completed successfully!")
    return True

def analyze_sample_data():
    """Analyze sample data from output files."""
    print(f"\n Sample Data Analysis:")
    
    # Analyze nodes
    nodes_file = "data/goa/goa_nodes.jsonl"
    if os.path.exists(nodes_file):
        print(f"\n Node Examples:")
        
        # Good gene node example
        with open(nodes_file, 'r') as f:
            for line in f:
                node = json.loads(line)
                if node.get('category') == ['biolink:Gene'] and 'name' in node and 'in_taxon' in node:
                    print(f"    Good Gene Node:")
                    print(f"      {json.dumps(node, indent=6)}")
                    break
        
        # Bad gene node example (missing properties)
        with open(nodes_file, 'r') as f:
            for line in f:
                node = json.loads(line)
                if node.get('category') == ['biolink:Gene'] and ('name' not in node or 'in_taxon' not in node):
                    print(f"    Bad Gene Node (missing properties):")
                    print(f"      {json.dumps(node, indent=6)}")
                    break
    
    # Analyze edges
    edges_file = "data/goa/goa_edges.jsonl"
    if os.path.exists(edges_file):
        print(f"\n Edge Examples:")
        
        # Good edge example (with real PMID)
        with open(edges_file, 'r') as f:
            for line in f:
                edge = json.loads(line)
                pubs = edge.get('publications', [])
                if any('PMID:' in pub and 'GO_REF' not in pub for pub in pubs):
                    print(f"    Good Edge (real PMID):")
                    print(f"      {json.dumps(edge, indent=6)}")
                    break
        
        # Bad edge example (with GO_REF)
        with open(edges_file, 'r') as f:
            for line in f:
                edge = json.loads(line)
                pubs = edge.get('publications', [])
                if any('GO_REF' in pub for pub in pubs):
                    print(f"      Bad Edge (GO_REF publication):")
                    print(f"      {json.dumps(edge, indent=6)}")
                    break

if __name__ == "__main__":
    print("GOA Ingest Benchmark Tool")
    print("=" * 30)
    
    # Check if we're in the right directory
    if not os.path.exists("Makefile"):
        print("  Error: Makefile not found. Please run this script from the translator-ingests root directory.")
        sys.exit(1)
    
    # Run benchmark
    success = run_benchmark()
    
    if success:
        # Analyze sample data
        analyze_sample_data()
        
        print(f"\n Performance Notes:")
        print(f"   - Koza processes records row-by-row, not in chunks")
        print(f"   - This approach is memory-friendly but may not be the fastest")
        print(f"   - Koza was chosen for its shared interface parser and existing functionality")
        print(f"   - For more info, see: https://github.com/monarch-initiative/koza")
    else:
        print(f"\n Benchmark failed!")
        sys.exit(1) 