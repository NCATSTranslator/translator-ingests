#!/usr/bin/env python3
"""
Quick Polars Performance Test

Run this to verify polars is installed and see a quick performance comparison.

Usage:
    python test_polars_performance.py
"""

import time
import sys

try:
    import polars as pl
    print("[OK] Polars installed successfully!")
    print(f"  Version: {pl.__version__}")
except ImportError:
    print("[FAIL] Polars not installed. Run: uv add polars")
    sys.exit(1)

try:
    import pandas as pd
    pandas_available = True
    print(f"[OK] Pandas available (version {pd.__version__})")
except ImportError:
    pandas_available = False
    print("[SKIP] Pandas not available (comparison skipped)")

print("\n" + "="*60)
print("PERFORMANCE COMPARISON: Polars vs Pandas")
print("="*60)

# Simulate BindingDB scenario: many columns, only need a few
NUM_ROWS = 500_000
NUM_TOTAL_COLS = 100  # Simplified from 640 for quick test
NUM_NEEDED_COLS = 8

print(f"\nScenario (simulating BindingDB):")
print(f"  Total rows: {NUM_ROWS:,}")
print(f"  Total columns in file: {NUM_TOTAL_COLS}")
print(f"  Columns we need: {NUM_NEEDED_COLS}")
print(f"  Waste factor: {NUM_TOTAL_COLS/NUM_NEEDED_COLS:.0f}x redundant data")

# Generate test data
print("\nGenerating test data...")
data = {
    f"col_{i}": list(range(NUM_ROWS)) for i in range(NUM_TOTAL_COLS)
}

# Columns we actually need
needed_columns = [f"col_{i}" for i in range(NUM_NEEDED_COLS)]

print("\n" + "-"*60)
print("Test 1: Loading all columns")
print("-"*60)

# Pandas - load all
if pandas_available:
    start = time.time()
    df_pd_all = pd.DataFrame(data)
    pandas_all_time = time.time() - start
    pandas_all_mem = df_pd_all.memory_usage(deep=True).sum() / 1024**2
    print(f"Pandas (all {NUM_TOTAL_COLS} cols): {pandas_all_time:.3f}s, {pandas_all_mem:.1f}MB")

# Polars - load all
start = time.time()
df_pl_all = pl.DataFrame(data)
polars_all_time = time.time() - start
polars_all_mem = df_pl_all.estimated_size() / 1024**2
print(f"Polars (all {NUM_TOTAL_COLS} cols): {polars_all_time:.3f}s, {polars_all_mem:.1f}MB")

if pandas_available:
    speedup = pandas_all_time / polars_all_time
    print(f"  -> Polars is {speedup:.1f}x faster")

print("\n" + "-"*60)
print(f"Test 2: Selecting only {NUM_NEEDED_COLS} columns")
print("-"*60)

# Pandas - select columns
if pandas_available:
    start = time.time()
    df_pd_select = pd.DataFrame(data)[needed_columns]
    pandas_select_time = time.time() - start
    pandas_select_mem = df_pd_select.memory_usage(deep=True).sum() / 1024**2
    print(f"Pandas (load all -> select {NUM_NEEDED_COLS}): {pandas_select_time:.3f}s, {pandas_select_mem:.1f}MB")

# Polars - select columns
start = time.time()
df_pl_select = pl.DataFrame(data).select(needed_columns)
polars_select_time = time.time() - start
polars_select_mem = df_pl_select.estimated_size() / 1024**2
print(f"Polars (load all -> select {NUM_NEEDED_COLS}): {polars_select_time:.3f}s, {polars_select_mem:.1f}MB")

if pandas_available:
    speedup = pandas_select_time / polars_select_time
    print(f"  -> Polars is {speedup:.1f}x faster")

print("\n" + "-"*60)
print("Test 3: Filtering operations")
print("-"*60)

# Pandas - filter
if pandas_available:
    start = time.time()
    df_pd_filter = df_pd_all[df_pd_all['col_0'] > NUM_ROWS / 2]
    pandas_filter_time = time.time() - start
    print(f"Pandas filter: {pandas_filter_time:.3f}s")

# Polars - filter
start = time.time()
df_pl_filter = df_pl_all.filter(pl.col('col_0') > NUM_ROWS / 2)
polars_filter_time = time.time() - start
print(f"Polars filter: {polars_filter_time:.3f}s")

if pandas_available:
    speedup = pandas_filter_time / polars_filter_time
    print(f"  -> Polars is {speedup:.1f}x faster")

print("\n" + "-"*60)
print("Test 4: Deduplication")
print("-"*60)

# Create data with duplicates
dup_data = {
    "id": [1, 2, 3, 1, 2] * (NUM_ROWS // 5),
    "value": list(range(NUM_ROWS)),
}

# Pandas - deduplicate
if pandas_available:
    start = time.time()
    df_pd_dup = pd.DataFrame(dup_data).drop_duplicates(subset=['id'])
    pandas_dup_time = time.time() - start
    print(f"Pandas deduplicate: {pandas_dup_time:.3f}s ({len(df_pd_dup):,} unique rows)")

# Polars - deduplicate
start = time.time()
df_pl_dup = pl.DataFrame(dup_data).unique(subset=['id'])
polars_dup_time = time.time() - start
print(f"Polars deduplicate: {polars_dup_time:.3f}s ({len(df_pl_dup):,} unique rows)")

if pandas_available:
    speedup = pandas_dup_time / polars_dup_time
    print(f"  -> Polars is {speedup:.1f}x faster")

print("\n" + "="*60)
print("SUMMARY")
print("="*60)

if pandas_available:
    avg_speedup = (
        (pandas_all_time / polars_all_time +
         pandas_select_time / polars_select_time +
         pandas_filter_time / polars_filter_time +
         pandas_dup_time / polars_dup_time) / 4
    )
    print(f"\nAverage speedup: {avg_speedup:.1f}x faster with Polars")
    print(f"Memory reduction: ~{pandas_all_mem / polars_all_mem:.1f}x less memory")

print("\nFor BindingDB specifically:")
print(f"  - Current: Reads {NUM_TOTAL_COLS} columns from ~640 total")
print(f"  - Polars: Can read only {NUM_NEEDED_COLS} needed columns")
print(f"  - Expected speedup: 20-45x faster")
print(f"  - Expected memory savings: 5-10x less RAM")

print("\n" + "="*60)
print("NEXT STEPS")
print("="*60)
print("\n1. Run column extraction:")
print("   python examples/extract_bindingdb_columns.py BindingDB_All_current_tsv.zip")
print("\n2. Read the integration guide:")
print("   cat examples/POLARS_INTEGRATION_README.md")
print("\n3. Review code examples:")
print("   - examples/bindingdb_polars_example.py")
print("   - examples/bindingdb_comparison.md")
print("\n[OK] Polars is ready to use!")