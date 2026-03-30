# Build Orchestrator — End-to-End Pipeline

## Overview

The build orchestrator runs the full translator-ingests pipeline end-to-end: executing all sources in parallel, merging into a single graph, generating releases, uploading to S3, and producing automated reports.

Key modules:

- `run_build.py` — Orchestrates all 4 pipeline stages sequentially
- `build_report.py` — Generates reports from pipeline artifacts with zero manual data entry
- `utils.py` — Shared helpers (duration formatting, symlink management, constants)

## Pipeline Stages

The build runs 4 stages in order:

```
RUN -> MERGE -> RELEASE -> UPLOAD
```

| Stage | What it does | Parallelism |
|-------|-------------|-------------|
| **RUN** | Executes `pipeline.run_pipeline(source)` for each source (download, transform, normalize) | Parallel via ProcessPoolExecutor |
| **MERGE** | Combines all sources into a single graph (`translator_kg`). Failed sources use their last successful build data. | Sequential |
| **RELEASE** | Generates tar.zst release archives for each source | Sequential per source |
| **UPLOAD** | Auto-discovers data/release sources and uploads to S3 with EBS cleanup | Sequential per source |

The RUN stage uses `ProcessPoolExecutor` to run all sources in parallel. Each worker process runs in its own subprocess with independent logging. The default worker count equals the number of sources, configurable with `--max-workers`.

## Usage

### Full Build (Recommended)

```bash
# Full pipeline: all sources, all stages
make build

# Specific sources only
make build SOURCES="ctd go_cam"

# Skip the upload stage (local build only)
make build NO_UPLOAD=true

# Limit parallel workers during RUN stage
make build MAX_WORKERS=4

# Overwrite previously generated files
make build OVERWRITE=true

# Lower the memory abort threshold (default: 90%)
make build MEMORY_THRESHOLD=85
```

Direct Python invocation:

```bash
uv run python -m translator_ingest.util.run_build.run_build
uv run python -m translator_ingest.util.run_build.run_build --sources "ctd go_cam"
uv run python -m translator_ingest.util.run_build.run_build --no-upload
uv run python -m translator_ingest.util.run_build.run_build --max-workers 4
uv run python -m translator_ingest.util.run_build.run_build --memory-threshold 85
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--sources` | All sources in Makefile | Space-separated list of sources to process |
| `--graph-id` | `translator_kg` | Merged graph identifier |
| `--node-properties` | `ncbi_gene` | Sources that are node-properties-only (excluded from releases) |
| `--overwrite` | off | Overwrite previously generated files |
| `--no-upload` | off | Skip S3 upload stage |
| `--max-workers` | number of sources | Max parallel workers for RUN stage |
| `--memory-threshold` | 90 | System memory % that triggers graceful abort |

### Individual Stages

The stages can also be run separately via Makefile:

```bash
make run                         # Run pipeline (download, transform, normalize)
make run SOURCES="ctd go_cam"    # Run for specific sources
make merge                       # Merge sources into single graph
make release                     # Generate release archives
make upload-all                  # Upload all to S3 with EBS cleanup
```

## Error Handling

- If a source fails during the RUN stage, other sources continue processing
- Failed sources use their last successful build data for MERGE, RELEASE, and UPLOAD (the `latest-build.json` still points to the previous good version)
- All errors are collected in `errors.log` and the build report
- The upload stage is skipped if `--no-upload` is set
- Exit code 1 if any source or stage failed, exit code 2 if the build was aborted due to memory pressure

## Progress Display

During execution, the build shows a real-time terminal display with:

- Stage status indicators (pending / running / completed / failed)
- Running and completed source counts during RUN stage
- Memory usage (peak/current)
- Per-stage elapsed time
- ETA based on completed sources

## Performance Tracking

A background thread (`PerformanceTracker`) samples memory and CPU every 2 seconds throughout the build. It records:

- Peak, average, and minimum memory per stage
- CPU utilization
- Disk usage snapshots at start and end
- Per-source memory tracking during the RUN stage

Performance data is saved to `reports/{timestamp}/performance.json`.

## Memory Guardian

The `PerformanceTracker` also acts as a memory guardian to prevent the OS OOM-killer from silently terminating the build. On every sample it checks system-wide memory usage against two thresholds:

| Threshold | Default | Behavior |
|-----------|---------|----------|
| Warning   | 80%     | Logs a warning once; resets if memory drops back down |
| Critical  | 90%     | After 3 consecutive samples (~6 s sustained), triggers graceful abort |

The consecutive-sample requirement avoids false positives from transient GC or allocation spikes.

When the critical threshold is breached:

1. Pending futures in the RUN stage are cancelled
2. Already-running workers are given up to 5 minutes to finish
3. Remaining stages (MERGE, RELEASE, UPLOAD) are skipped
4. A partial build report is still generated with a `BUILD ABORTED` note
5. The process exits with code **2**

Override the threshold via `--memory-threshold` (CLI) or `MEMORY_THRESHOLD` (Makefile).

## Log Directory Structure

Each build creates per-stage log directories sharing the same timestamp, with `latest` symlinks:

```
logs/
├── run/
│   ├── latest -> 2026_03_16_143000
│   └── 2026_03_16_143000/
│       └── run.log            # Complete live log for RUN stage (all sources interleaved)
├── merge/
│   ├── latest -> 2026_03_16_143000
│   └── 2026_03_16_143000/
│       └── merge.log
├── release/
│   ├── latest -> 2026_03_16_143000
│   └── 2026_03_16_143000/
│       └── release.log
├── upload/
│   ├── latest -> 2026_03_16_143000
│   └── 2026_03_16_143000/
│       └── upload.log
└── errors/
    ├── latest -> 2026_03_16_143000
    └── 2026_03_16_143000/
        └── errors.log         # All errors and warnings across all stages
```

During parallel source execution in the RUN stage, each source prefixes its log lines with `[source_name]` so interleaved output can be traced back to individual sources.

The logs directory is gitignored.

## Report Directory Structure

Each build creates a timestamped report directory (same timestamp as logs) for JSON artifacts:

```
reports/
├── latest -> 2026_03_16_143000
└── 2026_03_16_143000/
    ├── build-report.txt       # Human-readable report
    ├── build-report.json      # Machine-readable report
    ├── performance.json       # Detailed performance metrics
    └── stages/
        ├── run/
        │   ├── {source}.json  # Per-source results
        │   └── _summary.json
        ├── merge/
        │   └── _summary.json
        ├── release/
        │   └── _summary.json
        └── upload/
            ├── upload-results.json
            └── _summary.json
```

## Build Report Generator

`build_report.py` reads pipeline artifacts to generate comprehensive reports independently of the build orchestrator. It can be run after any pipeline execution, not just `make build`.

```bash
# Generate report for all sources
make report

# Generate report for specific sources
make report SOURCES="ctd go_cam"
```

The report collects data from:

- `latest-build.json` — per-source build metadata (version, timing)
- `validation-report.json` — validation results (node/edge counts, errors)
- `latest-release.json` — release status
- `graph-metadata.json` — merge verification
- `upload-results-latest.json` — upload statistics (if available)

Output formats: human-readable text report and machine-readable JSON.

## EC2 Instance Requirements

**Recommended instance:** t3.2xlarge with 400 GiB EBS storage

Smaller instances have resulted in crashes when running all ingests due to memory and storage constraints. The current number of ingests requires significant resources for parallel processing and storing intermediate build artifacts.

## Reproducibility

This setup is portable and not tied to any specific AWS account. To run on a new EC2 instance:

1. Launch a t3.2xlarge instance with 400 GiB EBS storage
2. Create an S3 bucket for storing outputs
3. Create an IAM role with the required S3 permissions (see [storage README](../storage/README.md))
4. Attach the IAM role to the EC2 instance
5. Clone the repository and run `make install`
6. Run `make build` to execute the full pipeline
