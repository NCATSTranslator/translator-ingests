# On Windows the bash shell that comes with Git for Windows should be used.
# If it is not on path, give the path to the executable in the following line.
#set windows-shell := ["C:/Program Files/Git/usr/bin/sh", "-cu"]

# Load environment variables from config.public.mk or specified file
set dotenv-load := true
# set dotenv-filename := env_var_or_default("LINKML_ENVIRONMENT_FILENAME", "config.public.mk")
set dotenv-filename := x'${LINKML_ENVIRONMENT_FILENAME:-config.public.mk}'

# List all commands as default command. The prefix "_" hides the command.
_default:
    @just --list

# Set cross-platform Python shebang line (assumes presence of launcher on Windows)
shebang := if os() == 'windows' {
  'py'
} else {
  '/usr/bin/env python3'
}

rootdir :=`pwd`

# Environment variables with defaults
# env_var_name := env_var_or_default("<SOME_ENVIRONMENT_VARIABLE_NAME>", "")
schema_name := env_var_or_default("LINKML_SCHEMA_NAME", "")

source_schema_path := `pwd`+"/$UV_PROJECT_ENVIRONMENT/Lib/site-packages/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml"

sources := "ctd go_cam goa"

### Help ###

export HELP := """
╭──────────────────────────────────────────────────────────────────╮
│   Just commands for ingest                                       │
│ ──────────────────────────────────────────────────────────────── │
│ Usage:                                                           │
│     just <target>    # uses default list of sources              │
│     just sources=\\"ctd go_cam\\" <target>                           │
│                                                                  │
│ Targets:                                                         │
│     help              Print this help message                    │
│                                                                  │
│     setup             Install everything and test                │
│     fresh             Clean and install everything               │
│     install           Install python requirements                │
│                                                                  │
│     clean             Clean up build artifacts                   │
│     clean-reports     Clean up validation reports                │
│     clobber           Clean up data and generated files          │
│                                                                  │
│     new-rig           Create a new resource ingest guide (RIG)   │
│     validate-rigs     Validate user-curated RIG file content     │
│                                                                  │
│     run               Run pipeline                               │
│                       (download->transform->normalize->validate) │
│                                                                  │
│     validate          Validate all sources in data/              │
│     validate-single   Validate only specified sources            │
│                                                                  │
│     test              Run all tests                              │
│                                                                  │
│     lint              Lint all code                              │
│     format            Format all code                            │
│     spell-fix         Fix spelling errors interactively          │
│                                                                  │
│ Configuration:                                                   │
│     sources           Space-separated list of sources            │
│                       Default: \\"ctd go_cam goa\\"                  │
│ Examples:                                                        │
│     just run    # uses default list of sources                   │
│     just  sources=\\"ctd go_cam\\" validate                          │
│     just sources=\\"go_cam\\" run                                    │
╰──────────────────────────────────────────────────────────────────╯
"""

# A few more just recipe details
help:
    echo "{{HELP}}"

# This project uses the uv dependency manager
run := 'uv run'

# Directory variables
src := "src"

### Installation and Setup ###

# clean clobber setup
fresh: clean clobber setup

# Install everything and test
setup: install test

_python:
	uv python install

# Install Python and related library dependencies
install: _python
	uv sync

### Testing ###

# Run all tests
test:
    {{run}} python -m pytest tests
    {{run}} codespell --skip="./data/*,**/site-packages" --ignore-words=.codespellignore
    {{run}} ruff check

### Running ###

# Download data files for specified 'sources'
download:
	for source in {{sources}}; do \
		echo "Downloading $source..."; \
		{{run}} downloader --output-dir {{rootdir}}/data/$source src/translator_ingest/ingests/$source/download.yaml; \
	done

# Transform input data into knowledge graphs for specified 'sources'
transform: download
	for source in {{sources}}; do \
		echo "Transforming $source..."; \
		{{run}} koza transform src/translator_ingest/ingests/$source/$source.yaml --output-dir {{rootdir}}/data/$source --output-format jsonl; \
	done

# Normalize knowledge graphs for specified 'sources'
normalize: transform
	echo "Normalization placeholder for sources: {{sources}}"

# Validate knowledge graphs for specified 'sources'
validate: normalize
	for source in {{sources}}; do \
		echo "Validating $source..."; \
		{{run}} python src/translator_ingest/util/validate_kgx.py --files {{rootdir}}/data/$source/*_nodes.jsonl {{rootdir}}/data/$source/*_edges.jsonl; \
	done

# Run the transformation on all specified 'sources'
run: normalize

# Clean out Python code cache artifacts
clean:
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf `find . -name __pycache__` \
		.venv .ruff_cache .pytest_cache **/.ipynb_checkpoints

_clean-reports:
    echo "Cleaning validation reports..."
    rm -rf {{rootdir}}/data/validation
    echo "All validation reports removed."

# Clean out all input data and output files
clobber:
	rm -rf {{rootdir}}/data
	rm -rf {{rootdir}}/output

# Lint checking of code
lint:
	{{run}} ruff check --diff --exit-zero
	{{run}} black -l 120 --check --diff src tests

# Check and repair code format
format:
	{{run}} ruff check --fix --exit-zero
	{{run}} black -l 120 src tests

# Spell checking and repair of code
spell_fix:
	{{run}} codespell --skip="./data/*,**/site-packages" --ignore-words=.codespellignore --write-changes --interactive=3

## RIG management targets (adapted from https://github.com/biolink/resource-ingest-guide-schema)

infores:= ""
name := ""

# Create a new RIG from template
# Usage: just infores=infores:ctd name="CTD Chemical-Disease Associations" new-rig
new-rig:
    @if [[ -z "{{infores}}" ]]; then \
        echo "INFORES is required. Usage: just INFORES=infores:example NAME='Example RIG' new-rig "; \
    elif [[ -z "{{name}}" ]]; then \
        echo "NAME is required. Usage: just INFORES=infores:example NAME='Example RIG' new-rig "; \
    else \
       {{run}} python {{src}}/scripts/create_rig.py --infores "{{infores}}" --name "{{name}}"; \
    fi

# Validate all RIG files against the schema
validate-rigs:
    @echo "Validating RIG files against schema..."
    @for rig_file in {{src}}/translator_ingest/ingests/*/*_rig.yaml; do \
        if [ -f "$rig_file" ]; then \
            echo "Validating $rig_file"; \
            {{run}} linkml-validate --schema {{source_schema_path}} "$rig_file"; \
        fi; \
    done
    @echo "✓ All RIG files validated (with any errors as indicated)"

import "project.justfile"
