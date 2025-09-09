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
sources := "ctd go_cam goa"

### Help ###

export HELP := """
Just commands for ingest

---------------------------------------------------------------------------

Usage:

    just <target>
    just <target> sources=\\"ctd go_cam\\"

Targets:

    help			Print this help message

    all				Install everything and test
    fresh			Clean and install everything
    clean			Clean up build artifacts
    clean-reports		Clean up validation reports
    clobber			Clean up generated files

    install			install python requirements
    run			        Run pipeline (download->transform->normalize)
    validate			Validate all sources in data/
    validate-single	        Validate only specified sources

    test			Run all tests

    lint			Lint all code
    format			Format all code
    spell-fix		        Fix spelling errors interactively

Configuration:

    sources		        Space-separated list of sources
				Default: ctd go_cam goa

Examples:

    just run
    just validate sources="ctd go_cam"
    just run sources="go_cam"
"""

help:
    echo "{{HELP}}"

# This project uses the uv dependency manager
run := 'uv run'

# Environment variables with defaults
# schema_name := env_var_or_default("<SOME_ENVIRONMENT_VARIABLE_NAME>", "")

# Directory variables
src := "src"

### Installation and Setup ###

setup: _install test

_python:
	uv python install

_install: _python
	uv sync

### Testing ###

# Run all tests
test:
    {{run}} python -m pytest tests

spell_fix:
	{{run}} codespell --skip="./data/*" --ignore-words=.codespellignore --write-changes --interactive=3


### Running ###

download:
	@for source in {{sources}}; do \
		echo "Downloading $$source..."; \
		{{run}} downloader --output-dir {{rootdir}}/data/$$source src/translator_ingest/ingests/$$source/download.yaml; \
	done

transform: download
	@for source in {{sources}}; do \
		echo "Transforming $$source..."; \
		{{run}} koza transform src/translator_ingest/ingests/$$source/$$source.yaml --output-dir {{rootdir}}/data/$$source --output-format jsonl; \
	done

normalize: transform
	@echo "Normalization placeholder for sources: {{sources}}"

validate: normalize
	{{run}} python src/translator_ingest/util/validate_kgx.py --data-dir {{rootdir}}/data

validate-single: normalize
	@for source in {{sources}}; do \
		echo "Validating $$source..."; \
		{{run}} python src/translator_ingest/util/validate_kgx.py --files {{rootdir}}/data/$$source/*_nodes.jsonl {{rootdir}}/data/$$source/*_edges.jsonl; \
	done

run: download transform normalize

clean-reports:
	@echo "Cleaning validation reports..."
	rm -rf {{rootdir}}/data/validation
	@echo "All validation reports removed."

_clobber:
	# Add any files to remove here
	@echo "Nothing to remove. Add files to remove to clobber target."

_lint:
	{{run}} ruff check --diff --exit-zero
	{{run}} black -l 120 --check --diff src tests

_format:
	{{run}} ruff check --fix --exit-zero
	{{run}} black -l 120 src tests

import "project.justfile"
