ROOTDIR = $(shell pwd)
RUN = uv run
SOURCE_ID = ctd

### Help ###

define HELP
╭───────────────────────────────────────────────────────────╮
  Make for ingest
│ ───────────────────────────────────────────────────────── │
│ Usage:                                                    │
│     make <target>                                         │
│                                                           │
│ Targets:                                                  │
│     help                Print this help message           │
│                                                           │
│     all                 Install everything and test       │
│     fresh               Clean and install everything      │
│     clean               Clean up build artifacts          │
│     clobber             Clean up generated files          │
│                                                           │
│     install             install python requirements       │
│     download            Download data                     │
│     transform           Transform data into KGX           │
│     normalize           Normalize the KGX files           │
│     run                 Run the whole pipeline            │
│                                                           │
│     test                Run all tests                     │
│                                                           │
│     lint                Lint all code                     │
│     format              Format all code                   │
╰───────────────────────────────────────────────────────────╯
endef
export HELP

.PHONY: help
help:
	@printf "$${HELP}"


### Installation and Setup ###

.PHONY: fresh
fresh: clean clobber all

.PHONY: all
all: install test

.PHONY: python
python:
	uv python install

.PHONY: install
install: python
	uv sync

### Testing ###

.PHONY: test
test:
	$(RUN) pytest tests


### Running ###

.PHONY: download
download:
	$(RUN) downloader --output-dir $(ROOTDIR)/data/$(SOURCE_ID) src/translator_ingest/ingests/$(SOURCE_ID)/download.yaml

.PHONY: transform
transform: download
	$(RUN) koza transform src/translator_ingest/ingests/$(SOURCE_ID)/$(SOURCE_ID).yaml --output-dir $(ROOTDIR)/data/$(SOURCE_ID) --output-format jsonl

.PHONY: normalize
normalize: transform
	echo "Normalization placeholder"

.PHONY: run
run: download transform normalize

### Linting, Formatting, and Cleaning ###

.PHONY: clean
clean:
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf `find . -name __pycache__` \
		.venv .ruff_cache .pytest_cache **/.ipynb_checkpoints

.PHONY: clobber
clobber:
	# Add any files to remove here
	@echo "Nothing to remove. Add files to remove to clobber target."

.PHONY: lint
lint: 
	$(RUN) ruff check --diff --exit-zero
	$(RUN) black -l 120 --check --diff src tests

.PHONY: format
format: 
	$(RUN) ruff check --fix --exit-zero
	$(RUN) black -l 120 src tests
