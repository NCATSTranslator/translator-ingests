ROOTDIR = $(shell pwd)
RUN = uv run
# Configure which sources to process (default: all available sources)
SOURCES ?= alliance ctd diseases ebi_gene2phenotype go_cam goa hpoa sider

# Include additional makefiles
include rig.Makefile
include doc.Makefile

### Help ###

define HELP
╭───────────────────────────────────────────────────────────╮
  Make for ingest
│ ───────────────────────────────────────────────────────── │
│ Usage:                                                    │
│     make <target>                                         │
│     make <target> SOURCES="ctd go_cam"                    │
│                                                           │
│ Targets:                                                  │
│     help                Print this help message           │
│                                                           │
│     all                 Install everything and test       │
│     fresh               Clean and install everything      │
│     clean               Clean up build artifacts          │
│     clean-reports       Clean up validation reports       │
│     clobber             Clean up generated files          │
│                                                           │
│     install             install python requirements       │
│     run                 Run pipeline (download→transform→normalize) │
│     validate            Validate all sources in data/     │
│     validate-single     Validate only specified sources   │
│                                                           │
│     test                Run all tests                     │
│                                                           │
│     lint                Lint all code                     │
│     format              Format all code                   │
│     spell-fix           Fix spelling errors interactively │
│     new-rig             Create a new RIG from template (requires INFORES and NAME)" │
│			validate-rigs       Validate all RIG files against the schema" │
│                                                           │
│     docs                Build documentation locally       │
│     docs-serve          Build and serve docs on port 8000│
│     docs-clean          Clean documentation build        │
│                                                           │
│ Configuration:                                            │
│     SOURCES             Space-separated list of sources   │
│                         Default: ctd go_cam goa           │
│                                                           │
│ Examples:                                                 │
│     make run                                              │
│     make validate SOURCES="ctd go_cam"                    │
│     make run SOURCES="go_cam"                             │
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
	$(RUN) codespell --skip="./data/*,**/site-packages" --ignore-words=.codespellignore
	$(RUN) ruff check


### Running ###

.PHONY: run
run:
	@for source in $(SOURCES); do \
		echo "Running pipeline for $$source..."; \
		$(RUN) python src/translator_ingest/pipeline.py $$source; \
	done

.PHONY: transform
transform:
	@for source in $(SOURCES); do \
		echo "Transform only for $$source..."; \
		$(RUN) python src/translator_ingest/pipeline.py $$source --transform-only; \
	done

.PHONY: validate
validate: run
	$(RUN) python src/translator_ingest/util/validate_kgx.py --data-dir $(ROOTDIR)/data;

.PHONY: validate-single
validate-single: run
	@for source in $(SOURCES); do \
		echo "Validating $$source..."; \
		$(RUN) python src/translator_ingest/util/validate_kgx.py --files $(ROOTDIR)/data/$$source/*_nodes.jsonl $(ROOTDIR)/data/$$source/*_edges.jsonl; \
	done

### Linting, Formatting, and Cleaning ###

.PHONY: clean
clean:
	rm -f `find . -type f -name '*.py[co]' `
	rm -rf `find . -name __pycache__` \
		.venv .ruff_cache .pytest_cache **/.ipynb_checkpoints

.PHONY: clean-reports
clean-reports:
	@echo "Cleaning validation reports..."
	rm -rf $(ROOTDIR)/data/validation
	@echo "All validation reports removed."

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

.PHONY: spell-fix
spell-fix:
	$(RUN) codespell --skip="./data/*,**/site-packages" --ignore-words=.codespellignore --write-changes --interactive=3
