ROOTDIR = $(shell pwd)
RUN = uv run
# Configure which sources to process (default: all available sources)
SOURCES ?= ctd ebi_gene2phenotype go_cam goa sider hpoa diseases


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
│     validate-only       Validate without re-running pipeline │
│     validate-single     Validate only specified sources   │
│                                                           │
│     test                Run all tests                     │
│                                                           │
│     lint                Lint all code                     │
│     format              Format all code                   │
│     spell-fix           Fix spelling errors interactively │
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

.PHONY: download
download:
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix download-,$(SOURCES))

.PHONY: download-%
download-%:
	@echo "Downloading $*..."
	@$(RUN) downloader --output-dir $(ROOTDIR)/data/$* src/translator_ingest/ingests/$*/download.yaml

.PHONY: transform
transform: download
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix transform-,$(SOURCES))

.PHONY: transform-%
transform-%:
	@echo "Transforming $*..."
	@$(RUN) koza transform src/translator_ingest/ingests/$*/$*.yaml --output-dir $(ROOTDIR)/data/$* --output-format jsonl

.PHONY: normalize
normalize: transform
	@echo "Normalization placeholder for sources: $(SOURCES)"

.PHONY: validate
validate: normalize
	$(RUN) python src/translator_ingest/util/validate_biolink_kgx.py --data-dir $(ROOTDIR)/data

.PHONY: validate-only
validate-only:
	$(RUN) python src/translator_ingest/util/validate_biolink_kgx.py --data-dir $(ROOTDIR)/data

.PHONY: validate-single
validate-single: normalize
	@for source in $(SOURCES); do \
		echo "Validating $$source..."; \
		$(RUN) python src/translator_ingest/util/validate_biolink_kgx.py --files $(ROOTDIR)/data/$$source/*_nodes.jsonl $(ROOTDIR)/data/$$source/*_edges.jsonl; \
	done

.PHONY: run
run: normalize

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
