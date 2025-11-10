ROOTDIR = $(shell pwd)
RUN = uv run
# Configure which sources to process (default: all available sources)
SOURCES ?= alliance ctd diseases gene2phenotype go_cam goa hpoa panther sider

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
│     run                 Run pipeline (download→transform→normalize→validate) │
│     transform           Transform the source to KGX       │
│     validate            Validate all sources in data/     │
│     validate-single     Validate only specified sources   │
│     validate-only       Validate without re-running pipeline │
│     merge               Merge specified sources into one KG │
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
│     make merge SOURCES="ctd go_cam goa"                   │
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
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix run-,$(SOURCES))

.PHONY: run-%
run-%:
	@echo "Running pipeline for $*..."
	@$(RUN) python src/translator_ingest/pipeline.py $*

.PHONY: transform
transform:
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix transform-,$(SOURCES))

.PHONY: transform-%
transform-%:
	@echo "Transform only for $*..."
	@$(RUN) python src/translator_ingest/pipeline.py $* --transform-only

.PHONY: validate
validate: run
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix validate-,$(SOURCES))

.PHONY: validate-%
validate-%:
	@echo "Validating $*..."
	@$(RUN) python src/translator_ingest/util/validate_biolink_kgx.py --files $(ROOTDIR)/data/$*/*_nodes.jsonl $(ROOTDIR)/data/$*/*_edges.jsonl

.PHONY: validate-only
validate-only:
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix validate-only-,$(SOURCES))

.PHONY: validate-only-%
validate-only-%:
	@echo "Validating $*..."
	@$(RUN) python src/translator_ingest/util/validate_biolink_kgx.py --files $(ROOTDIR)/data/$*/*_nodes.jsonl $(ROOTDIR)/data/$*/*_edges.jsonl


.PHONY: merge
merge:
	@echo "Merging sources and building translator_kg...";
	$(RUN) python src/translator_ingest/merging.py translator_kg $(SOURCES);

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
