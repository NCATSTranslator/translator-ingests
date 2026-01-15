ROOTDIR = $(shell pwd)
RUN = uv run
# Configure which sources to process (default: all available sources)
SOURCES ?= alliance bgee bindingdb chembl cohd ctd ctkp dakp dgidb diseases drug_rep_hub gtopdb gene2phenotype geneticskp go_cam goa hpoa icees intact ncbi_gene panther semmeddb sider signor tmkp ttd ubergraph

# Set to any non-empty value to overwrite previously generated files
OVERWRITE ?=
# Clear OVERWRITE if explicitly set to "false" or "False"
ifeq ($(OVERWRITE),false)
OVERWRITE :=
endif
ifeq ($(OVERWRITE),False)
OVERWRITE :=

endif

# Graph ID for merge target (default: translator_kg)
GRAPH_ID ?= translator_kg

# Include additional makefiles
include rig.Makefile
include doc.Makefile


### Help ###

define HELP
╭──────────────────────────────────────────────────────────────────────────────╮
│ Make for translator-ingests                                                  │
│ ──────────────────────────────────────────────────────────────────────────── │
│ Usage:                                                                       │
│     make <target>                                                            │
│     make <target> SOURCES="ctd go_cam"                                       │
│                                                                              │
│ Targets:                                                                     │
│     help                Print this help message                              │
│                                                                              │
│     all                 Install everything and test                          │
│     fresh               Clean and install everything                         │
│     clean               Clean up build artifacts                             │
│     clean-reports       Clean up validation reports                          │
│     clobber             Clean up generated files                             │
│                                                                              │
│     install             Install python requirements                          │
│     run                 Run the full ingest pipeline for specified sources   │
│                         (download → transform → normalize → merge → validate)│
│     transform           Run only download and transform                      │
│     validate            Validate all sources in data/                        │
│     validate-single     Validate only specified sources                      │
│     release             Generate releases for the specified sources          │
│     merge               Merge specified sources into one KG                  │
│                                                                              │
│     test                Run all tests                                        │
│                                                                              │
│     upload              Upload data and releases to S3                       │
│     upload-all          Upload all sources to S3                             │
│     cleanup-ebs         Clean up old EBS versions                            │
│     cleanup-s3          Delete all objects from S3 bucket (DANGEROUS)        │
│     cleanup-s3-source   Delete specific source from S3 (DANGEROUS)           │
│                                                                              │
│     lint                Lint all code                                        │
│     lint-fix            Fix linting errors automatically                     │
│     format              Format all code                                      │
│     spell-fix           Fix spelling errors interactively                    │
│     new-rig             Create RIG from template (requires INFORES and NAME) │
│     validate-rigs       Validate all RIG files against the schema            │
│                                                                              │
│     docs                Build documentation locally                          │
│     docs-serve          Build and serve docs on port 8000                    │
│     docs-clean          Clean documentation build                            │
│                                                                              │
│ Configuration:                                                               │
│     SOURCES             Space-separated list of sources                      │
│                         Default: all available sources                       │
│     GRAPH_ID            Graph ID for merged graphs                           │
│                         Default: translator_kg                               │
│                                                                              │
│ Examples:                                                                    │
│     # Run pipeline for all sources                                           │
│     make run                                                                 │
│     # Run pipeline only for specified sources                                │
│     make run SOURCES="go_cam"                                                │
│                                                                              │
│     # Validate all sources                                                   │
│     make validate                                                            │
│     # Validate only specified sources                                        │
│     make validate SOURCES="go_cam"                                           │
│                                                                              │
│     # Make releases for all sources                                          │
│     make release                                                             │
│     # Make releases only for specified sources                               │
│     make release SOURCES="ctd go_cam goa"                                    │
│                                                                              │
│     # Merge all sources into one graph named translator_kg                   │
│     make merge                                                               │
│     # Merge specified sources into a graph named example_custom_graph        │
│     make merge GRAPH_ID=example_custom_graph SOURCES="ctd go_cam goa"        │
╰──────────────────────────────────────────────────────────────────────────────╯
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
	@$(RUN) python src/translator_ingest/pipeline.py $* $(if $(OVERWRITE),--overwrite)

.PHONY: transform
transform:
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix transform-,$(SOURCES))

.PHONY: transform-%
transform-%:
	@echo "Transform only for $*..."
	@$(RUN) python src/translator_ingest/pipeline.py $* $(if $(OVERWRITE),--overwrite) --transform-only


.PHONY: validate
validate: run
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix validate-,$(SOURCES))

.PHONY: validate-%
validate-%:
	@echo "Validating $*..."
	@NODES_FILE=$$(find $(ROOTDIR)/data/$* -name "normalized_nodes.jsonl" -type f | head -1 || find $(ROOTDIR)/data/$* -name "*nodes.jsonl" -type f | head -1); \
	EDGES_FILE=$$(find $(ROOTDIR)/data/$* -name "normalized_edges.jsonl" -type f | head -1 || find $(ROOTDIR)/data/$* -name "*edges.jsonl" -type f | head -1); \
	if [ -z "$$NODES_FILE" ] || [ -z "$$EDGES_FILE" ]; then \
		echo "Error: Could not find nodes or edges files for $*"; \
		exit 1; \
	fi; \
	echo "Using nodes file: $$NODES_FILE"; \
	echo "Using edges file: $$EDGES_FILE"; \
	$(RUN) python src/translator_ingest/util/validate_biolink_kgx.py --files "$$NODES_FILE" --files "$$EDGES_FILE"

.PHONY: merge
merge:
	@echo "Merging sources and building $(GRAPH_ID)..."
	$(RUN) python src/translator_ingest/merging.py $(GRAPH_ID) $(SOURCES) $(if $(OVERWRITE),--overwrite)

.PHONY: release
release:
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix release-,$(SOURCES))
	@$(RUN) python src/translator_ingest/release.py --summary

.PHONY: release-%
release-%:
	@echo "Creating release for $*..."
	@$(RUN) python src/translator_ingest/release.py $*

### S3 Upload and Storage Management ###

.PHONY: upload
upload:
	@echo "Uploading sources to S3: $(SOURCES)"
	@$(RUN) python src/translator_ingest/upload_s3.py $(SOURCES)

.PHONY: upload-%
upload-%:
	@echo "Uploading $* to S3..."
	@$(RUN) python src/translator_ingest/upload_s3.py $*

.PHONY: upload-all
upload-all:
	@echo "Uploading all sources to S3..."
	@$(RUN) python src/translator_ingest/upload_s3.py

.PHONY: cleanup-ebs
cleanup-ebs:
	@echo "Cleaning up old versions from EBS for sources: $(SOURCES)"
	@for source in $(SOURCES); do \
		echo "Cleaning up $$source..."; \
		$(RUN) python -c "from translator_ingest.util.storage.s3 import cleanup_old_source_versions, cleanup_old_releases; \
		cleanup_old_source_versions('$$source'); cleanup_old_releases('$$source')"; \
	done

.PHONY: cleanup-s3
cleanup-s3:
	@echo "WARNING: This will delete ALL objects from the S3 bucket!"
	@$(RUN) python -c "from translator_ingest.util.storage.s3 import cleanup_s3_bucket; cleanup_s3_bucket()"

.PHONY: cleanup-s3-source
cleanup-s3-source:
	@echo "WARNING: This will delete source data from S3!"
	@for source in $(SOURCES); do \
		echo "Deleting $$source from S3..."; \
		$(RUN) python -c "from translator_ingest.util.storage.s3 import cleanup_s3_source; cleanup_s3_source('$$source')"; \
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

.PHONY: lint-fix
lint-fix:
	$(RUN) codespell --skip="./data/*,**/site-packages" --ignore-words=.codespellignore
	$(RUN) ruff check --fix

.PHONY: spell-fix
spell-fix:
	$(RUN) codespell --skip="./data/*,**/site-packages" --ignore-words=.codespellignore --write-changes --interactive=3
