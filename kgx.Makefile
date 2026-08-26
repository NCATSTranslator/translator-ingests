# This Makefile gathers targets for KGX related tasks.

ROOTDIR = $(shell pwd)
RUN = uv run

# By default, kgxval-summary picks the most recently modified transform run for each
# source. To validate an older run instead, pin one or both of:
#   VERSION    the dated version dir, e.g. VERSION=2026-02-16
#   TRANSFORM  the transform hash, e.g. TRANSFORM=e80fcfa7 (with or without the
#              "transform_" prefix)
VERSION ?=
TRANSFORM ?=

.PHONY: kgxval-summary
kgxval-summary:
	@$(MAKE) -j $(words $(SOURCES)) $(addprefix kgxval-summary-,$(SOURCES)) VERSION=$(VERSION) TRANSFORM=$(TRANSFORM)

.PHONY: kgxval-summary-%
kgxval-summary-%:
	@echo "Generating KGXval summary for $*..."
	# kgxval needs Python 3.13 + bmt-from-git, so it runs via uvx, isolated from this
	# project's 3.12 env. It reads the JSONL output directly. The rollup-sampling pass
	# is slow (~5-10 min).
	@VERSION_GLOB="$(if $(VERSION),$(VERSION),*)"; \
	case "$(TRANSFORM)" in \
		"") TRANSFORM_GLOB="transform_*" ;; \
		transform_*) TRANSFORM_GLOB="$(TRANSFORM)" ;; \
		*) TRANSFORM_GLOB="transform_$(TRANSFORM)" ;; \
	esac; \
	NODES_FILE=$$(ls -t $(ROOTDIR)/data/$*/$$VERSION_GLOB/$$TRANSFORM_GLOB/*_nodes.jsonl 2>/dev/null | head -1); \
	if [ -z "$$NODES_FILE" ]; then \
		echo "Error: Could not find a transform-level nodes file for $* under data/$*/$$VERSION_GLOB/$$TRANSFORM_GLOB/"; \
		exit 1; \
	fi; \
	TRANSFORM_DIR=$$(dirname "$$NODES_FILE"); \
	EDGES_FILE=$$(ls "$$TRANSFORM_DIR"/*_edges.jsonl 2>/dev/null | head -1); \
	if [ -z "$$EDGES_FILE" ]; then \
		echo "Error: Found nodes file $$NODES_FILE but no matching edges file in $$TRANSFORM_DIR"; \
		exit 1; \
	fi; \
	echo "Using nodes file: $$NODES_FILE"; \
	echo "Using edges file: $$EDGES_FILE"; \
	SHORTNAME=$$(echo "$*" | cut -c1-31); \
	workdir=$$(mktemp -d); \
	mkdir -p "$$workdir/$$SHORTNAME"; \
	cp "$$NODES_FILE" "$$EDGES_FILE" "$$workdir/$$SHORTNAME/"; \
	( cd "$$workdir" && uvx --python 3.13 --from 'git+https://github.com/monarch-initiative/kgxval' \
	many_sources "$$workdir" ); \
	xlsx=$$(find "$$workdir/data/output" -name '*.xlsx' | head -1); \
	if [ -z "$$xlsx" ]; then \
		echo "Error: kgxval did not produce an .xlsx report"; \
		exit 1; \
	fi; \
	outdir="$$TRANSFORM_DIR/kgxval"; \
	mkdir -p "$$outdir"; \
	cp "$$xlsx" "$$outdir/$*_kgxval_summary.xlsx"; \
	rm -rf "$$workdir"; \
	echo "Wrote $$outdir/$*_kgxval_summary.xlsx"
