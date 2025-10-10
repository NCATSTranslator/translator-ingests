### Add your own custom Makefile targets here
#
### Staging Makefile area for RIG make targets copied over from resource-ingest-guide-schema
#
#MAKEFLAGS += --warn-undefined-variables
#SHELL := bash
#.SHELLFLAGS := -eu -o pipefail -c
#.DEFAULT_GOAL := help
#.DELETE_ON_ERROR:
#.SUFFIXES:
#.SECONDARY:
#
## environment variables
#.EXPORT_ALL_VARIABLES:
#ifdef LINKML_ENVIRONMENT_FILENAME
#include ${LINKML_ENVIRONMENT_FILENAME}
#else
#include config.public.mk
#endif
#
#RUN = uv run

#SRC = src
#DEST = project
#PYMODEL = $(SRC)/$(SCHEMA_NAME)/datamodel
#DOCDIR = docs
#DOCTEMPLATES = $(SRC)/docs/doc-templates
#
## Use += to append variables from the variables file
#CONFIG_YAML =
#ifdef LINKML_GENERATORS_CONFIG_YAML
#CONFIG_YAML += "--config-file"
#CONFIG_YAML += ${LINKML_GENERATORS_CONFIG_YAML}
#endif
#
#GEN_DOC_ARGS =
#ifdef LINKML_GENERATORS_DOC_ARGS
#GEN_DOC_ARGS += ${LINKML_GENERATORS_DOC_ARGS}
#endif
#
#
## basename of a YAML file in model/
#.PHONY: all clean setup gen-project gendoc new-rig validate-rigs
#
## note: "help" MUST be the first target in the file,
## when the user types "make" they should get help info
#help: status
#	@echo ""
#	@echo "make install -- install dependencies"
#	@echo "make test -- runs tests"
#	@echo "make lint -- perform linting"
#	@echo "make testdoc -- builds docs and runs local test server"
#	@echo "make new-rig -- create a new RIG from template (requires INFORES and NAME)"
#	@echo "make validate-rigs -- validate all RIG files against the schema"
#	@echo "make help -- show this help"
#	@echo ""
#
## install any dependencies required for building
#install:
#	uv sync --extra dev
#.PHONY: install
#
#all: site
#site: gen-project gendoc
#%.yaml: gen-project
#deploy: all mkd-gh-deploy
#
## generates all project files
#gen-project: $(PYMODEL)
#	$(RUN) gen-project ${CONFIG_YAML} -d $(DEST) $(SOURCE_SCHEMA_PATH) && mv $(DEST)/*.py $(PYMODEL)
#
#
#test: test-schema test-python validate-rigs gendoc
#
#test-schema:
#	$(RUN) gen-project ${CONFIG_YAML} -d tmp $(SOURCE_SCHEMA_PATH)
#
#test-python:
#	$(RUN) python -m pytest
#
#lint:
#	$(RUN) linkml-lint $(SOURCE_SCHEMA_PATH)
#
## Test documentation locally
#serve: mkd-serve
#
## Python datamodel
#$(PYMODEL):
#	mkdir -p $@
#
#$(DOCDIR):
#	mkdir -p $@
#
#gendoc: $(DOCDIR)
#	cp $(SOURCE_SCHEMA_PATH) $(DOCDIR) ; \
#	cp $(SRC)/docs/files/*.md $(DOCDIR) ; \
#	cp $(SRC)/docs/files/*.yaml $(DOCDIR) ; \
#	cp -r $(SRC)/docs/images $(DOCDIR)/images ; \
#	$(RUN) python $(SRC)/scripts/rig_to_markdown.py --input-dir $(SRC)/docs/rigs --output-dir $(DOCDIR) ; \
#	$(RUN) python $(SRC)/scripts/generate_rig_index.py --rig-dir $(SRC)/docs/rigs --template-dir $(DOCTEMPLATES) --input-file $(SRC)/docs/files/rig_index.md --output-file $(DOCDIR)/rig_index.md ; \
#	if ls $(SRC)/docs/rigs/*.yaml 1> /dev/null 2>&1; then cp $(SRC)/docs/rigs/*.yaml $(DOCDIR)/; fi ; \
#	$(RUN) gen-doc ${GEN_DOC_ARGS} -d $(DOCDIR) --template-directory $(DOCTEMPLATES) $(SOURCE_SCHEMA_PATH)
#
#testdoc: gendoc serve
#
#MKDOCS = $(RUN) mkdocs
#mkd-%:
#	$(MKDOCS) $*
#
## only necessary if setting up via cookiecutter
#.cruft.json:
#	echo "creating a stub for .cruft.json. IMPORTANT: setup via cruft not cookiecutter recommended!" ; \
#	touch $@

## Validate all RIG files against the schema
#validate-rigs:
#	@echo "Validating RIG files against schema..."
#	@for rig_file in $(SRC)/docs/rigs/*.yaml; do \
#		if [ -f "$$rig_file" ]; then \
#			echo "Validating $$rig_file"; \
#			$(RUN) linkml-validate --schema $(SOURCE_SCHEMA_PATH) "$$rig_file"; \
#		fi; \
#	done
#	@echo "✓ All RIG files validated successfully"
#
#clean:
#	rm -rf $(DEST)
#	rm -rf tmp
#	rm -fr $(DOCDIR)/*
#	rm -fr $(PYMODEL)/*
