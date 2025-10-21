ROOTDIR = $(shell pwd)
RUN = uv run


new-rig:
ifndef INFORES
	$(error INFORES is required. Usage: make new-rig INFORES=infores:example NAME="Example RIG")
endif
ifndef NAME
	$(error NAME is required. Usage: make new-rig INFORES=infores:example NAME="Example RIG")
endif
	$(RUN) python src/docs/scripts/create_rig.py --infores "$(INFORES)" --name "$(NAME)"

# Validate all RIG files against the schema
validate-rigs:
	@echo "Validating RIG files against schema..."
	@for rig_file in src/translator_ingest/ingests/*/*_rig.yaml; do \
		if [ -f "$$rig_file" ]; then \
			echo "Validating $$rig_file"; \
			$(RUN) python -m resource_ingest_guide_schema.cli validate "$$rig_file"; \
		fi; \
	done
	@echo "✓ All RIG files validated successfully"

# List all RIG files
list-rigs:
	@echo "RIG files in the ingests directory:"
	@find src/translator_ingest/ingests -name "*_rig.yaml" -type f | sort | while read rig; do \
		echo "  - $$rig"; \
	done

# Usage: make mkg-to-rig --INGEST <ingest_dir_name>
# Populate target_info.(node|edge)_type_info using the contents of a
# TRAPI-style Meta Knowledge Graph JSON file to RIGmkg-to-rig:
	$(RUN) python src/docs/scripts/mkg_to_rig.py --ingest "$(INGEST)"
