ROOTDIR = $(shell pwd)
RUN = uv run


new-rig:
ifndef INFORES
	$(error INFORES is required. Usage: make new-rig INFORES=infores:example NAME="Example RIG")
endif
ifndef NAME
	$(error NAME is required. Usage: make new-rig INFORES=infores:example NAME="Example RIG")
endif
	$(RUN) python $(SRC)/scripts/create_rig.py --infores "$(INFORES)" --name "$(NAME)"

# Validate all RIG files against the schema
validate-rigs:
	@echo "Validating RIG files against schema..."
	@for rig_file in $(SRC)/docs/rigs/*.yaml; do \
		if [ -f "$$rig_file" ]; then \
			echo "Validating $$rig_file"; \
			$(RUN) linkml-validate --schema $(SOURCE_SCHEMA_PATH) "$$rig_file"; \
		fi; \
	done
	@echo "✓ All RIG files validated successfully"
