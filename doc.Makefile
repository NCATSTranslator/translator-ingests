# Documentation Makefile for MkDocs

.PHONY: help docs-install docs-clean docs-build docs-serve docs-deploy

docs-install:  ## Install documentation dependencies
	uv sync --group dev

docs-clean:  ## Clean documentation build artifacts
	rm -rf site/
	rm -rf docs/index.md
	rm -rf docs/src/
	rm -rf docs/rigs/

docs-setup:  ## Setup documentation structure
	@echo "Setting up documentation structure..."
	mkdir -p docs/src/docs docs/rigs
	cp README.md docs/index.md
	if [ -d "src/docs" ]; then cp -r src/docs/* docs/src/docs/; fi
	@echo "Copying RIG YAML files..."
	find src/translator_ingest/ingests -name "*rig*.yaml" -exec cp {} docs/rigs/ \;
	@echo "Converting RIG YAML files to markdown..."
	uv run python src/docs/scripts/rig_to_markdown.py --input-dir docs/rigs --output-dir docs/rigs
	@echo "Generating RIG table..."
	uv run python src/docs/scripts/generate_rig_table.py

docs-build: docs-clean docs-setup  ## Build the documentation site
	@echo "Building MkDocs site..."
	uv run mkdocs build --clean

docs-serve: docs-clean docs-setup  ## Build and serve documentation locally on port 8001
	@echo "Starting MkDocs server on http://127.0.0.1:8001"
	uv run mkdocs serve --dev-addr 127.0.0.1:8001

docs-deploy: docs-build  ## Deploy documentation to GitHub Pages
	@echo "Deploying to GitHub Pages..."
	uv run mkdocs gh-deploy --force
