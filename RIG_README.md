# Resource Ingest Guide Schema

A LinkML schema for describing Reference Ingest Guides (RIGs) - structured documents that capture the scope, rationale, and modeling approach for ingesting content from external sources into Biolink Model-compliant data repositories.

## Overview

This repository provides:

- **LinkML Schema**: Formal specification for Reference Ingest Guides in `src/resource_ingest_guide_schema/schema/`
- **Documentation Generator**: Automated conversion of RIG YAML files to human-readable markdown
- **Validation Tools**: Schema validation for RIG files using LinkML
- **Template System**: Standardized templates and creation tools for new RIGs
- **Example RIGs**: Real-world examples from CTD, DISEASES, and Clinical Trials KP

### What are Reference Ingest Guides (RIGs)?

RIGs are structured documents that describe:

- **Source Information**: Details about data sources (access, formats, licensing)
- **Ingest Information**: What content is included/excluded and filtering rationale
- **Target Information**: How data is modeled in the output knowledge graph
- **Provenance Information**: Contributors and related artifacts

RIGs help ensure reproducible, well-documented data ingestion processes for biomedical knowledge graphs.

## Website

[https://biolink.github.io/resource-ingest-guide-schema](https://biolink.github.io/resource-ingest-guide-schema)

## Repository Structure

```
├── src/
│   ├── resource_ingest_guide_schema/
│   │   └── schema/                    # LinkML schema definition
│   ├── docs/
│   │   ├── files/                     # Static documentation files
│   │   ├── rigs/                      # Example RIG YAML files
│   │   └── doc-templates/             # Jinja2 templates for docs
│   └── scripts/                       # Python utilities for RIG processing
├── docs/                              # Generated documentation
├── tests/                             # Test suite
└── project/                           # Generated LinkML artifacts
```



## Developer Documentation

### Prerequisites

This project uses [uv](https://docs.astral.sh/uv/) for dependency management. Install it with:

```bash
# On macOS and Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or with pip
pip install uv
```

### Getting Started

Note that the following commands assume you are in the project root directory, and the equivalent **`just`** commands may be substituted for several **`make`** targets (namely `just test` instead of `make test`)

1. **Install dependencies:**
   ```bash
   uv sync --extra dev
   ```

2. **Run tests:**
   ```bash
   make test  # or just test
   ```

3. **Generate documentation:**
   ```bash
   make gendoc
   ```

### Working with RIGs

#### Creating a New RIG

```bash
# Create a new RIG from the template
make new-rig INFORES=infores:example NAME="Example Data Source"

# This creates src/docs/rigs/mydatasource_rig.yaml
# Edit the file to fill in your specific information
```

or using the equivalent **`just`** command:

```bash
just INFORES=infores:example NAME="Example Data Source" new-rig 
```

Note that for the **`just`** command, the script variables must precede the just recipe ("target") name on the command line (reverse of the make command).

#### Validating RIGs

```bash
# Validate all RIG files against the schema
make validate-rigs  
```

or

```bash
just validate-rigs
```
To validate a specific RIG:

```bash
uv run linkml-validate --schema src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml src/docs/rigs/my_rig.yaml
```

#### Building Documentation

```bash
# Generate all documentation including RIG index and markdown versions
make gendoc

# Test documentation locally
make testdoc  # Builds docs and starts local server
```

### Development Workflow

#### 1. Schema Development

The LinkML schema is defined in `src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml`. After making changes:

```bash
# Regenerate Python datamodel and other artifacts
make gen-project

# Test the schema
make test-schema

# Lint the schema
make lint
```

#### 2. Script Development

Python utilities are in `src/scripts/`:
- `create_rig.py`: Generate new RIG from template
- `rig_to_markdown.py`: Convert RIG YAML to Markdown
- `generate_rig_index.py`: Create RIG index table

To test script changes:

```bash
# Run scripts directly
uv run python src/scripts/create_rig.py --help
uv run python src/scripts/rig_to_markdown.py --input-dir src/docs/rigs --output-dir docs
```

#### 3. Documentation Development

Templates are in `src/docs/doc-templates/` and static files in `src/docs/files/`:

```bash
# Regenerate docs after template changes
make gendoc

# View changes locally
make serve  # or make testdoc
```

### Available Commands

Note: some **`make`** targets (like **`new-rig`** and **`validate-rigs`**) have **`just`** command equivalents (remember instead to put the just recipe target name _after_ any command line arguments)

| Command | Description |
|---------|-------------|
| `make help` | Show all available commands |
| `make install` | Install dependencies with uv |
| `make test` | Run full test suite |
| `make test-schema` | Test schema generation |
| `make test-python` | Run Python tests |
| `make lint` | Lint the LinkML schema |
| `make gen-project` | Generate LinkML artifacts (Python, JSON Schema, etc.) |
| `make gendoc` | Generate documentation including RIG processing |
| `make serve` | Start local documentation server |
| `make testdoc` | Build docs and start server |
| `make new-rig` | Create new RIG (requires INFORES and NAME) |
| `make validate-rigs` | Validate all RIG files |
| `make clean` | Clean generated files |
| `make deploy` | Deploy documentation |

### Project Structure Details

#### Key Directories

- **`src/resource_ingest_guide_schema/schema/`**: LinkML schema definition
- **`src/docs/rigs/`**: Example RIG YAML files (CTD, DISEASES, Clinical Trials KP)
- **`src/docs/files/`**: Static documentation files copied to output
- **`src/docs/doc-templates/`**: Jinja2 templates for documentation generation
- **`src/scripts/`**: Python utilities for RIG creation and processing
- **`docs/`**: Generated documentation output (do not edit directly)
- **`project/`**: Generated LinkML artifacts (Python models, JSON Schema, etc.)

#### Generated Artifacts

The `make gen-project` command generates:
- **Python datamodel**: `src/resource_ingest_guide_schema/datamodel/`
- **JSON Schema**: `project/jsonschema/`
- **OWL ontology**: `project/owl/`
- **GraphQL schema**: `project/graphql/`
- **SQL DDL**: `project/sqlschema/`
- **And more**: See `project/` directory

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make changes following the existing patterns
4. Ensure tests pass: `make test`
5. Update documentation if needed: `make gendoc`
6. Submit a pull request

#### Adding New RIG Examples

1. Create YAML file in `src/docs/rigs/`
2. Follow the schema structure (see existing examples)
3. Validate: `make validate-rigs`
4. Regenerate docs: `make gendoc`
5. The RIG will automatically appear in the documentation index

#### Schema Changes

1. Modify `src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml`
2. Regenerate artifacts: `make gen-project`
3. Update any affected RIG files
4. Test: `make test`
5. Update documentation as needed
