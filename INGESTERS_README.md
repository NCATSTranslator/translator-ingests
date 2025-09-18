# Writing and running an ingest for this repo (common ingest pipeline)

## Links and Instructions

Basic development environment setup:

1. Install [UV](https://docs.astral.sh/uv/getting-started/installation/) - this is both a virtual environment manager, a Python version manager, and your all-around best friend for dev environments
    - `curl -LsSf https://astral.sh/uv/install.sh | sh` if you want to install it system-wide, or via uvx if you're familiar with pipx.
2. `git clone https://github.com/NCATSTranslator/translator-ingests`
3. `uv sync`  - this creates a virtual environment isolated from your system environment, installing all necessary Python dependencies. To access this environment directly, remember the `uv run` prefix for your commands.  Our makefile targets all call `uv run` as part of their call.
4. `make test` (or `just test`\*) -  all should pass for you if your environment is set up. 
5. for just one source, `make run SOURCES="go_cam"` (or `just run`...\*)
    - To run multiple sources, `make run SOURCES="go_cam ctd goa"`. **This will take a while (~15 min) to run to completion.** This will create several KGX bundles in `data/` directory at the top level of your project so you can see how they look in JSONlines format. 
6. `make validate` (or `just validate`\*): runs the validator on all KGX files in your local `data/` directory.  Currently, validation is minimal, but we envision a plug-in architecture that allows us to customize it per source.

\* On `just`: The `just` command (installation instructions found [here](https://just.systems/man/en/introduction.html)) is a cross-platform alternative to `make` that runs well on Windows (as well as Unix-based systems). `just` can be used instead of `make` for all of the commands shown here (if it doesn’t work, let Sierra know - we’ll fix the justfile).

[Commands](https://github.com/NCATSTranslator/translator-ingests/blob/main/DEVELOPERS_README.md#running-and-validating-ingests) to run and validate individual sources
- `make run SOURCES=""` - want it to run successfully, review output
- `make validate SOURCES=""` - want it to run successfully
- Use `uv run` prefix for commands to use its environ (`make` commands actually do this)
- Run `uv sync` often to update environ after pipeline updates
- And update (git) dependencies regularly too: `uv add --upgrade biolink-model` and `uv add --upgrade koza`

## Writing code based on template

[Template](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests/_ingest_template)

**Documentation on declarative yaml**: [docs](https://github.com/monarch-initiative/koza/blob/translator-ingests/docs/Ingests/koza_config.md), [code](https://github.com/monarch-initiative/koza/tree/translator-ingests/src/koza/model)

- `transform` section
  - `code` field is optional, only needed if py file has diff name than yaml
- `filters` go in `reader`/`readers` section

### References
- [ctd](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests/ctd)
- [go_cam](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests/go_cam)
- [goa](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests/goa)
- [hpoa-reference-ingest](https://github.com/NCATSTranslator/translator-ingests/pull/25)
- [ebi-gene2phenotype](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests/ebi-gene2phenotype)

### Template instructions
- Replace function bodies. Delete all template comments and unused functions
- Tags are to divide ingest into multiple sections: useful for multiple input files, transforms. These should be specified in the yaml file (`readers` section's keys). Then they're used in parameters of corresponding koza decorators (`tag="tag_id"`)
  - examples in template: `"ingest_by_record"`, `"ingest_all"`, `"ingest_all_streaming"`
- `koza.state` is a dictionary that can be used for arbitrary data storage, **persists across 1 `reader`/transform** (not true global) 
- **Data will have empty strings for empty values "".** [Pandas doesn't recognize these as NA](https://pandas.pydata.org/docs/reference/api/pandas.Series.isna.html#pandas.Series.isna), [will need adjustments to recognize](https://stackoverflow.com/questions/29314033/drop-rows-containing-empty-cells-from-a-pandas-dataframe) - [replace over whole df](https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.replace.html#pandas.DataFrame.replace) will work. 
- `@koza.prepare_data()`: optional. Currently runs first!!!
- `@koza.on_data_begin()`: optional, but good for setting something up at the very beginning
- `@koza.on_data_end()` optional, but good for reporting stuff at the very end
- `@koza.transform()` OR `@koza.transform_record()`: MUST have one, cannot have both. 
  - `transform_record` runs over 1 row at a time, creates KnowledgeGraph that can be multiple edges.
  - `transform` runs over all rows together, creates list of KnowledgeGraph (1 per row). Can be returned in batches / generator for streaming
  - Should have core "data transformation" logic
  - Should generate/return KnowledgeGraph objects
    - Nodes
    - Edges (Association objects)
- NodeNorming step should overwrite categories, and then validation should check that Associations/predicates are still valid

## Testing Code

You are **strongly encouraged** to write unit tests for your resource.

- Unit tests should be **small, simple, and fast**.
- **Do not** download files or fetch data from external sources (e.g., API calls).
- More complex testing approaches are still being discussed, but for now the focus is:
*"Can the code run on example data and produce the expected output?"*

This kind of testing helps catch changes or bugs introduced in **biolink-model** or in the pipeline itself.

### Running tests:

- `make test` (or `just test`): run all tests (using pytest under the hood). **you should run this before every PR**
  - there are some general standards tests for `transform_record` methods
- If you choose to use `pytest` directly and not the `make` target, be sure you are in the right virtual environment (e.g. `uv run pytest`).  In general, `make test` is easier and does this wrapping for you. 
  * Run specific tests with: `uv run pytest <path-to-your-test>`

*(Note: this is different from `make validate`, which performs data-level validation.)*

## Pydantic, biolink-model, KGX

Koza uses the [Biolink Pydantic models](https://github.com/biolink/biolink-model/blob/master/src/biolink_model/datamodel/pydanticmodel_v2.py) automatically generated by LinkML in Biolink.

The Pydantic serialization of Biolink is published to [PyPI](https://pypi.org/project/biolink-model/) and can be imported like any other Python package. It is also imported into **translator-ingests**. You can review the details at the link above, or simply install the package, import it, and use your IDE to explore the available classes.

At a high level:
- **Validation**: The Pydantic serialization of Biolink handles the first level of node/edge object validation.
- **Pydantic background**: For more information on how Pydantic classes work, see the [Pydantic models documentation](https://docs.pydantic.dev/latest/concepts/models/).
- **Serialization generation**: See the [LinkML Pydantic serialization docs](https://linkml.io/linkml/generators/pydantic.html) for how these classes are generated.

As you create nodes and edges, they will be validated against the Biolink Pydantic serialization using the imported classes. This same structure is the foundation for **KGX JSONlines output**, which is the JSONlines serialization of Biolink-conformant data.

Koza automatically produces KGX JSONlines serialization. You can look at existing ingests for examples. For more details on the KGX format itself, see the [KGX documentation](https://biolink.github.io/kgx/).

## Validating Output KGX

Run `make validate` to check the KGX JSONlines output.

This command runs all available ingests and performs a limited set of validation checks, including:

- Ensuring that nodes listed in the **nodes file** are also referenced in the **edges file**.
- Ensuring that edge subject/object identifiers are present in the **nodes file**.

## CI/CD via GH actions

This repository uses GitHub Actions to automate testing and quality checks.  Code must pass all checks to be merged (in addition to a code review).

**Build and Test Workflow**

- **Triggers on:**
  - Pushes to the `main` branch (e.g. a PR is merged)
  - Pull requests (`opened`, `reopened`, `synchronized`, `ready_for_review`)
- **What it does:**
  - Runs linting and setup checks on Python 3.12 (ruff, black, etc.)
  - Runs the test suite across multiple Python versions (3.9 → 3.13)
  - Executes `make test` to validate code with `pytest`

**Codespell Workflow**

- **Triggers on:**
  - Pushes to the `main` branch (e.g. a PR is merged)
  - Pull requests (`opened`, `reopened`, `synchronized`, `ready_for_review`)
- **What it does:**
  - Runs [Codespell](https://github.com/codespell-project/codespell) to check for common spelling errors in the codebase
  - Skips files under `./data/*`
  - Uses `.codespellignore` for custom exceptions

## FYI CX's process

- Writing:
  - Notebooks: explore data, figure out what I want to do in an environ that's easy to debug.
  - Copy [Template](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests/_ingest_template) files over to my resource's folder, rename
Download.yaml
  - (Optional) Copy over how the notebook constructs edge, adjust if needed for pipeline
    - Color code what is from record, vs pull from biolink pydantic, vs hard code
  - (Optional) Go through notebook - record actions are needed, decide where they could go
  - Yaml
  - Py 
- After writing:
  - `make test`: Run all - including general tests to see if ingest works. Try running some of the dependencies too (ruff, black, codespell?) 
  - `make run SOURCES=""`: some debugging at this point
    - Include changing biolink-model in branch, pushing, manually triggering "regenerate artifacts"
    - And adjusting pyproject to point to the branch
  - `make validate SOURCES=""` - want it to run successfully
  - After run successfully, review output 
    - Line counts - vs notebook output. See notes on individ ingests below
    - Counts of ID prefixes - one in every row?
    - Look for null, None, "", uniprot (string we removed) - want none
    - Example of each kind of edge / variation - manual review in json viewer: does it have all fields, correct? 
- Write tests for resource, run them. Only do once previous steps are done. 
