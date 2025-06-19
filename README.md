# NCATS Translator Data Ingests

## Prerequisites

The project uses the [uv project (dependency) management tool](https://docs.astral.sh/uv/) which should first be installed on your system and used to install project dependencies into a virtual environment. 

## Initial MVP: CTD

Here, we apply a koza transform of CTD data, writing knowledge graph output to a jsonlines (jsonl) file. The project is built and executed using a conventional Makefile:

    │ Usage:
    │     make <target>
    │
    │ Targets:
    │     help                Print this help message
    │ 
    │     all                 Install everything and test
    │     fresh               Clean and install everything
    │     clean               Clean up build artifacts
    │     clobber             Clean up generated files
    │
    │     install             install python requirements
    │     download            Download data
    │     run                 Run the transform
    │
    │     test                Run all tests
    │
    │     lint                Lint all code
    │     format              Format all code  running the following steps.

The task involves the following steps/components:

- Download source data: [download.yaml](./download.yaml)
- CTD transform configuration file: [ctd.yaml](./src/translator_ingest/ingests/ctd/ctd.yaml)
- CTD transform code: [ctd.py](./src/translator_ingest/ingests/ctd/ctd.py)
- [CTD transform documentation](./src/translator_ingest/ingests/ctd/README.md)
- Unit tests: [test_ctd.py](./tests/unit/ctd/test_ctd.py)