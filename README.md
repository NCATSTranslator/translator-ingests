# NCATS Translator Data Ingests

This software repository forms an integral part of the Biomedical Data Translator Consortium, Performance Phase 3 efforts at biomedical knowledge integration, within the auspices of the **D**ata **ING**est and **O**perations ("DINGO") Working Group.
The repository aggregates and coordinates the development of knowledge-specific and shared library software used for Translator data ingests from primary (mostly external "third party") knowledge sources, into so-called [Translator "Tier 1" knowledge graph(s)](https://github.com/NCATSTranslator/Data-Access-Working-Group/blob/main/data-tiers.md#tier-1). This software is primarily coded in Python.

## Data Model and Dataset Design

The data ingest pipeline proposes to manage all of its intermediate and final knowledge graph data, as instances of the [Knowledge Graph eXchange ("KGX")](https://github.com/biolink/kgx) specification.  This kind of dataset has nodes and edges serialized into corresponding JSON lines formatted text files. The updated KGX specification will include a file of metadata (file format T.B.A.). The semantics of all data in the KGX dataset will be fully constrained by current releases of the [Biolink Model](https://biolink.github.io/biolink-model/).

## General Flow of the Ingest Process

A general discussion of the Translator Data Ingest architecture is provided [here](https://docs.google.com/presentation/d/1MNkosy-AmHVIXvfc5tNxpCaqnUskypIO6gwQle0H1_M). 

The knowledge graph data ingest process may be conceptually described as follows:

* Download primary knowledge source (PKS) Files 
  * Substep of deciding which PKS files to get (when there are versions)
* Parse downloads into a KGX dataset
  * This should (not must) be a complete capture of the knowledge graph(s) in the original PKS
  * This resolves node and edge data in the PKS, but the resulting KGX dataset will be incompletely populated, hence, not converted to fully expressive Biolink semantics, and without normalized node identifiers.
* Serialize to storage
* Filter 
  * Apply quality or other metrics 
  * Per-source filtering, depending on the details
* Enrich the Biolink Model representation of the original PKS defined knowledge graph
  * Lots of edge types and property mapping
* Serialize to storage
* Normalize (mostly node identifiers, but perhaps, other slot values)
* Serialize to storage
* Annotate nodes
* Serialize to storage
* Validate Knowledge Graph
  * Ingest level quality assurance of the output data, including global referential integrity
* Serialize to storage
  * Final KGX dataset is loaded into the Translator Tier 1 database

## Project Prerequisites

The project uses the [**uv**](https://docs.astral.sh/uv/) Python package and project manager You will need to [install **uv** onto your system](https://docs.astral.sh/uv/getting-started/installation/), along with a suitable Python (Release 3.12) interpreter.

The project initially (mid-June 2025) uses a conventional unix-style **make** file to execute tasks. For this reason, working within a command line interface terminal.  A MacOSX, Ubuntu or Windows WSL2 (with Ubuntu) is recommended. See the [Developers' README](DEVELOPERS_README.md) for tips on configuring your development environment.

## June 2025 Initial Prototype: CTD

Here, Kevin Schaper applied a [koza](https://koza.monarchinitiative.org/) transform of data from the [Comparative Toxicology Database](https://ctdbase.org/), writing the knowledge graph output out to jsonlines (jsonl) files. The project is built and executed using a conventional (unix-like) Makefile:

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
