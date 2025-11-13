# NCATS Translator Data Ingests

This software repository forms an integral part of the Biomedical Data Translator Consortium, Performance Phase 3 efforts at biomedical knowledge integration, within the auspices of the **D**ata **ING**est and **O**perations ("DINGO") Working Group.
The repository aggregates and coordinates the development of knowledge-specific and shared library software used for Translator data ingests from primary (mostly external "third party") knowledge sources, into so-called Translator "Tier 1" knowledge graph(s). This software is primarily coded in Python.

A general discussion of the Translator Data Ingest architecture is provided [here](https://docs.google.com/presentation/d/11RaXtVAPX_i6MpD1XG2zQMwi81UxEXJuL5cu6FpcyHU).

## Technical Prerequisites

The project uses the [**uv**](https://docs.astral.sh/uv/) Python package and project manager You will need to [install **uv** onto your system](https://docs.astral.sh/uv/getting-started/installation/), along with a suitable Python (Release 3.12) interpreter.

The project initially (mid-June 2025) uses a conventional unix-style **make** file to execute tasks. For this reason, working within a command line interface terminal.  A MacOSX, Ubuntu or Windows WSL2 (with Ubuntu) is recommended. See the [Developers' README](DEVELOPERS_README.md) for tips on configuring your development environment.

## Ingest Processes and Artifacts
To ensure that ingests are performed rigorously, consistently, and reproducibly, we have defined an [Standard Operating Procedure (SOP)](https://github.com/NCATSTranslator/translator-ingests/blob/main/source-ingest-sop.md) to guide the source ingest process.  

The SOP is initially tailored to guide re-ingest of current sources to create a "functional replacement" of the Phase 2 knowledge provider sources, but it can be adapted to guide the ingest tasks of new sources as well. 

Follow the steps and use / generate the artifacts described below, to perform a source ingest according to standard operating procedure.

1. **Ingest Assignment and Tracking** **_(required)_**: Record owner/contributor assignments and track status for each ingest. ([ingest list](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=506291936#gid=506291936)) 
2. **Ingest Surveys** **_(as needed)_**: Describe past ingests of a source to facilitate comparison and alignment (useful when there are multiple prior ingests). ([directory](https://drive.google.com/drive/folders/1temEMKNvfMXKkC-6G4ssXG06JXYXY4gT)) ([ctd example)](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
3. **Resource Ingest Guides (RIGs)** **_(required)_**: Document scope, content, and modeling decisions for an ingest task, in a computable yaml file format. ([yaml schema](https://github.com/biolink/resource-ingest-guide-schema/blob/main/src/resource_ingest_guide_schema/schema/resource_ingest_guide_schema.yaml)) ([yaml template](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/docs/rig_template.yaml)) ([yaml example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/ctd_rig.yaml)) ([derived markdown example](https://ncatstranslator.github.io/translator-ingests/rigs/ctd_rig/)) ([full rig catalog](https://ncatstranslator.github.io/translator-ingests/src/docs/rig_index/)). For KGX passthrough ingests, if a meta_knowledge_graph.json file is available, then RIG's can be partially populated, after creation, with node and edge target_info data using the [mkg_to_rig.py](src/docs/scripts/mkg_to_rig.py) script.  If global additions or deletions of RIG property tag-values are needed, then the [annotate_rig.py](src/docs/scripts/annotate_rig.py) script may be used.
4. **Source Ingest Tickets** **_(as needed)_**: If content or modeling questions arise, create a `source-ingest` ticket in the DINGO repo ([ingest issues](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=label%3A%22source%20ingest%22))
5. **Ingest Code and Tests** **_(required)_**: Author ingest code / artifacts following RIG spec, along with unit tests, using shared python code base. ([ingest code](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/)) ([code template](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/_ingest_template.yaml)) ([code example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/ctd.py)) ([unit tests](https://github.com/NCATSTranslator/translator-ingests/blob/main/tests/unit/ingests)) ([unit test template](https://github.com/NCATSTranslator/translator-ingests/blob/main/tests/unit/_ingest_template/test_ingest_template.py))
6. **KGX Files** **_(required)_**: Execute ingest code and normalization services to generate normalized knowledge graphs and ingest metadata artifacts. ([ctd example]() - TO DO)
7. **KGX Summary Reports** **_(under development)_**: Automated scripts generate reports that summarize the content of KGX ingest files, to facilitate manual QA/debugging, and provide documentation of KG content and modeling. ([ctd example]() - TO DO)

## Additional Notes
- Populate the ingest-specific download.yaml file that describes the input data of the knowledge source ([ingest template example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/download.yaml)).
- Write the configuration file that describes the source and the transform to be applied. ([directory](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests)) ([ingest template example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/_ingest_template.yaml))
- Write the Python script used to execute the ingest task as described in a RIG and to pass the unit tests which were written. ([directory](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests)) ([ingest template example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/_ingest_template.py))
- Write unit tests with mock (but realistic) data, to illustrate how input records for a specified source are transformed into knowledge graph nodes and edges.  See the ([unit ingest tests directory](https://github.com/NCATSTranslator/translator-ingests/blob/main/tests/unit/ingests)) for some examples, and the ([ingest template example](https://github.com/NCATSTranslator/translator-ingests/blob/main/tests/unit/_ingest_template/test_ingest_template.py)) highlighting the use of some generic utility code available to fast-track the development of such ingest unit tests.
- Ingest Code parsers are generally written to generate their knowledge graphs - nodes and edges - using a Biolink Model-constrained Pydantic model (the exception to this is a 'pass-through' KGX file processor which bypasses the Pydantic model).
- Use of the Pydantic model is recommended since it provides a standardized way to validate and transform input data.
- The Translator Ingest pipeline converts the resulting parser Koza KnowledgeGraph output objects into KGX node and edge (jsonl) file content (that is, the Ingest Code does not write the KGX files directly, nor need to worry about doing so).
- That said, the KGX ingest metadata needs to be generated separately using the [ingest metadata schema](https://github.com/biolink/ingest-metadata) which has a Python implementation.

## Initial Minimal Viable Product: A CTD Example

Here, we apply a [koza](https://koza.monarchinitiative.org/) transform of data from the [Comparative Toxicology Database](https://ctdbase.org/), writing the knowledge graph output out to jsonlines (jsonl) files. The project is built and executed using targets in a conventional (unix-like) **make** command, operating on a **Makefile** in the repository.

Alternately, there is a **justfile** upon which the cross-platform **just** command tool may be used on functionally equivalent targets. [**Install just**](https://just.systems/man/en/introduction.html) then type **`just help`** for usage.

    │ Usage:
    │     make <target>  # or just <target>
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

- CTD download source data: [download.yaml](./src/translator_ingest/ingests/ctd/download.yaml)
- CTD transform configuration file: [ctd.yaml](./src/translator_ingest/ingests/ctd/ctd.yaml)
- CTD transform code: [ctd.py](./src/translator_ingest/ingests/ctd/ctd.py)
- [CTD transform documentation](./src/translator_ingest/ingests/ctd/README.md)
- Unit tests: [test_ctd.py](./tests/unit/ctd/test_ctd.py)
