# NCATS Translator Data Ingests

This software repository forms an integral part of the Biomedical Data Translator Consortium, Performance Phase 3 efforts at biomedical knowledge integration, within the auspices of the **D**ata **ING**est and **O**perations ("DINGO") Working Group.
The repository aggregates and coordinates the development of knowledge-specific and shared library software used for Translator data ingests from primary (mostly external "third party") knowledge sources, into so-called Translator "Tier 1" knowledge graph(s). This software is primarily coded in Python.

A general discussion of the Translator Data Ingest architecture is provided [here](https://docs.google.com/presentation/d/11RaXtVAPX_i6MpD1XG2zQMwi81UxEXJuL5cu6FpcyHU).

## Technical Prerequisites

The project uses the [**uv**](https://docs.astral.sh/uv/) Python package and project manager You will need to [install **uv** onto your system](https://docs.astral.sh/uv/getting-started/installation/), along with a suitable Python (Release 3.12) interpreter.

The project initially (mid-June 2025) uses a conventional unix-style **make** file to execute tasks. For this reason, working within a command line interface terminal.  A MacOSX, Ubuntu or Windows WSL2 (with Ubuntu) is recommended. See the [Developers' README](DEVELOPERS_README.md) for tips on configuring your development environment.

## Ingest Processes and Artifacts
To ensure that ingests are performed rigorously, consistently, and reproducibly, we have defined an [Standard Operating Procedure (SOP)](https://github.com/NCATSTranslator/translator-ingests/edit/main/source-ingest-sop.md) to guide the source ingest process.  

The SOP is initially tailored to guide re-ingest of current sources to create a "functional replacement" of the Phase 2 system - but it can be adapted to guide ingest of new sources as well. 

Below are descriptions and links for the various artifacts prescribed by the SOP. 

1. **Ingest Assignment Table**: Records owner and contributor assignments for each ingest. ([Link](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1969427496#gid=1969427496))
2. **Source Ingest Tickets**: Tracks contributor questions and discussions about the ingest. ([Tickets](https://github.com/orgs/NCATSTranslator/projects/33/views/1?layout=board)) ([Project Board](https://github.com/orgs/NCATSTranslator/projects/33/views/1?layout=board)) ([CTD Example](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22))
3. **Ingest Surveys**: Describe current ingests of a source from Phase 2 to facilaitate comparison and alignment. ([Directory](https://drive.google.com/drive/folders/1temEMKNvfMXKkC-6G4ssXG06JXYXY4gT)) ([CTD Example)](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
4. **Reference Ingest Guides (RIGs)**: Document scope, content, and modeling decisions for an ingest. ([Template](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/rig-template.md)) ([Instructions](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/rig-instructions.md)) ([CTD Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/rig.md))
5. **Ingst Code**: Python code used to execute an ingest as described in a RIG. ([Directory](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests)) ([CTD Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/ctd.py))
6. **KGX Files**: The final knowledge graphs and ingest metadata that is produced by ingest code. ([CTD Example]() - TO DO)


## Initial Minimal Viable Product: A CTD Example

Here, we apply a [koza](https://koza.monarchinitiative.org/) transform of data from the [Comparative Toxicology Database](https://ctdbase.org/), writing the knowledge graph output out to jsonlines (jsonl) files. The project is built and executed using a conventional (unix-like) Makefile:

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

- CTD download source data: [download.yaml](./src/translator_ingest/ingests/ctd/download.yaml)
- CTD transform configuration file: [ctd.yaml](./src/translator_ingest/ingests/ctd/ctd.yaml)
- CTD transform code: [ctd.py](./src/translator_ingest/ingests/ctd/ctd.py)
- [CTD transform documentation](./src/translator_ingest/ingests/ctd/README.md)
- Unit tests: [test_ctd.py](./tests/unit/ctd/test_ctd.py)
