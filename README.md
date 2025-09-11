# NCATS Translator Data Ingests

This software repository forms an integral part of the Biomedical Data Translator Consortium, Performance Phase 3 efforts at biomedical knowledge integration, within the auspices of the **D**ata **ING**est and **O**perations ("DINGO") Working Group.
The repository aggregates and coordinates the development of knowledge-specific and shared library software used for Translator data ingests from primary (mostly external "third party") knowledge sources, into so-called Translator "Tier 1" knowledge graph(s). This software is primarily coded in Python.

A general discussion of the Translator Data Ingest architecture is provided [here](https://docs.google.com/presentation/d/11RaXtVAPX_i6MpD1XG2zQMwi81UxEXJuL5cu6FpcyHU).

## Technical Prerequisites

The project uses the [**uv**](https://docs.astral.sh/uv/) Python package and project manager You will need to [install **uv** onto your system](https://docs.astral.sh/uv/getting-started/installation/), along with a suitable Python (Release 3.12) interpreter.

The project initially (mid-June 2025) uses a conventional unix-style **make** file to execute tasks. For this reason, working within a command line interface terminal.  A MacOSX, Ubuntu or Windows WSL2 (with Ubuntu) is recommended. See the [Developers' README](DEVELOPERS_README.md) for tips on configuring your development environment.

## Ingest Processes and Artifacts
To ensure that ingests are performed rigorously, consistently, and reproducibly, we have defined an [Standard Operating Procedure (SOP)](https://github.com/NCATSTranslator/translator-ingests/blob/main/source-ingest-sop.md) to guide the source ingest process.  

The SOP is initially tailored to guide re-ingest of current sources to create a "functional replacement" of the Phase 2 system but it can be adapted to guide the ingest of new sources as well. 

Below are descriptions and links for the various artifacts prescribed by the SOP. 

1. **Ingest Assignment Tables**: First, add your proposed source here: Records owner and contributor assignments for each ingest. ([Sheet 1](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=506291936#gid=506291936)) ([Sheet 2](https://docs.google.com/spreadsheets/d/1nbhTsEb-FicBz1w69pnwCyyebq_2L8RNTLnIkGYp1co/edit?gid=1969427496#gid=1969427496)) 
2. **Source Ingest Tickets**: Second, document your source in a dedicated Git issue. Tracks contributor questions and discussions about the ingest. ([Tickets](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22)) ([Project Board](https://github.com/orgs/NCATSTranslator/projects/33/views/1?layout=board)) ([CTD Example](https://github.com/NCATSTranslator/Data-Ingest-Coordination-Working-Group/issues?q=state%3Aopen%20label%3A%22source%20ingest%22))
3. **Ingest Surveys**: Third, document your semantic understanding of the nodes and edges to be generated: here we describe current ingests of a source from Phase 2, to facilitate comparison, alignment and redesign. ([Directory](https://drive.google.com/drive/folders/1temEMKNvfMXKkC-6G4ssXG06JXYXY4gT)) ([CTD Example)](https://docs.google.com/spreadsheets/d/1R9z-vywupNrD_3ywuOt_sntcTrNlGmhiUWDXUdkPVpM/edit?gid=0#gid=0)
4. **Reference Ingest Guides (RIGs)**: Fourth, document scope, content, and modeling decisions for the ingest in the form of a Reference Ingest Guide. Current best practices now require the creation of a rig.yaml file in a dedicated repository ([RIG development repository](https://github.com/biolink/resource-ingest-guide-schema)) ([Instructions](https://github.com/biolink/resource-ingest-guide-schema?tab=readme-ov-file#working-with-rigs)) and ([General Documentation](https://biolink.github.io/resource-ingest-guide-schema/))
5. **Ingest Code**: Fifth, write the parse scripts to implement the ingest task:
   - _**Write unit tests:**_ with mock (but realistic) data, to illustrate how input records for a specified source are transformed into knowledge graph nodes and edges.  See the ([Unit Ingest Tests Directory](https://github.com/NCATSTranslator/translator-ingests/blob/main/tests/unit/ingests)) for some examples. See also specifically the ([_ingest_template Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/tests/unit/_ingest_template/test_ingest_template.py)) highlighting the use of some generic utility code available to fast-track the development of such ingest unit tests.
   - _**Populate the ingest-specific download.yaml file:**_ that describes the input data of the knowledge source ([_ingest_template Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/download.yaml)).
   - _**Write the configuration file:**_ that describes the source and the transform to be applied. ([Directory](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests)) ([_ingest_template Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/_ingest_template.yaml))
   - _**Write the Python script:**_ used to execute the ingest task as described in a RIG and to pass the unit tests which were written. ([Directory](https://github.com/NCATSTranslator/translator-ingests/tree/main/src/translator_ingest/ingests)) ([_ingest_template Example](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/_ingest_template.py))
6. **KGX Files**: The final knowledge graphs and ingest metadata that is produced by ingest code. ([CTD Example]() - TO DO)
   - The aforementioned Ingest Code parsers are generally written to generate their knowledge graphs - nodes and edges - using a Biolink Model-constrained Pydantic model (the exception to this is a 'pass-through' KGX file processor which bypasses the Pydantic model). Use of the Pydantic model is recommended since it provides a standardized way to validate and transform input data. The Translator Ingest pipeline converts the resulting parser Koza KnowledgeGraph output objects into KGX node and edge (jsonl) file content (that is, the Ingest Code does not write the KGX files directly, nor need to worry about doing so).
   - That said, the KGX ingest metadata needs to be generated separately using the [Ingest Metadata schema](https://github.com/biolink/ingest-metadata) which has a Python implementation.


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
