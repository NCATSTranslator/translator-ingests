# Translator Ingest Platform

This folder contains both knowledge-source-specific ingest specifications (declarative metadata and code) and shared code libraries used by those specifications.

## Some Special Ingest Topics of Interest

### KGX "Passthrough" Ingest Parsers

The special case of writing a KGX ingest parser is described in the [KGX Passthrough Ingest Parser README](WRITING_A_KGX_PASSTHROUGH_PARSER.md).

### Parameterized data release specification within ingest download.yaml files

If you need to specify a versioned data access endpoint in your ingest-specific **download.yaml** file, please see [here](translator_ingest/util/DOWNLOAD_VERSION_SUBSTITUTION.md) for details.


