# Ingest Template 

This directory provides a template that can be used to implement a new knowledge source ingest. It is not intended to run or work as is.

Make a copy of this directory and change the name of the directory, ingest-template.py, and ingest-template.yaml to the infores identifier corresponding to the new knowledge source. 

Populate the files with implementation details specific to that knowledge source. Avoid implementing major functionalities that seem like they should be shared across multiple ingests; instead consult translator_ingest maintainers with questions or suggestions.

## Reference Ingest Guide (RIG)

RIGs document the scope, rationale, and modeling approach for ingesting content from a single source.

A **new RIG** can be created by copying and overwriting the markdown content of the [rig-template](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/_ingest_template/rig-template.md).

**Instructions** for populating a RIG can be found [here](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/rig-instructions.md).

An **informative example** of a completed RIG for the CTD ingest can be found [here](https://github.com/NCATSTranslator/translator-ingests/blob/main/src/translator_ingest/ingests/ctd/rig.md).

