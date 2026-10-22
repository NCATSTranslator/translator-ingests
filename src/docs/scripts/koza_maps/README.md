# Koza Map generation scripts

This folder segregates a set of scripts used to generate specific Koza mapping files. 

The current set are specifically designed for the HMDB ingests but should other ingests require them, their 
generation and placement could be suitably modified, since the files themselves are agnostic about HMDB per say.

Generally, if the source ontology are stable releases, the scripts only need to be run (offline from the
Translator Ingest pipeline) once per ontology release, then the corresponding mapping files committed to the repository.

These scripts both depend on OakLib which is a somewhat heavyweight thus is only part of the 'uv' **dependency-group** 
named **koza_map** and should be implicitly _uv sync'd_ into the local dev environment before these scripts are run.

## HMDB 'cellular_locations' names to Gene Ontology Cellular Compartment terms

The [go_cellular_map.py](./go_cellular_map.py) script generates the contents of the 
[go_cellular_lookup.csv](../../../translator_ingest/ingests/hmdb/maps/go_cellular_lookup.csv) file.

## HMDB 'biospecimen_locations' or 'tissue_locations' names to UBERON terms

The [go_cellular_map.py](./uberon_map.py) script generates the contents of the 
[uberon_lookup.csv](../../../translator_ingest/ingests/hmdb/maps/uberon_lookup.csv) file.
