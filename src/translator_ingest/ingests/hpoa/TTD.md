# HPOA Ingest Things to Do

- Need to fix **`modes_of_inheritance = read_ontology_to_exclusion_terms(ontology_obo_file=HPO_FILE_PATH)`** in **disease_to_phenotype.py** to be able to use mock data in unit tests
- convert disease_to_phenotype **`aspect == I`** -> inheritance parse from edge assertion into (annotated) disease node assertion (**_DESIGN DECISION TO MAKE THIS HPOA INGEST MODELLING CHANGE IS NOT YET FINALIZED AS OF MONDAY 11 AUG 2025_**)
    - Challenge is what Biolink Model design to use? Maybe see: https://github.com/biolink/biolink-model/issues/1064
