# HPOA Ingest Thing to Do

- Need to fix **`modes_of_inheritance = read_ontology_to_exclusion_terms(ontology_obo_file=HPO_FILE_PATH)`** in disease_to_phenotype.py to be able to use mock data in unit tests
- convert disease_to_phenotype I = inheritance parse from edge assertion into (annotated) disease node assertion
- filter out gene_to_phenotype_transform to only transform MENDELIAN traits (problem: MENDELIAN is declared in phenotype.hpoa while gene_to_phenotype.txt used to generate the relevant Associations)