# HPOA Ingest Things to Do

- Need to fix **`modes_of_inheritance = read_ontology_to_exclusion_terms(ontology_obo_file=HPO_FILE_PATH)`** in **disease_to_phenotype.py** to be able to use mock data in unit tests
- convert disease_to_phenotype **`aspect == I`** -> inheritance parse from edge assertion into (annotated) disease node assertion 
    - Challenge is what Biolink Model design to use?
- filter out **gene_to_phenotype_transform.py** to only transform MENDELIAN traits (problem: MENDELIAN is declared in _phenotype.hpoa_ while **gene_to_phenotype.txt** used to generate the relevant Associations)
- review strategy (with Kevin et.al.) for incorporating **gene_to_phenotype.py** publications into **gene_to_phenotype_transform.py** ingest
    - Is this ingest necessary (or will it rather be done later?)