"""
translator_ingest.ingests.hpoa package.

Stub implementation of a wrapper for the 
Human Phenotype Ontology Annotation (HPOA) as a DataSource,
just to allow for temporary mock implementation of
expected Translator Ingest pipeline features, like version capture.
Should generally be replaced by a shared code implementation
(currently a design work-in-progress within DINGO)
"""
from os.path import join, abspath

from translator_ingest import PRIMARY_DATA_PATH

PHENOTYPE_HPOA_FILE: str = abspath(join(PRIMARY_DATA_PATH, "hpoa", "phenotype.hpoa"))

def get_version(file_path=PHENOTYPE_HPOA_FILE) -> str:
    with open(file_path, "r") as phf:
        line = phf.readline()
        while not line.startswith("#version:"):
            line = phf.readline()
        return line.split(":")[1].strip()
