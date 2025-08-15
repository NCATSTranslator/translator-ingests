"""
translator_ingest.ingests.hpoa package.

Shared utilities for HPOA ingest:

    get_version() - get version from phenotype.hpoa file
"""
from os.path import join, abspath

from translator_ingest.util.github import GitHubReleases

from translator_ingest import PRIMARY_DATA_PATH

PHENOTYPE_HPOA_FILE: str = abspath(join(PRIMARY_DATA_PATH, "hpoa", "phenotype.hpoa"))

def get_version(file_path=PHENOTYPE_HPOA_FILE) -> str:
    with open(file_path, "r") as phf:
        line = phf.readline()
        while not line.startswith("#version:"):
            line = phf.readline()
        return line.split(":")[1].strip()

def get_latest_version() -> str:
    ghr = GitHubReleases(git_org="obophenotype", git_repo="human-phenotype-ontology")
    return ghr.get_latest_version()
