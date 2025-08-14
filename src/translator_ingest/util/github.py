from typing import Optional, List, Dict
from os import environ
from os.path import join, abspath, dirname
import requests
try:
    from yaml import dump, load, CLoader as Loader
except ImportError:
    from yaml import dump, load, Loader

# Undocumented possible local environmental variable
# override of the ReasonerAPI schema access endpoint
GIT_ORG = environ.setdefault('GIT_ORGANIZATION', "NCATSTranslator")
GIT_REPO = environ.setdefault('GIT_REPOSITORY', "ReasonerAPI")

VERSION_CACHE_FILE: str = abspath(join(dirname(__file__), "versions.yaml"))

_version_catalog: Optional[Dict[str, List[str]]] = None


def get_releases(refresh: bool = False):
    global _version_catalog
    if refresh:

        version_data: Dict[str, List[str]] = dict()

        response = requests.get(f"https://api.github.com/repos/{GIT_ORG}/{GIT_REPO}/releases")
        release_data = response.json()
        version_data["releases"] = [release_tag["tag_name"] for release_tag in release_data]

        response = requests.get(f"https://api.github.com/repos/{GIT_ORG}/{GIT_REPO}/branches")
        branch_data = response.json()
        version_data["branches"] = [branch_tag["name"] for branch_tag in branch_data]

        with open(VERSION_CACHE_FILE, "w") as version_cache:
            dump(data=version_data, stream=version_cache)

    with open(VERSION_CACHE_FILE, "r") as version_cache:
        # is now a two-level YAML catalog of "releases" and "branches"
        _version_catalog = load(version_cache, Loader=Loader)


def get_versions() -> Dict:
    """
    Get the catalog of currently available TRAPI (GitHub) releases and branches.
    :return:
    """
    global _version_catalog
    if _version_catalog is None:
        get_releases()
    return _version_catalog
