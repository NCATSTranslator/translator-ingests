from typing import Optional, List

import requests
try:
    from yaml import dump, load, CLoader as Loader
except ImportError:
    from yaml import dump, load, Loader

class GitHubReleases:

    def __init__(self, git_org: str, git_repo: str, version_cache_file: Optional[str] = None):
        """
        Construct a GitHub repository-specific releases tracking object.
        :param git_org:
        :param git_repo:
        :param version_cache_file:
        """
        self.git_org: str = git_org
        self.git_repo: str = git_repo
        self.version_cache_file: str = version_cache_file \
            if version_cache_file \
            else f"{git_org}-{git_repo}-releases.yaml"
        self._release_catalog: Optional[List[str]] = None

    def get_release_catalog(self, refresh: bool = False):
        """
        Retrieve the GitHub release catalog.
        :param refresh: True if the catalog should be refreshed, False otherwise.
        :return: None but the internal cache of GitHubReleases is loaded with the catalog.
        """
        if refresh:

            response = requests.get(f"https://api.github.com/repos/{self.git_org}/{self.git_repo}/releases")
            release_data = response.json()
            version_data: List[str] = [release_tag["tag_name"] for release_tag in release_data]

            with open(self.version_cache_file, "w") as version_cache:
                dump(data=version_data, stream=version_cache)

        with open(self.version_cache_file, "r") as version_cache:
            # is now a two-level YAML catalog of "releases" and "branches"
            self._release_catalog = load(version_cache, Loader=Loader)

    def get_releases(self) -> List[str]:
        """
        Get the catalog of currently available project releases.
        :return: List of release version strings
        """
        if self._release_catalog is None:
            self.get_release_catalog()
        return self._release_catalog

    def get_latest_version(self) -> str:
        """
        Get the latest GitHub repository release
        :return: Version string (without any leading "v")
        """
        response = requests.get(f"https://api.github.com/repos/{self.git_org}/{self.git_repo}/releases/latest")
        release_data = response.json()
        return release_data["tag_name"].strip("v")
