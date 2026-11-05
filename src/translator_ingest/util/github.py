from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from yaml import dump, load, CLoader as Loader
except ImportError:
    from yaml import dump, load, Loader

GITHUB_TIMEOUT_SECONDS = 60


def retrying_session() -> requests.Session:
    """A requests session that retries transient GitHub failures (as ORION does).

    Retries honor a ``Retry-After`` header. A primary API rate limit (403) is not retried, because it
    lasts until the hourly reset; the caller's ``raise_for_status()`` reports it instead.
    """
    session = requests.Session()
    retries = Retry(
        total=8,
        backoff_factor=0.5,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


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
        self.version_cache_file: str = (
            version_cache_file if version_cache_file else f"{git_org}-{git_repo}-releases.yaml"
        )
        self._release_catalog: Optional[list[str]] = None

    def get_release_catalog(self, refresh: bool = False):
        """
        Retrieve the GitHub release catalog.
        :param refresh: True if the catalog should be refreshed, False otherwise.
        :return: None but the internal cache of GitHubReleases is loaded with the catalog.
        """
        if refresh:

            response = requests.get(f"https://api.github.com/repos/{self.git_org}/{self.git_repo}/releases")
            release_data = response.json()
            version_data: list[str] = [release_tag["tag_name"] for release_tag in release_data]

            with open(self.version_cache_file, "w") as version_cache:
                dump(data=version_data, stream=version_cache)

        with open(self.version_cache_file, "r") as version_cache:
            # is now a two-level YAML catalog of "releases" and "branches"
            self._release_catalog = load(version_cache, Loader=Loader)

    def get_releases(self) -> list[str]:
        """
        Get the catalog of currently available project releases.
        :return: list of release version strings
        """
        if self._release_catalog is None:
            self.get_release_catalog()
        return self._release_catalog

    def get_latest_tag(self) -> str:
        """
        Get the git tag of the latest GitHub repository release, exactly as published.
        :return: Tag name, e.g. "v1.1.14"
        :raises requests.HTTPError: if GitHub refuses the request, e.g. when the API rate limit is exceeded
        """
        response = retrying_session().get(
            f"https://api.github.com/repos/{self.git_org}/{self.git_repo}/releases/latest",
            timeout=GITHUB_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        return response.json()["tag_name"]

    def get_latest_version(self) -> str:
        """
        Get the latest GitHub repository release
        :return: Version string (without any leading "v")
        """
        return self.get_latest_tag().removeprefix("v")
