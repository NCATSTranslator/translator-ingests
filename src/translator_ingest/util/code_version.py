"""The version of the translator-ingests code being run, read from git at run time.

Code versions are provenance: each pipeline stage records the code version that ran it in the metadata it writes,
so any output can be traced to the exact code that produced it. Individual pipeline stage outputs can be reused across 
commits, so a single graph can be made by several code versions. Code versions are deliberately not part of any transform or
build version: those decide whether work needs to be redone, and a new commit alone should not rebuild every source.
"""

import subprocess
from pathlib import Path
from typing import TypedDict

from translator_ingest import TRANSLATOR_INGEST_DIR

# The key used to record the code version a stage ran with.
CODE_VERSION_KEY = "code"

# Only semantic version release tags (e.g. v1.0.0) are used to describe the code version.
RELEASE_TAG_PATTERN = "v[0-9]*.[0-9]*.[0-9]*"


class CodeVersion(TypedDict):
    """The git state of the translator-ingests code, as a JSON serializable dict."""

    # commit: the full git commit hash, the exact identity of the code
    commit: str
    # version: a readable description of the commit relative to the latest release tag, from git describe.
    # e.g. "v1.0.0" on a release tag, "v1.0.0-3-g0123abc" three commits after one, or "0123abc" without any tags,
    # suffixed with "-dirty" if tracked files had uncommitted changes.
    version: str


def get_code_version(repo_dir: Path = TRANSLATOR_INGEST_DIR) -> CodeVersion:
    """Read the version of the code in a git repository, by default the translator-ingests code being run.

    Raises:
        subprocess.CalledProcessError: if repo_dir is not in a git repository with at least one commit.
    """

    def git(*args: str) -> str:
        return subprocess.run(["git", *args], cwd=repo_dir, check=True, capture_output=True, text=True).stdout.strip()

    return CodeVersion(
        commit=git("rev-parse", "HEAD"),
        version=git("describe", "--tags", f"--match={RELEASE_TAG_PATTERN}", "--always", "--dirty"),
    )