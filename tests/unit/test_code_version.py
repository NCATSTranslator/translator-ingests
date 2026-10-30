"""Tests for reading the translator-ingests code version from git, using real temporary git repositories."""

import re
import subprocess
from pathlib import Path

import pytest

from translator_ingest.util.code_version import CodeVersion, get_code_version
from translator_ingest.util.metadata import PipelineMetadata

# Identity and signing settings so commits and tags work regardless of the user's git configuration
GIT_TEST_CONFIG = ("-c", "user.name=Test", "-c", "user.email=test@example.com",
                   "-c", "commit.gpgsign=false", "-c", "tag.gpgsign=false")

LIGHTWEIGHT_TAG: tuple[str, ...] = ()
ANNOTATED_TAG: tuple[str, ...] = ("-a", "-m", "release")


def git(repo: Path, *args: str) -> str:
    """Run a git command in repo and return its stripped output."""
    return subprocess.run(["git", *GIT_TEST_CONFIG, *args],
                          cwd=repo, check=True, capture_output=True, text=True).stdout.strip()


def commit(repo: Path, content: str) -> str:
    """Commit content to a tracked file in repo, returning the new commit hash."""
    (repo / "file.txt").write_text(content)
    git(repo, "add", "file.txt")
    git(repo, "commit", "-m", content)
    return git(repo, "rev-parse", "HEAD")


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    """A git repository with a single commit and no tags."""
    git(tmp_path, "init")
    commit(tmp_path, "first")
    return tmp_path


def test_code_version_of_untagged_repo_is_the_abbreviated_commit(repo: Path):
    code_version = get_code_version(repo)
    assert code_version["commit"] == git(repo, "rev-parse", "HEAD")
    assert re.fullmatch(r"[0-9a-f]{7,}", code_version["version"])
    assert code_version["commit"].startswith(code_version["version"])


@pytest.mark.parametrize("tag_args", [LIGHTWEIGHT_TAG, ANNOTATED_TAG])
def test_code_version_on_a_release_tag_is_the_tag(repo: Path, tag_args: tuple[str, ...]):
    git(repo, "tag", *tag_args, "v1.0.0")
    assert get_code_version(repo)["version"] == "v1.0.0"


def test_code_version_after_a_release_tag_counts_commits_since_it(repo: Path):
    git(repo, "tag", "v1.0.0")
    head = commit(repo, "second")
    code_version = get_code_version(repo)
    assert code_version["commit"] == head
    match = re.fullmatch(r"v1\.0\.0-1-g([0-9a-f]{7,})", code_version["version"])
    assert match is not None
    assert head.startswith(match.group(1))


def test_code_version_describes_the_latest_release_tag(repo: Path):
    git(repo, "tag", "v1.0.0")
    commit(repo, "second")
    git(repo, "tag", "v1.1.0")
    assert get_code_version(repo)["version"] == "v1.1.0"


@pytest.mark.parametrize("other_tag", ["banana", "v1.1", "release-2.0.0"])
def test_code_version_ignores_tags_that_are_not_release_tags(repo: Path, other_tag: str):
    git(repo, "tag", "v1.0.0")
    commit(repo, "second")
    git(repo, "tag", other_tag)
    assert get_code_version(repo)["version"].startswith("v1.0.0-1-g")


def test_code_version_with_uncommitted_changes_is_dirty(repo: Path):
    git(repo, "tag", "v1.0.0")
    head = git(repo, "rev-parse", "HEAD")
    (repo / "file.txt").write_text("uncommitted")
    assert get_code_version(repo) == CodeVersion(commit=head, version="v1.0.0-dirty")


def test_code_version_ignores_untracked_files(repo: Path):
    git(repo, "tag", "v1.0.0")
    (repo / "untracked.txt").write_text("untracked")
    assert get_code_version(repo)["version"] == "v1.0.0"


def test_code_version_outside_a_git_repository_fails(tmp_path: Path):
    with pytest.raises(subprocess.CalledProcessError):
        get_code_version(tmp_path)


def test_code_version_of_translator_ingests():
    code_version = get_code_version()
    assert re.fullmatch(r"[0-9a-f]{40}", code_version["commit"])
    assert code_version["version"]


def test_code_versions_do_not_change_the_build_version():
    """A new commit alone must not trigger a rebuild, so code versions are not part of the build version."""
    versions = dict(source="test_source", source_version="v1", transform_version="abc12345",
                    babel_version="babel", node_normalizer_version="nn", normalization_code_version="norm",
                    merging_code_version="merge", biolink_version="4.2.0")
    first = PipelineMetadata(**versions, code_versions={"build": CodeVersion(commit="a" * 40, version="v1.0.0")})
    second = PipelineMetadata(**versions, code_versions={"build": CodeVersion(commit="b" * 40, version="v1.1.0")})
    assert first.generate_build_version() == second.generate_build_version()


def test_code_versions_round_trip_through_release_metadata():
    code_versions = {"transform": None,
                     "normalization": CodeVersion(commit="a" * 40, version="v1.0.0"),
                     "build": CodeVersion(commit="b" * 40, version="v1.1.0")}
    pipeline_metadata = PipelineMetadata(source="test_source", code_versions=code_versions)
    assert PipelineMetadata.from_dict(pipeline_metadata.get_release_metadata()).code_versions == code_versions