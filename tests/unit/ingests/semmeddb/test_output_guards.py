"""Tests for the output guards of the text-location build (``analysis/output_guards.py``).

The guards run through the whole build in ``test_build_text_locations.py``; these tests pin the
locks, the staged writes and the aliases only the filesystem sees (hard links, firmlinks) on their
own, with real files and links in ``tmp_path``.
"""

import doctest
import errno
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from types import FrameType
from typing import Any

import pytest

from tests.unit.ingests.semmeddb.text_location_fixtures import hardlink, symlink
from translator_ingest.ingests.semmeddb.analysis import output_guards
from translator_ingest.ingests.semmeddb.analysis.text_location_report import REPO_ROOT
from translator_ingest.ingests.semmeddb.analysis.work_dir import WORK_DIR_MARKER
from translator_ingest.ingests.semmeddb.analysis.output_guards import (
    LOCK_SUFFIX,
    ResolvedOutputs,
    STAGED_SUFFIX,
    is_inside,
    lock_beside,
    output_locks,
    path_key,
    publishing,
    refuse_unsafe_outputs,
    same_file,
    staged_outputs,
    staged_path,
)

# How long the signal test waits for a signal sent to itself to reach a Python handler.
SIGNAL_WAIT_SECONDS = 0.2
# macOS also mounts the data volume here, so each of its directories has a second, firmlinked path.
MACOS_DATA_VOLUME = Path("/System/Volumes/Data")


class BuildFailedError(Exception):
    """Stands for any error that stops a build while it holds its locks."""


def test_module_doctests_pass() -> None:
    """The doctests of the output guards pass."""
    results = doctest.testmod(output_guards)
    assert results.failed == 0
    assert results.attempted > 0


def test_output_locks_are_exclusive_and_released(tmp_path: Path) -> None:
    """Each output gets a lock; a second claim of one fails and releases the locks it already took."""
    outputs = [tmp_path / "out" / "a.parquet", tmp_path / "out" / "a.json"]
    other = tmp_path / "b.parquet"
    with output_locks(ResolvedOutputs.at_preflight(outputs)):
        assert sorted(path.name for path in (tmp_path / "out").iterdir()) == ["a.json.lock", "a.parquet.lock"]
        with (
            pytest.raises(FileExistsError, match="a.json.lock"),
            output_locks(ResolvedOutputs.at_preflight([other, outputs[1]])),
        ):
            pass
        assert not other.with_name(other.name + LOCK_SUFFIX).exists()
        assert (tmp_path / "out" / "a.json.lock").exists()
    assert not list(tmp_path.rglob(f"*{LOCK_SUFFIX}"))


def test_output_locks_are_released_when_the_build_fails(tmp_path: Path) -> None:
    """A build that stops with an error still releases its locks."""
    with pytest.raises(BuildFailedError), output_locks(ResolvedOutputs.at_preflight([tmp_path / "a.parquet"])):
        raise BuildFailedError
    assert not list(tmp_path.rglob(f"*{LOCK_SUFFIX}"))


def test_output_lock_is_shared_by_paths_to_one_file(tmp_path: Path) -> None:
    """The lock sits next to the resolved output, so a symlink to a locked output finds it held."""
    real = tmp_path / "real.parquet"
    link = symlink(tmp_path / "link.parquet", real)
    with (
        output_locks(ResolvedOutputs.at_preflight([real])),
        pytest.raises(FileExistsError, match="real.parquet.lock"),
        output_locks(ResolvedOutputs.at_preflight([link])),
    ):
        pass


def test_hard_linked_outputs_are_one_path(tmp_path: Path) -> None:
    """Two outputs that are hard links to one file are refused, though their paths differ."""
    parquet = tmp_path / "a.parquet"
    parquet.write_bytes(b"an earlier build")
    manifest = hardlink(tmp_path / "a.json", parquet)
    with pytest.raises(ValueError, match="are one path"):
        refuse_unsafe_outputs(ResolvedOutputs.at_preflight([parquet, manifest]), [], {})


def test_directory_under_a_second_mount_path_is_found(tmp_path: Path) -> None:
    """A directory reached through a macOS firmlink is found, though resolving keeps the paths apart."""
    alias = MACOS_DATA_VOLUME / tmp_path.resolve().relative_to("/")
    if not same_file(alias, tmp_path):
        pytest.skip("this filesystem has no second mount path to the test directory")
    assert path_key(alias) != path_key(tmp_path)
    assert is_inside(alias / "out" / "a.parquet", tmp_path)


def test_an_output_that_is_another_outputs_lock_file_is_refused(tmp_path: Path) -> None:
    """An output hard-linked to an output's lock file is refused, though its own name has no lock suffix."""
    parquet = tmp_path / "a.parquet"
    lock_beside(parquet).write_text("a stale lock")
    manifest = hardlink(tmp_path / "a.json", lock_beside(parquet))
    with pytest.raises(ValueError, match="a.json is or is named like an output lock"):
        refuse_unsafe_outputs(ResolvedOutputs.at_preflight([parquet, manifest]), [], {})


def write_pair(directory: Path, parquet: bytes, manifest: bytes) -> list[Path]:
    """Write an output pair into ``directory`` and return the two paths, parquet first."""
    pair = [directory / "a.parquet", directory / "a.json"]
    for path, content in zip(pair, (parquet, manifest), strict=True):
        path.write_bytes(content)
    return pair


def test_a_failure_after_the_parquet_is_written_leaves_the_earlier_pair(tmp_path: Path) -> None:
    """Staged files are moved into place only when the block succeeds; a failure removes them."""
    pair = write_pair(tmp_path, b"old parquet", b"old manifest")
    with pytest.raises(BuildFailedError), staged_outputs(ResolvedOutputs.at_preflight(pair)) as staged:
        staged[0].write_bytes(b"new parquet")
        raise BuildFailedError
    assert [path.read_bytes() for path in pair] == [b"old parquet", b"old manifest"]
    assert sorted(tmp_path.iterdir()) == sorted(pair)


@pytest.mark.parametrize("old_parquet", [b"old parquet", None])
def test_a_manifest_move_that_fails_puts_the_old_parquet_back(old_parquet: bytes | None, tmp_path: Path) -> None:
    """When the manifest cannot be moved into place (here a directory holds its name), publishing is undone.

    The new parquet goes back to its staged name and is removed, the old parquet (or its absence)
    is restored, nothing else is left, and the error is raised.
    """
    parquet, manifest = tmp_path / "a.parquet", tmp_path / "a.json"
    if old_parquet is not None:
        parquet.write_bytes(old_parquet)
    manifest.mkdir()
    (manifest / "kept").write_text("kept")
    before = sorted(tmp_path.rglob("*"))
    with pytest.raises(IsADirectoryError), staged_outputs(ResolvedOutputs.at_preflight([parquet, manifest])) as staged:
        for path, content in zip(staged, (b"new parquet", b"new manifest"), strict=True):
            path.write_bytes(content)
    assert (parquet.read_bytes() if parquet.exists() else None) == old_parquet
    assert (manifest / "kept").read_text() == "kept"
    assert sorted(tmp_path.rglob("*")) == before


def test_staged_outputs_replace_each_output_with_a_new_file(tmp_path: Path) -> None:
    """On success each staged file replaces its output; a hard link to an old output keeps its bytes."""
    pair = write_pair(tmp_path, b"old parquet", b"old manifest")
    old_manifest = hardlink(tmp_path / "old.json", pair[1])
    with staged_outputs(ResolvedOutputs.at_preflight(pair)) as staged:
        assert all(path.name.startswith(".") and path.name.endswith(STAGED_SUFFIX) for path in staged)
        for path, content in zip(staged, (b"new parquet", b"new manifest"), strict=True):
            path.write_bytes(content)
    assert [path.read_bytes() for path in pair] == [b"new parquet", b"new manifest"]
    assert old_manifest.read_bytes() == b"old manifest"
    assert sorted(tmp_path.iterdir()) == sorted([*pair, old_manifest])
    assert not list(tmp_path.glob(f"*{STAGED_SUFFIX}"))


@pytest.mark.parametrize("old_parquet", [b"old parquet", None])
def test_every_output_exists_at_every_step_of_publishing(old_parquet: bytes | None, tmp_path: Path) -> None:
    """A reader never finds an output missing: the old parquet is kept as a hard link, not moved aside.

    Publishing is run one filesystem step at a time. Where there is no old parquet, it only needs to
    exist once it is published.
    """
    parquet, manifest = tmp_path / "a.parquet", tmp_path / "a.json"
    if old_parquet is not None:
        parquet.write_bytes(old_parquet)
    manifest.write_bytes(b"old manifest")
    moves = [(staged_path(output), output) for output in (parquet, manifest)]
    for (staged, _), content in zip(moves, (b"new parquet", b"new manifest"), strict=True):
        staged.write_bytes(content)
    present = [(parquet.exists(), manifest.exists()) for _ in publishing(moves)]
    assert present and all(manifest_present for _, manifest_present in present)
    assert all(parquet_present for parquet_present, _ in present) or old_parquet is None
    assert (parquet.read_bytes(), manifest.read_bytes()) == (b"new parquet", b"new manifest")
    assert sorted(tmp_path.iterdir()) == sorted([parquet, manifest])


def test_a_parquet_move_that_fails_leaves_the_old_parquet_and_no_backup(tmp_path: Path) -> None:
    """If the parquet itself cannot be moved (its staged file is gone), the old pair stays and its backup goes.

    Renaming the backup back onto the old parquet, which is the same file, leaves both names, so the
    backup name has to be removed.
    """
    pair = write_pair(tmp_path, b"old parquet", b"old manifest")
    moves = [(staged_path(output), output) for output in pair]
    moves[1][0].write_bytes(b"new manifest")
    with pytest.raises(FileNotFoundError):
        for _ in publishing(moves):
            pass
    assert [path.read_bytes() for path in pair] == [b"old parquet", b"old manifest"]
    assert sorted(tmp_path.iterdir()) == sorted([*pair, moves[1][0]])


def test_a_signal_during_publishing_is_handled_once_the_pair_is_consistent(tmp_path: Path, request: Any) -> None:
    """A SIGTERM sent between the parquet's and the manifest's renames reaches its handler only after both.

    The test's own SIGTERM handler records the pair it sees instead of ending the process.
    """
    pair = write_pair(tmp_path, b"old parquet", b"old manifest")
    moves = [(staged_path(output), output) for output in pair]
    for (staged, _), content in zip(moves, (b"new parquet", b"new manifest"), strict=True):
        staged.write_bytes(content)
    handled: list[list[bytes]] = []

    def on_sigterm(_number: int, _frame: FrameType | None) -> None:
        handled.append([path.read_bytes() for path in pair])
        assert sorted(tmp_path.iterdir()) == sorted(pair), "a backup or staged file is left"

    previous = signal.signal(signal.SIGTERM, on_sigterm)
    request.addfinalizer(lambda: signal.signal(signal.SIGTERM, previous))
    for step, _ in enumerate(publishing(moves)):
        if [path.read_bytes() for path in pair] == [b"new parquet", b"old manifest"]:
            os.kill(os.getpid(), signal.SIGTERM)
            time.sleep(SIGNAL_WAIT_SECONDS)
            assert (step, handled) == (1, [])
    assert handled == [[b"new parquet", b"new manifest"]]
    assert signal.getsignal(signal.SIGTERM) is on_sigterm


LOCK_WRITE_FAILURE_SCRIPT = """
import resource, signal, sys
from pathlib import Path
from translator_ingest.ingests.semmeddb.analysis.output_guards import ResolvedOutputs, output_locks

signal.signal(signal.SIGXFSZ, signal.SIG_IGN)
resource.setrlimit(resource.RLIMIT_FSIZE, (0, resource.getrlimit(resource.RLIMIT_FSIZE)[1]))
with output_locks(ResolvedOutputs.at_preflight([Path(sys.argv[1])])):
    pass
"""


def test_a_lock_whose_write_fails_is_removed(tmp_path: Path) -> None:
    """A lock created but not written (a full disk, here a file size limit of 0) is removed, not left behind.

    The limit is set in a child process, where creating the empty lock works but writing it fails
    with EFBIG; the child's exit status and message show the write failed.
    """
    output = tmp_path / "a.parquet"
    result = subprocess.run(
        [sys.executable, "-c", LOCK_WRITE_FAILURE_SCRIPT, str(output)], capture_output=True, text=True, check=False
    )
    assert result.returncode != 0
    assert f"[Errno {errno.EFBIG}]" in result.stderr
    assert not list(tmp_path.iterdir())


def test_an_output_inside_a_marked_work_dir_is_refused(tmp_path: Path) -> None:
    """An output below a directory holding the work dir marker is refused, also through a symlinked parent."""
    work = tmp_path / "work"
    (work / "output").mkdir(parents=True)
    (work / WORK_DIR_MARKER).write_text("a running build")
    linked = symlink(tmp_path / "linked", work / "output")
    for output in (work / "output" / "a.parquet", linked / "a.parquet"):
        with pytest.raises(ValueError, match="the work dir of a running or failed build"):
            refuse_unsafe_outputs(ResolvedOutputs.at_preflight([output, tmp_path / "a.json"]), [], {})
    (work / WORK_DIR_MARKER).unlink()
    refuse_unsafe_outputs(ResolvedOutputs.at_preflight([work / "output" / "a.parquet", tmp_path / "a.json"]), [], {})


def test_locks_check_for_a_live_work_dir_once_taken(tmp_path: Path) -> None:
    """A work dir claimed around an output after the preflight stops the build once its locks are taken."""
    outputs = [tmp_path / "work" / "output" / "a.parquet", tmp_path / "a.json"]
    refuse_unsafe_outputs(ResolvedOutputs.at_preflight(outputs), [], {})
    (tmp_path / "work").mkdir()
    (tmp_path / "work" / WORK_DIR_MARKER).write_text("another build")
    with (
        pytest.raises(ValueError, match="the work dir of a running or failed build"),
        output_locks(ResolvedOutputs.at_preflight(outputs)),
    ):
        pass
    assert not list(tmp_path.rglob(f"*{LOCK_SUFFIX}"))


def test_publishing_checks_for_a_live_work_dir_just_before_it_moves_anything(tmp_path: Path) -> None:
    """A work dir claimed around the outputs while they are staged stops the publish; the old pair stays."""
    pair = write_pair(tmp_path, b"old parquet", b"old manifest")
    with (
        pytest.raises(ValueError, match="the work dir of a running or failed build"),
        staged_outputs(ResolvedOutputs.at_preflight(pair)) as staged,
    ):
        for path, content in zip(staged, (b"new parquet", b"new manifest"), strict=True):
            path.write_bytes(content)
        (tmp_path / WORK_DIR_MARKER).write_text("another build")
    assert [path.read_bytes() for path in pair] == [b"old parquet", b"old manifest"]
    assert sorted(tmp_path.iterdir()) == sorted([*pair, tmp_path / WORK_DIR_MARKER])


# One of the build's own source files, which an output must never replace.
CODE_FILE = REPO_ROOT / "src/translator_ingest/ingests/semmeddb/analysis/kg2_records.py"


def test_an_output_that_is_one_of_the_builds_code_files_is_refused(tmp_path: Path) -> None:
    """An output at a code file's path, or a symlink to one, is refused before anything is written."""
    for output in (CODE_FILE, symlink(tmp_path / "link.json", CODE_FILE)):
        with pytest.raises(ValueError, match="is the build's code file"):
            refuse_unsafe_outputs(ResolvedOutputs.at_preflight([tmp_path / "a.parquet", output]), [], {}, [CODE_FILE])


def test_resolved_outputs_stop_the_build_when_a_directory_is_replaced(tmp_path: Path) -> None:
    """The output directories identified at preflight, or when the locks create them, must stay the same."""
    existing, created = tmp_path / "existing", tmp_path / "created"
    existing.mkdir()
    outputs = ResolvedOutputs.at_preflight([existing / "a.parquet", created / "a.json"])
    created.mkdir()
    outputs.identify_new_directories()
    outputs.check_directories()
    existing.rename(tmp_path / "moved")
    existing.mkdir()
    with pytest.raises(ValueError, match=f"output directories replaced while the build ran: .*{existing.name}"):
        outputs.check_directories()


def test_the_guard_checks_the_paths_resolved_at_preflight(tmp_path: Path) -> None:
    """An output symlink is retargeted between the resolution and the checks.

    The guard checks the paths resolved at preflight, which are the ones the build locks, stages and
    publishes. Resolved onto the KG2 input, the output is refused although the link now points to a
    safe file; resolved onto a safe file, it is published there although the link now points to the
    input, which keeps its bytes.
    """
    kg2_edges, safe = tmp_path / "edges.jsonl.gz", tmp_path / "safe.json"
    kg2_edges.write_bytes(b"the KG2 input")
    link = symlink(tmp_path / "out.json", kg2_edges)
    onto_the_input = ResolvedOutputs.at_preflight([tmp_path / "a.parquet", link])
    link.unlink()
    link.symlink_to(safe)
    with pytest.raises(ValueError, match="is the input"):
        refuse_unsafe_outputs(onto_the_input, [kg2_edges], {})
    onto_the_safe_file = ResolvedOutputs.at_preflight([tmp_path / "a.parquet", link])
    link.unlink()
    link.symlink_to(kg2_edges)
    refuse_unsafe_outputs(onto_the_safe_file, [kg2_edges], {})
    with output_locks(onto_the_safe_file), staged_outputs(onto_the_safe_file) as staged:
        for path, content in zip(staged, (b"new parquet", b"new manifest"), strict=True):
            path.write_bytes(content)
    assert (kg2_edges.read_bytes(), safe.read_bytes()) == (b"the KG2 input", b"new manifest")


@pytest.mark.parametrize("work_dir", ["export/work", "export", "EXPORT/work"])
def test_a_work_dir_inside_or_equal_to_the_export_dir_is_refused(work_dir: str, tmp_path: Path) -> None:
    """The work dir may not lie inside the export dir (or be it, in any case), nor the other way round."""
    (tmp_path / "export").mkdir()
    directories = {"--semmeddb-dir": tmp_path / "export", "--work-dir": tmp_path / work_dir}
    outputs = ResolvedOutputs.at_preflight([tmp_path / "a.parquet", tmp_path / "a.json"])
    with pytest.raises(ValueError, match="--work-dir .* is inside or equal to --semmeddb-dir"):
        refuse_unsafe_outputs(outputs, [], directories)
    refuse_unsafe_outputs(outputs, [], {**directories, "--work-dir": tmp_path / "work"})
