"""Tests that the text-location build never destroys its inputs, a previous artifact or another build's files.

They run the whole build on the fixture set (``conftest.py``) with outputs, locks, work dirs and
signals arranged to collide; ``test_output_guards.py`` and ``test_work_dir.py`` pin the pieces.
"""

import json
import logging
import os
import re
import shutil
import signal
import stat
import unicodedata
from collections.abc import Callable
from pathlib import Path
from types import FrameType
from typing import Any

import polars as pl
import pytest

from tests.unit.ingests.semmeddb.text_location_fixtures import (
    CHUNK_A_ROWS,
    EXPECTED_ROWS,
    KG2_RECORDS,
    TOTAL_ROWS,
    argv_for,
    hardlink,
    input_bytes,
    leftover_build_files,
    output_rows,
    symlink,
    write_inputs,
)
from translator_ingest.ingests.semmeddb.analysis.build_text_locations import main
from translator_ingest.ingests.semmeddb.analysis.output_guards import LOCK_SUFFIX, same_file
from translator_ingest.ingests.semmeddb.analysis.text_location_report import REPO_ROOT
from translator_ingest.ingests.semmeddb.analysis.work_dir import WORK_DIR_MARKER


def test_work_dir_is_removed_unless_kept(tmp_path: Path) -> None:
    """The work dir is deleted at the end unless --keep-work is given; the output locks always are."""
    paths = write_inputs(tmp_path, {"chunk_001": CHUNK_A_ROWS}, KG2_RECORDS[:2])
    assert main(argv_for(paths, len(CHUNK_A_ROWS))) == 0
    assert not paths["work_dir"].exists()
    assert not leftover_build_files(paths)
    assert main(argv_for(paths, len(CHUNK_A_ROWS), "--keep-work")) == 0
    assert (paths["work_dir"] / "output").is_dir()


@pytest.mark.parametrize(
    "output, target, message",
    [
        ("out_parquet", lambda paths: paths["work_dir"] / "nested" / "output", "inside --work-dir"),
        ("out_manifest", lambda paths: paths["work_dir"] / "manifest.json", "inside --work-dir"),
        ("out_manifest", lambda paths: paths["out_parquet"], "are one path"),
        ("out_parquet", lambda paths: paths["kg2_edges"], "is the input"),
        ("out_manifest", lambda paths: paths["mrcui"], "is the input"),
        ("out_parquet", lambda paths: paths["semmeddb_dir"] / "chunk_001.tsv.zst", "inside --semmeddb-dir"),
        (
            "out_manifest",
            lambda paths: paths["out_parquet"].with_name(paths["out_parquet"].name.upper()),
            "are one path",
        ),
        # A symlink outside the export dir to a chunk, and a dangling one into the work dir.
        (
            "out_manifest",
            lambda paths: symlink(paths["mrcui"].with_name("link.json"), paths["semmeddb_dir"] / "chunk_002.tsv.zst"),
            "is the input .*chunk_002.tsv.zst",
        ),
        (
            "out_parquet",
            lambda paths: symlink(paths["mrcui"].with_name("link.parquet"), paths["work_dir"] / "out.parquet"),
            "inside --work-dir",
        ),
        # A hard link outside the export dir to one of its files, and a path through a symlinked dir.
        (
            "out_manifest",
            lambda paths: hardlink(paths["mrcui"].with_name("rows.json"), paths["semmeddb_dir"] / "chunk_002.rows"),
            "is the input .*chunk_002.rows",
        ),
        (
            "out_parquet",
            lambda paths: symlink(paths["mrcui"].with_name("linked_dir"), paths["semmeddb_dir"]) / "out.parquet",
            "inside --semmeddb-dir",
        ),
        # The other output's lock, which lock cleanup deletes, and a link to another build's lock name.
        (
            "out_manifest",
            lambda paths: paths["out_parquet"].with_name(paths["out_parquet"].name + LOCK_SUFFIX),
            "named like an output lock",
        ),
        (
            "out_manifest",
            lambda paths: symlink(
                paths["mrcui"].with_name("link.json"), paths["mrcui"].with_name("other.parquet.LOCK")
            ),
            "named like an output lock",
        ),
        ("out_parquet", lambda paths: paths["mrcui"].parent, "is a directory"),
        # An output named like the work dir marker would mark its directory a live work dir.
        (
            "out_manifest",
            lambda paths: paths["out_parquet"].with_name(WORK_DIR_MARKER),
            "named like the work dir marker",
        ),
        # An output that would become the directory holding the other output (absent, in another case).
        ("out_manifest", lambda paths: paths["out_parquet"].parent, "would contain another output"),
        ("out_manifest", lambda paths: paths["out_parquet"].parent.with_name("OUT"), "would contain another output"),
        ("out_parquet", lambda paths: paths["out_manifest"] / "nested.parquet", "would contain another output"),
        # A work dir inside or equal to an output or a lock: creating it would make that path a directory.
        # An output inside the other output's lock path, which taking that lock creates as a file.
        (
            "out_parquet",
            lambda paths: paths["out_manifest"].with_name(paths["out_manifest"].name + LOCK_SUFFIX) / "out.parquet",
            "inside an output's lock path",
        ),
        ("work_dir", lambda paths: paths["out_parquet"] / "work", "--work-dir .* inside or equal to the output"),
        # A work dir inside the export dir would change the export the build hashed and checked.
        (
            "work_dir",
            lambda paths: paths["semmeddb_dir"] / "work",
            "--work-dir .* is inside or equal to --semmeddb-dir",
        ),
        ("work_dir", lambda paths: paths["out_manifest"], "--work-dir .* inside or equal to the output"),
        (
            "work_dir",
            lambda paths: paths["out_parquet"].with_name(paths["out_parquet"].name.upper() + LOCK_SUFFIX) / "work",
            "--work-dir .* inside or equal to the output or lock",
        ),
    ],
)
def test_unsafe_output_paths_are_refused(
    output: str, target: Callable[[dict[str, Path]], Path], message: str, inputs: dict[str, Path]
) -> None:
    """Outputs on one path, on an input or inside --semmeddb-dir or --work-dir, through links too, are refused.

    Nothing is written: every input keeps its bytes and neither the work dir nor a lock is created.
    """
    inputs[output] = target(inputs)
    before = input_bytes(inputs)
    with pytest.raises(ValueError, match=f"unsafe output paths: .*{message}"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert input_bytes(inputs) == before
    assert not inputs["work_dir"].exists()
    assert not (inputs["mrcui"].parent / "out").exists()
    assert not leftover_build_files(inputs)


def test_a_build_replaces_hard_linked_and_read_only_outputs_as_new_files(inputs: dict[str, Path]) -> None:
    """Outputs are moved into place, not written into: other links to the old files keep the old bytes.

    The old manifest is read-only and hard-linked elsewhere, and the old parquet is hard-linked too.
    """
    old: dict[str, Path] = {}
    for output in ("out_parquet", "out_manifest"):
        inputs[output].parent.mkdir(exist_ok=True)
        inputs[output].write_text(f"old {output}")
        old[output] = hardlink(inputs["mrcui"].with_name(f"old_{output}"), inputs[output])
    inputs["out_manifest"].chmod(0o444)
    assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    assert set(output_rows(inputs)) == EXPECTED_ROWS
    assert json.loads(inputs["out_manifest"].read_text())["output"]["rows"] == len(EXPECTED_ROWS)
    for output, link in old.items():
        assert link.read_text() == f"old {output}"
        assert not same_file(link, inputs[output])
    assert not leftover_build_files(inputs)


@pytest.mark.skipif(not hasattr(os, "chflags"), reason="needs BSD file flags to make a file immutable")
def test_a_manifest_that_cannot_be_replaced_leaves_the_old_pair(inputs: dict[str, Path], request: Any) -> None:
    """An immutable old manifest fails its replacement after the parquet's; the old parquet is put back.

    The build raises the error, and the old pair, staged files and locks are as before the build.
    """
    old = {"out_parquet": b"old parquet", "out_manifest": b"old manifest"}
    for output, content in old.items():
        inputs[output].parent.mkdir(exist_ok=True)
        inputs[output].write_bytes(content)
    parquet_inode = inputs["out_parquet"].stat().st_ino
    os.chflags(inputs["out_manifest"], stat.UF_IMMUTABLE)
    request.addfinalizer(lambda: os.chflags(inputs["out_manifest"], 0))
    with pytest.raises(PermissionError):
        main(argv_for(inputs, TOTAL_ROWS))
    assert {output: inputs[output].read_bytes() for output in old} == old
    assert inputs["out_parquet"].stat().st_ino == parquet_inode
    assert not leftover_build_files(inputs)


# The build's log message right after it publishes, before cleanup and the lock release.
PUBLISHED_MESSAGE = "published "
# Its first message once the work dir is claimed and holds parts: the first SemMedDB chunk is split.
FIRST_CHUNK_MESSAGE = "SemMedDB: "


class OnLogMessage(logging.Handler):
    """Runs an action once, on the first build log message that starts with ``prefix``."""

    def __init__(self, prefix: str, action: Callable[[], object]) -> None:
        super().__init__()
        self.prefix = prefix
        self.action: Callable[[], object] | None = action

    def emit(self, record: logging.LogRecord) -> None:
        """Run the action if the message starts with the prefix and it has not run yet."""
        if self.action is not None and record.getMessage().startswith(self.prefix):
            action, self.action = self.action, None
            action()


def on_log_message(prefix: str, action: Callable[[], object], request: pytest.FixtureRequest) -> None:
    """Make the build run ``action`` at its first log message starting with ``prefix``, for this test only."""
    logger = logging.getLogger("build_text_locations")
    handler = OnLogMessage(prefix, action)
    logger.addHandler(handler)
    request.addfinalizer(lambda: logger.removeHandler(handler))
    level = logger.level
    logger.setLevel(logging.INFO)
    request.addfinalizer(lambda: logger.setLevel(level))


def test_a_signal_after_publishing_waits_until_the_locks_are_released(
    inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """A SIGTERM arriving between the publish and the lock release is handled only once the build is done.

    The test's own handler records what it finds instead of ending the process: the published pair,
    and no lock, staged file or backup left.
    """
    handled: list[list[Path]] = []

    def on_sigterm(_number: int, _frame: FrameType | None) -> None:
        handled.append(leftover_build_files(inputs))

    previous = signal.signal(signal.SIGTERM, on_sigterm)
    request.addfinalizer(lambda: signal.signal(signal.SIGTERM, previous))
    on_log_message(PUBLISHED_MESSAGE, lambda: os.kill(os.getpid(), signal.SIGTERM), request)
    assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    assert handled == [[]]
    assert set(output_rows(inputs)) == EXPECTED_ROWS


# Build B's pair, published inside build A's work dir while A runs, and a part of A's that B replaced.
FOREIGN_WORK_FILES = (
    "output/bucket_release.parquet",
    "output/bucket_release.manifest.json",
    "output/bucket_000.parquet",
)


def test_cleanup_removes_only_what_the_build_created(
    inputs: dict[str, Path], request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> None:
    """Cleanup removes the exact files and directories the build created, while they still are those.

    Build B publishes ``output/bucket_release.parquet`` and its manifest inside build
    A's work dir before A cleans up, and replaces A's ``output/bucket_000.parquet`` with a file of
    its own. A's cleanup keeps all three, the directories holding them and the work dir, and warns.
    """
    work = inputs["work_dir"]

    def publish_build_b() -> None:
        for name in FOREIGN_WORK_FILES:
            staged = work / f"{name}.b"
            staged.write_text(f"build B: {name}")
            os.replace(staged, work / name)

    on_log_message(PUBLISHED_MESSAGE, publish_build_b, request)
    with caplog.at_level(logging.WARNING):
        assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    assert {name: (work / name).read_text() for name in FOREIGN_WORK_FILES} == {
        name: f"build B: {name}" for name in FOREIGN_WORK_FILES
    }
    assert sorted(path.relative_to(work).as_posix() for path in work.rglob("*")) == sorted(
        ["output", *FOREIGN_WORK_FILES]
    )
    assert "holds what the build did not write" in caplog.text


def test_outputs_through_safe_symlinks_write_the_files_they_point_to(inputs: dict[str, Path]) -> None:
    """A symlinked output that resolves to a safe place is allowed, and the build writes through it."""
    real = inputs["semmeddb_dir"].parent / "real"
    for output in ("out_parquet", "out_manifest"):
        inputs[output] = symlink(inputs["semmeddb_dir"].parent / f"{output}.link", real / inputs[output].name)
    assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    assert inputs["out_parquet"].is_symlink() and inputs["out_manifest"].is_symlink()
    assert set(pl.read_parquet(real / inputs["out_parquet"].resolve().name).rows()) == EXPECTED_ROWS
    manifest = json.loads((real / inputs["out_manifest"].resolve().name).read_text())
    assert manifest["output"]["rows"] == len(EXPECTED_ROWS)


@pytest.mark.parametrize("output", ["out_parquet", "out_manifest"])
def test_a_build_of_outputs_another_build_holds_stops(output: str, inputs: dict[str, Path]) -> None:
    """Each output is locked: a build finding ``<output>.lock`` (a running build) stops before writing.

    The other build's lock stays, and this build releases the lock it took on the other output.
    """
    lock = inputs[output].with_name(inputs[output].name + LOCK_SUFFIX)
    lock.parent.mkdir(parents=True)
    lock.write_text("running build")
    with pytest.raises(FileExistsError, match=re.escape(str(lock.name))):
        main(argv_for(inputs, TOTAL_ROWS))
    assert lock.read_text() == "running build"
    assert list(lock.parent.iterdir()) == [lock]
    assert not inputs["work_dir"].exists()


def test_a_second_build_on_the_same_work_dir_stops_and_leaves_it_alone(inputs: dict[str, Path]) -> None:
    """The work dir is claimed atomically: a build finding it taken (by a running build) stops untouched."""
    inputs["work_dir"].mkdir()
    marker = inputs["work_dir"] / "other_build_in_progress"
    marker.write_text("busy")
    with pytest.raises(FileExistsError):
        main(argv_for(inputs, TOTAL_ROWS))
    assert marker.read_text() == "busy"
    assert not inputs["out_parquet"].exists()
    assert not leftover_build_files(inputs)


def folds_case(directory: Path) -> bool:
    """Return whether the filesystem holding ``directory`` treats names that differ in case as one."""
    (directory / "case_probe").write_text("")
    return (directory / "CASE_PROBE").exists()


def test_case_aliased_input_is_refused_where_the_filesystem_folds_case(inputs: dict[str, Path]) -> None:
    """An output named like an input in another case is that input where the filesystem folds case."""
    inputs["out_manifest"] = inputs["kg2_edges"].with_name(inputs["kg2_edges"].name.upper())
    if folds_case(inputs["kg2_edges"].parent):
        with pytest.raises(ValueError, match="is the input"):
            main(argv_for(inputs, TOTAL_ROWS))
    else:
        assert main(argv_for(inputs, TOTAL_ROWS)) == 0


# A work dir name with a letter that has a precomposed and a decomposed Unicode form.
ACCENTED_WORK_DIR = "work_caf\u00e9"


@pytest.mark.parametrize("alias", [ACCENTED_WORK_DIR.upper(), unicodedata.normalize("NFD", ACCENTED_WORK_DIR)])
def test_output_under_an_alias_of_the_work_dir_is_refused(alias: str, inputs: dict[str, Path]) -> None:
    """An output under the work dir's name in another case or Unicode form is refused on every filesystem.

    The work dir does not exist yet, so the filesystem cannot say whether the names are one; they
    are compared ignoring case and normalization, which on a case-sensitive filesystem refuses a
    safe path but never lets the build delete its own output.
    """
    inputs["work_dir"] = inputs["work_dir"].with_name(ACCENTED_WORK_DIR)
    inputs["out_parquet"] = inputs["work_dir"].with_name(alias) / "locations.parquet"
    with pytest.raises(ValueError, match="inside --work-dir"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert not inputs["work_dir"].exists()
    assert not inputs["out_parquet"].parent.exists()


def work_dir_state(work: Path) -> list[tuple[str, int, int]]:
    """Return every path under ``work`` with its inode and size, to tell whether anything changed."""
    return sorted(
        (path.relative_to(work).as_posix(), path.stat().st_ino, path.stat().st_size) for path in work.rglob("*")
    )


def test_a_build_refuses_an_output_inside_a_running_builds_work_dir(
    inputs: dict[str, Path], tmp_path: Path, request: pytest.FixtureRequest, caplog: pytest.LogCaptureFixture
) -> None:
    """While build A runs, build B asks to publish into A's work dir; B refuses, A goes on.

    B is started when A has split its first chunk. Its parquet would be ``output/bucket_000.parquet``
    in A's work dir, which A writes, reads and cleans up. B stops before writing anything, A's work
    dir is unchanged by it, and A finishes and removes its work dir as usual.
    """
    work = inputs["work_dir"]
    build_b = write_inputs(tmp_path / "build_b", {"chunk_001": CHUNK_A_ROWS}, KG2_RECORDS[:2])
    build_b["out_parquet"] = work / "output" / "bucket_000.parquet"
    attempts: list[tuple[str, list[tuple[str, int, int]], list[tuple[str, int, int]]]] = []

    def start_build_b() -> None:
        before = work_dir_state(work)
        with pytest.raises(ValueError, match="unsafe output paths") as refused:
            main(argv_for(build_b, len(CHUNK_A_ROWS)))
        attempts.append((str(refused.value), before, work_dir_state(work)))

    on_log_message(FIRST_CHUNK_MESSAGE, start_build_b, request)
    with caplog.at_level(logging.WARNING):
        assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    assert len(attempts) == 1
    message, before, after = attempts[0]
    assert f"inside {work.resolve()}, the work dir of a running or failed build" in message
    assert before == after
    assert not build_b["work_dir"].exists() and not leftover_build_files(build_b)
    assert not work.exists()
    assert "holds what the build did not write" not in caplog.text
    assert set(output_rows(inputs)) == EXPECTED_ROWS


# Build log messages after which a part is in place: every KG2 part is written (before matching
# reads the pairs), the first output part is written (before the artifact is assembled), or the
# artifact is assembled (before it is published).
KEPT_GROUPS_MESSAGE = "raw groups per kept record"
FIRST_OUTPUT_PART_MESSAGE = "bucket "
ASSEMBLED_MESSAGE = "assembled "


@pytest.mark.parametrize(
    "message, part",
    [
        (KEPT_GROUPS_MESSAGE, "pairs/bucket_000/part_00000.parquet"),
        (FIRST_OUTPUT_PART_MESSAGE, "output/bucket_000.parquet"),
        (ASSEMBLED_MESSAGE, "output/bucket_001.parquet"),
    ],
)
def test_a_part_replaced_after_the_build_wrote_it_stops_the_build(
    message: str,
    part: str,
    inputs: dict[str, Path],
    request: pytest.FixtureRequest,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Another process puts a copy of a part at its name (a new inode, the same rows).

    The build checks every part it wrote before it assembles the artifact and again before it
    publishes, so it stops, and nothing is published: no parquet, no manifest, no lock or staged file.
    A part replaced before the assembly stops the build before it assembles anything.
    """
    work = inputs["work_dir"]

    def replace_part() -> None:
        copy = work / f"{part}.copy"
        shutil.copyfile(work / part, copy)
        os.replace(copy, work / part)

    on_log_message(message, replace_part, request)
    with (
        caplog.at_level(logging.INFO, logger="build_text_locations"),
        pytest.raises(ValueError, match=f"changed after the build wrote them, the first: .*{re.escape(part)}"),
    ):
        main(argv_for(inputs, TOTAL_ROWS))
    assert (ASSEMBLED_MESSAGE in caplog.text) == (message == ASSEMBLED_MESSAGE)
    assert not inputs["out_parquet"].exists()
    assert not inputs["out_manifest"].exists()
    assert not leftover_build_files(inputs)


def test_a_build_never_overwrites_a_file_another_process_put_at_one_of_its_names(
    inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """Another build publishes ``output/bucket_000.parquet`` in this work dir before this build writes it.

    The part is linked to its name, which fails when anything is there: the build stops loudly, the
    other file keeps its bytes, and nothing is published.
    """
    foreign = inputs["work_dir"] / "output" / "bucket_000.parquet"

    def publish_build_b() -> None:
        foreign.parent.mkdir()
        foreign.write_bytes(b"build B's parquet")

    on_log_message(KEPT_GROUPS_MESSAGE, publish_build_b, request)
    with pytest.raises(FileExistsError):
        main(argv_for(inputs, TOTAL_ROWS))
    assert foreign.read_bytes() == b"build B's parquet"
    assert not inputs["out_parquet"].exists() and not inputs["out_manifest"].exists()
    assert not leftover_build_files(inputs)


@pytest.mark.parametrize("bucket_dir", ["pairs/bucket_000", "semmeddb/bucket_000"])
def test_a_part_the_build_did_not_write_stops_the_build(
    bucket_dir: str, inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """A stale parquet with the same schema sits in a bucket dir, where a listing would read it.

    The build reads only the parts it recorded and stops when a bucket dir holds anything else, so
    a stale part can never add an earlier mention and change the spans. Nothing is published.
    """
    directory = inputs["work_dir"] / bucket_dir

    def add_stale_part() -> None:
        shutil.copyfile(sorted(directory.glob("*.parquet"))[0], directory / "stale.parquet")

    on_log_message(KEPT_GROUPS_MESSAGE, add_stale_part, request)
    with pytest.raises(ValueError, match=f"{re.escape(bucket_dir)} holds files the build did not write"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert not inputs["out_parquet"].exists() and not inputs["out_manifest"].exists()


@pytest.mark.parametrize("changed", ["kg2_edges", "mrcui", "chunk_002"])
def test_an_input_changed_while_the_build_ran_stops_it(
    changed: str, inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """An input replaced after the build read it would leave the manifest describing a file the build never read.

    Every input is hashed before the build reads it and again before it publishes, so the build stops
    and publishes nothing.
    """
    path = inputs["semmeddb_dir"] / f"{changed}.tsv.zst" if changed.startswith("chunk_") else inputs[changed]

    def change_input() -> None:
        with path.open("ab") as handle:
            handle.write(b"changed")

    on_log_message(KEPT_GROUPS_MESSAGE, change_input, request)
    with pytest.raises(ValueError, match=f"inputs changed while the build ran: .*{re.escape(path.name)}"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert not inputs["out_parquet"].exists() and not inputs["out_manifest"].exists()
    assert not leftover_build_files(inputs)


def test_an_output_inside_a_work_dir_claimed_after_the_preflight_is_refused_before_publishing(
    inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """Another build marks the outputs' directory as its work dir after this build's preflight.

    The publish checks for a live work dir again just before it moves anything, so the build stops,
    the outputs are not published, and the staged files and locks are gone.
    """
    marker = inputs["out_parquet"].parent / WORK_DIR_MARKER
    on_log_message(ASSEMBLED_MESSAGE, lambda: marker.write_text("build A"), request)
    with pytest.raises(ValueError, match="the work dir of a running or failed build"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert not inputs["out_parquet"].exists() and not inputs["out_manifest"].exists()
    assert not leftover_build_files(inputs)


def test_a_bracketed_work_dir_never_reads_a_decoy_sibling(inputs: dict[str, Path], tmp_path: Path) -> None:
    """polars reads a path as a glob unless told not to, and ``work[1]`` matches ``work1``.

    A decoy build leaves its parts in ``work1``, and an empty decoy ``MRCUI1.RRF`` sits next to
    ``MRCUI[1].RRF``. This build reads only its own parts and its own MRCUI (every read is literal),
    so it builds the expected rows.
    """
    decoy = write_inputs(tmp_path / "decoy", {"chunk_001": CHUNK_A_ROWS}, KG2_RECORDS[:2])
    decoy["work_dir"] = tmp_path / "work1"
    assert main(argv_for(decoy, len(CHUNK_A_ROWS), "--keep-work")) == 0
    inputs["work_dir"] = tmp_path / "work[1]"
    inputs["mrcui"] = inputs["mrcui"].rename(inputs["mrcui"].with_name("MRCUI[1].RRF"))
    inputs["mrcui"].with_name("MRCUI1.RRF").write_text("")
    assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    assert set(output_rows(inputs)) == EXPECTED_ROWS
    assert not inputs["work_dir"].exists()


def test_an_output_that_is_one_of_the_builds_code_files_is_refused_through_the_build(
    inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """``--out-manifest`` at one of the build's own source files is refused at preflight.

    A stale part is added once the build runs, so that, were the preflight to let the output
    through, the build would still stop before it publishes anything over the source file.
    """
    code_file = REPO_ROOT / "src/translator_ingest/ingests/semmeddb/analysis/kg2_records.py"
    before = code_file.read_bytes()
    inputs["out_manifest"] = code_file
    pairs = inputs["work_dir"] / "pairs" / "bucket_000"
    on_log_message(
        KEPT_GROUPS_MESSAGE,
        lambda: shutil.copyfile(sorted(pairs.glob("*.parquet"))[0], pairs / "stale.parquet"),
        request,
    )
    with pytest.raises(ValueError, match="unsafe output paths: .*is the build's code file"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert code_file.read_bytes() == before
    assert not list(code_file.parent.glob(f"*{LOCK_SUFFIX}"))


def test_outputs_resolved_at_preflight_ignore_a_symlink_moved_later(
    inputs: dict[str, Path], tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    """``current`` points at release A at preflight and at release B before the publish.

    The build locks, stages and publishes only the paths it resolved at preflight, so both outputs
    land in release A, which it holds the locks for, and release B gets nothing.
    """
    release_a, release_b = tmp_path / "release_a", tmp_path / "release_b"
    release_a.mkdir()
    release_b.mkdir()
    current = symlink(tmp_path / "current", release_a)
    inputs["out_parquet"], inputs["out_manifest"] = current / "out.parquet", current / "out.manifest.json"

    def move_current() -> None:
        current.unlink()
        current.symlink_to(release_b)

    on_log_message(KEPT_GROUPS_MESSAGE, move_current, request)
    assert main(argv_for(inputs, TOTAL_ROWS)) == 0
    assert sorted(path.name for path in release_a.iterdir()) == ["out.manifest.json", "out.parquet"]
    assert not list(release_b.iterdir())


def test_an_output_directory_replaced_before_the_publish_stops_the_build(
    inputs: dict[str, Path], tmp_path: Path, request: pytest.FixtureRequest
) -> None:
    """The directory at an output path is replaced (moved away, a new one made) while the build runs.

    The build checks, just before it publishes, that each output directory is the one it identified,
    so it stops and publishes into neither directory.
    """
    release = tmp_path / "release"
    inputs["out_parquet"], inputs["out_manifest"] = release / "out.parquet", release / "out.manifest.json"
    moved = tmp_path / "moved"

    def replace_release() -> None:
        release.rename(moved)
        release.mkdir()

    on_log_message(KEPT_GROUPS_MESSAGE, replace_release, request)
    with pytest.raises(ValueError, match="output directories replaced while the build ran"):
        main(argv_for(inputs, TOTAL_ROWS))
    for directory in (release, moved):
        assert not {"out.parquet", "out.manifest.json"} & {path.name for path in directory.iterdir()}


@pytest.mark.parametrize("change", ["added", "removed"])
def test_an_export_file_added_or_removed_while_the_build_ran_stops_it(
    change: str, inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """The export dir is listed once; listed again before the publish, a new or missing file stops the build."""
    export = inputs["semmeddb_dir"]

    def change_export() -> None:
        if change == "added":
            (export / "chunk_009.status").write_text("1\n")
        else:
            (export / "chunk_003.status").unlink()

    on_log_message(KEPT_GROUPS_MESSAGE, change_export, request)
    with pytest.raises(ValueError, match=f"added or removed while the build ran: .*{change} \\[.*status"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert not inputs["out_parquet"].exists() and not inputs["out_manifest"].exists()


# The build's log message once it has re-hashed every input, just before the last export listing.
REHASHED_MESSAGE = "re-hashed "


def test_an_export_file_added_during_the_rehash_stops_the_build(
    inputs: dict[str, Path], request: pytest.FixtureRequest
) -> None:
    """The export dir is listed once more after the long re-hash, so a file made during it is seen too."""
    partial = inputs["semmeddb_dir"] / "chunk_009.tsv.zst.part"
    on_log_message(REHASHED_MESSAGE, lambda: partial.write_bytes(b"an export still running"), request)
    with pytest.raises(ValueError, match=f"added or removed while the build ran: added .*{re.escape(partial.name)}"):
        main(argv_for(inputs, TOTAL_ROWS))
    assert not inputs["out_parquet"].exists() and not inputs["out_manifest"].exists()
    assert not leftover_build_files(inputs)
