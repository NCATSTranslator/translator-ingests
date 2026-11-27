"""Tests for the work dir record of the text-location build (``analysis/work_dir.py``).

The build's own cleanup runs end to end in ``test_build_text_locations.py``; these tests pin what
``WorkFiles`` records and removes, with real files in ``tmp_path``.
"""

import logging
import os
import shutil
from pathlib import Path

import polars as pl
import pytest

from translator_ingest.ingests.semmeddb.analysis.work_dir import WORK_DIR_MARKER, Created, WorkFiles

PART = pl.DataFrame({"pmid": [1, 2]})


def test_a_claimed_work_dir_cannot_be_claimed_again(tmp_path: Path) -> None:
    """The work dir is created exclusively, so a second build on it stops."""
    WorkFiles.claim(tmp_path / "work")
    with pytest.raises(FileExistsError):
        WorkFiles.claim(tmp_path / "work")


def test_remove_takes_everything_the_build_created(tmp_path: Path) -> None:
    """Parts written through ``WorkFiles``, the directories made for them and the work dir all go."""
    work_files = WorkFiles.claim(tmp_path / "work")
    work_files.write_parquet(PART, tmp_path / "work" / "pairs" / "bucket_000" / "part_00000.parquet")
    work_files.write_parquet(PART, tmp_path / "work" / "output" / "bucket_000.parquet")
    work_files.remove()
    assert not list(tmp_path.iterdir())


def test_remove_keeps_a_directory_the_build_did_not_create(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    """A directory that already existed when the build wrote into it is not the build's, so it stays."""
    work_files = WorkFiles.claim(tmp_path / "work")
    (tmp_path / "work" / "output").mkdir()
    work_files.write_parquet(PART, tmp_path / "work" / "output" / "bucket_000.parquet")
    with caplog.at_level(logging.WARNING):
        work_files.remove()
    assert sorted(path.relative_to(tmp_path).as_posix() for path in tmp_path.rglob("*")) == ["work", "work/output"]
    assert "holds what the build did not write" in caplog.text


def test_remove_keeps_a_file_put_at_one_of_the_builds_names(tmp_path: Path) -> None:
    """A part replaced by another file under the same name (a new inode) is not the build's, so it stays."""
    work_files = WorkFiles.claim(tmp_path / "work")
    part = tmp_path / "work" / "output" / "bucket_000.parquet"
    work_files.write_parquet(PART, part)
    other = tmp_path / "other.parquet"
    other.write_text("another build")
    other.replace(part)
    work_files.remove()
    assert part.read_text() == "another build"


def test_a_claimed_work_dir_is_marked_until_it_is_removed(tmp_path: Path) -> None:
    """The marker that keeps other builds' outputs out is there from the claim on and goes with the work dir."""
    work_files = WorkFiles.claim(tmp_path / "work")
    assert (tmp_path / "work" / WORK_DIR_MARKER).is_file()
    work_files.remove()
    assert not list(tmp_path.iterdir())


def test_check_unchanged_passes_until_a_written_file_changes(tmp_path: Path) -> None:
    """Untouched parts pass; a part replaced by a copy (a new inode) or grown in place stops the check."""
    work_files = WorkFiles.claim(tmp_path / "work")
    parts = [tmp_path / "work" / "output" / f"bucket_00{number}.parquet" for number in range(2)]
    for part in parts:
        work_files.write_parquet(PART, part)
    work_files.check_unchanged()
    copy = tmp_path / "copy.parquet"
    shutil.copyfile(parts[0], copy)
    os.replace(copy, parts[0])
    with pytest.raises(ValueError, match="1 work dir files changed after the build wrote them"):
        work_files.check_unchanged()
    with parts[1].open("ab") as handle:
        handle.write(b"more")
    with pytest.raises(ValueError, match="2 work dir files changed after the build wrote them"):
        work_files.check_unchanged()


def test_a_part_is_never_written_over_a_file_already_at_its_name(tmp_path: Path) -> None:
    """Writing a part where another file already is fails, and that file keeps its bytes."""
    work_files = WorkFiles.claim(tmp_path / "work")
    part = tmp_path / "work" / "output" / "bucket_000.parquet"
    part.parent.mkdir()
    part.write_bytes(b"another build")
    with pytest.raises(FileExistsError):
        work_files.write_parquet(PART, part)
    assert part.read_bytes() == b"another build"
    assert sorted(path.name for path in part.parent.iterdir()) == ["bucket_000.parquet"]


def test_parts_are_only_the_files_the_build_wrote(tmp_path: Path) -> None:
    """``parts`` lists the build's own files in a dir and refuses a dir that holds anything else."""
    work_files = WorkFiles.claim(tmp_path / "work")
    bucket = tmp_path / "work" / "pairs" / "bucket_000"
    for name in ("part_00001.parquet", "part_00000.parquet"):
        work_files.write_parquet(PART, bucket / name)
    assert work_files.parts(bucket) == [bucket / "part_00000.parquet", bucket / "part_00001.parquet"]
    assert not work_files.parts(tmp_path / "work" / "pairs" / "bucket_001")
    (bucket / "stale.parquet").write_bytes(b"stale")
    with pytest.raises(ValueError, match="holds files the build did not write"):
        work_files.parts(bucket)


def test_a_part_is_recorded_as_written_and_confirmed_under_its_name(tmp_path: Path) -> None:
    """The part's identity is taken when it is written and must be what its name holds after the link.

    A file put at the name afterwards is a different inode, so the same confirmation fails for it.
    """
    work_files = WorkFiles.claim(tmp_path / "work")
    part = tmp_path / "work" / "output" / "bucket_000.parquet"
    work_files.write_parquet(PART, part)
    recorded = work_files.created[-1]
    assert recorded == Created.now(part)
    recorded.confirm()
    copy = tmp_path / "copy.parquet"
    shutil.copyfile(part, copy)
    os.replace(copy, part)
    with pytest.raises(ValueError, match="is not the file the build just wrote"):
        recorded.confirm()
