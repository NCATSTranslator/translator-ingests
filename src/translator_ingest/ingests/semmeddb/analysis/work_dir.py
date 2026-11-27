"""Where the text-location build keeps its intermediates under ``--work-dir``, and how it removes them.

Each stage writes parquet parts under one subdirectory, most of them split into ``bucket_NNN``
directories by PMID. A claimed work dir holds ``WORK_DIR_MARKER`` until it is cleaned up, so another
build refuses to put an output anywhere inside it. Every directory and file the build creates there
goes through ``WorkFiles``, which records its path, identity (device and inode), size and
modification time right after creating it. A part is written under a unique temporary name and then
hard-linked to its own name, which fails if anything is already there, so the build never overwrites
a file it did not create; and the build reads exactly the parts it recorded, never a directory
listing, stopping if a part directory holds anything else. Before the build assembles its artifact
from the parts, and again just before it publishes, ``check_unchanged`` stops it if any file it
wrote is no longer as written, so rows another process put at one of its names never reach the
artifact. Cleanup
removes only what the build created: a file only while the same path still holds the same file, and
a directory only when it is also empty. Anything else, such as another build's outputs published
inside this work dir or a file put at one of the build's own names, stays, and so does the work dir,
with a warning.

It lives in ``analysis/`` with the build script, so it does not change the transform version.
"""

import logging
import os
import stat
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

import polars as pl

LOGGER = logging.getLogger("build_text_locations")

PAIRS_SUBDIR = "pairs"
PUBLICATIONS_SUBDIR = "publications"
RECORDS_SUBDIR = "records"
SEMMEDDB_SUBDIR = "semmeddb"
OUTPUT_SUBDIR = "output"
BUCKET_PREFIX = "bucket_"
# The file that marks a work dir a build is using (or a failed build left); outputs may not lie below it.
WORK_DIR_MARKER = ".semmeddb-text-locations-work"
# Suffix of the hidden name a part is written under before it is linked to its own name.
TEMPORARY_SUFFIX = ".partial"
PART_PREFIX = "part_"


def bucket_directory(root: Path, bucket: int) -> Path:
    """Return the directory holding one bucket's parquet parts under ``root``.

    >>> bucket_directory(Path("work/pairs"), 7).as_posix()
    'work/pairs/bucket_007'
    """
    return root / f"{BUCKET_PREFIX}{bucket:03d}"


class Created(NamedTuple):
    """A directory or file the build created, as it was right after: identity, size and modification time."""

    path: Path
    device: int
    inode: int
    size: int
    mtime_ns: int
    is_dir: bool

    @classmethod
    def now(cls, path: Path) -> "Created":
        """Return what ``path`` is now, without following a symlink."""
        info = path.lstat()
        return cls(path, info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, stat.S_ISDIR(info.st_mode))

    def identity(self) -> tuple[int, int]:
        """Return the device and inode, which tell one file from another."""
        return self.device, self.inode

    def state(self) -> tuple[int, int, int, int]:
        """Return the identity, size and modification time, which also tell a changed file."""
        return self.device, self.inode, self.size, self.mtime_ns

    def same_file(self) -> bool:
        """Return whether the path still names the created file or directory (same device and inode)."""
        return os.path.lexists(self.path) and Created.now(self.path).identity() == self.identity()

    def unchanged(self) -> bool:
        """Return whether the path still names the created file, with the same size and modification time."""
        return os.path.lexists(self.path) and Created.now(self.path).state() == self.state()

    def confirm(self) -> None:
        """Raise ValueError unless the path names exactly this file now (``unchanged``)."""
        if not self.unchanged():
            raise ValueError(f"{self.path} is not the file the build just wrote; another process replaced it")


@dataclass
class WorkFiles:
    """The work dir and every directory and file the build created in it, as they were when created."""

    root: Path
    created: list[Created] = field(default_factory=list)

    @classmethod
    def claim(cls, root: Path) -> "WorkFiles":
        """Create the work dir atomically, so a second build on it fails, mark it, and record both."""
        root.mkdir(parents=True, exist_ok=False)
        work_files = cls(root)
        work_files.record(root)
        marker = root / WORK_DIR_MARKER
        with marker.open("x", encoding="utf-8") as handle:
            handle.write(f"work dir of a text-location build, pid {os.getpid()}\n")
        work_files.record(marker)
        return work_files

    def record(self, path: Path) -> None:
        """Record a path the build has just created (a file once it is written), as it is now."""
        self.created.append(Created.now(path))

    def directory(self, path: Path) -> Path:
        """Return ``path``, a directory under the work dir, creating and recording each missing level."""
        level = self.root
        for part in path.relative_to(self.root).parts:
            level = level / part
            if not level.is_dir():
                level.mkdir()
                self.record(level)
        return path

    def write_parquet(self, frame: pl.DataFrame, path: Path) -> None:
        """Write ``frame`` to the new file ``path`` under the work dir, creating its directories, and record it.

        The frame goes to a unique temporary name first, whose identity, size and modification time
        are taken there, then the file is hard-linked to ``path``: the link fails with FileExistsError
        if anything is already there, so another process's file at one of the build's names is never
        overwritten and taken for the build's own. A hard link is the same file, so ``path`` must
        then be exactly what was written (``Created.confirm``), else another process replaced it
        between the link and the check and the build stops. The temporary name is removed either way.
        """
        self.directory(path.parent)
        temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}{TEMPORARY_SUFFIX}")
        frame.write_parquet(temporary)
        written = Created.now(temporary)._replace(path=path)
        try:
            os.link(temporary, path)
            written.confirm()
        finally:
            temporary.unlink()
        self.created.append(written)

    def parts(self, directory: Path) -> list[Path]:
        """Return the files the build wrote directly in ``directory``, sorted; raise if it holds anything else.

        The build reads only these, never a listing of the directory, so a stale or foreign parquet
        there cannot add rows. A directory the build never wrote into holds no parts.
        """
        written = sorted(entry.path for entry in self.created if not entry.is_dir and entry.path.parent == directory)
        foreign = sorted(set(directory.iterdir()) - set(written)) if directory.is_dir() else []
        if foreign:
            raise ValueError(f"{directory} holds files the build did not write: {foreign[:5]}")
        return written

    def check_unchanged(self) -> None:
        """Raise ValueError if a file the build wrote is not as it was written: another inode, size or mtime.

        Another process could have put its own file at one of the build's names, or changed one, after
        the build wrote it; its rows must not end up in the artifact, so the build stops instead.
        """
        changed = [entry.path for entry in self.created if not entry.is_dir and not entry.unchanged()]
        if changed:
            raise ValueError(
                f"{len(changed):,} work dir files changed after the build wrote them, the first: {changed[:5]}"
            )

    def remove(self) -> None:
        """Remove what the build created, newest first, then warn if the work dir has to stay.

        A file goes only while its path still holds the recorded file, so one another process put at
        the same name stays; a directory goes only when, in addition, it is empty. The marker, created
        right after the work dir, goes last, just before the work dir. Nothing is removed recursively.
        """
        for entry in reversed(self.created):
            if not entry.same_file():
                continue
            if not entry.is_dir:
                entry.path.unlink()
            elif not any(entry.path.iterdir()):
                entry.path.rmdir()
        if self.root.exists():
            LOGGER.warning(
                "kept %s, which holds what the build did not write: %s", self.root, sorted(self.root.rglob("*"))
            )
