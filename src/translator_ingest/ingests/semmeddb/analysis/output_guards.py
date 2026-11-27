"""Keep the text-location build from destroying its inputs, colliding with another build, or
leaving a parquet and a manifest that do not belong together.

The build replaces its outputs, deletes their locks and deletes its work dir at the end.
``ResolvedOutputs`` first resolves each output once, following its symlinks (a dangling one as far
as it leads); every rule below is checked on those paths, and only those paths are locked, staged
and published. ``refuse_unsafe_outputs`` runs before anything is written and refuses an output that
is the same file as an input, as one of the build's code files or as the other output, that lies
inside the export dir or the work dir, that is, is named like or lies inside an output lock, that is
a directory, that lies inside another build's marked work dir, or that would contain the other
output, and a work dir and export dir that lie inside one another. Files are compared by the
filesystem (``os.path.samefile``: case and normalization aliases, symlinks, hard links,
firmlinks). A directory that does not exist yet, such as the work dir, cannot be compared that
way, so paths are also compared resolved and ignoring case and Unicode normalization, which on a
case-sensitive filesystem refuses a few safe paths. Only one of these rules is checked again
later: that no output lies inside another build's marked work dir, once the locks are taken and
just before the publish. ``output_locks`` claims an exclusive lock next to each output, so two
builds never write the same output, and ``staged_outputs`` writes each output under a temporary
name next to it and moves both into place only when the build has finished, putting the old
parquet back if the manifest cannot be moved into place, with SIGINT, SIGTERM and SIGHUP held back
until the pair is consistent, and stopping if an output directory was replaced in the meantime.

It lives in ``analysis/`` with the build script, so it does not change the transform version.
"""

import os
import signal
import unicodedata
import uuid
from collections.abc import Iterator, Mapping, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from itertools import combinations, permutations
from pathlib import Path
from types import FrameType
from typing import Final

from translator_ingest.ingests.semmeddb.analysis.work_dir import WORK_DIR_MARKER

LOCK_SUFFIX = ".lock"
# Suffix of the hidden file an output is written to before it is moved into place.
STAGED_SUFFIX = ".partial"
# Paths are compared in this form both before and after case folding.
CANONICAL_FORM: Final = "NFD"
# Signals that would stop a build between its two renames; publishing holds them back (those this
# platform has).
DEFERRED_SIGNALS: tuple[signal.Signals, ...] = tuple(
    signal.Signals[name] for name in ("SIGINT", "SIGTERM", "SIGHUP") if hasattr(signal, name)
)


def same_file(path: Path, other: Path) -> bool:
    """Return whether both paths exist and are one file, as the filesystem sees it (case, links)."""
    return path.exists() and other.exists() and os.path.samefile(path, other)


def path_key(path: Path) -> str:
    """Return the resolved path's text under Unicode canonical caseless matching.

    Two paths with equal keys can name one file on a case- or normalization-insensitive filesystem,
    such as the macOS default, even before the file exists.

    >>> path_key(Path("/data/\\u00c9TUDE")) == path_key(Path("/data/e\\u0301tude"))
    True
    """
    text = unicodedata.normalize(CANONICAL_FORM, str(path.resolve()))
    return unicodedata.normalize(CANONICAL_FORM, text.casefold())


def is_inside(path: Path, directory: Path) -> bool:
    """Return whether ``path``, resolved through its symlinks, is ``directory`` or below it.

    An ancestor of the resolved path matches when it is the same file as ``directory`` (which also
    sees aliases that resolving keeps apart, such as macOS firmlinks) or has the same ``path_key``
    (which also works for a directory that does not exist yet).
    """
    resolved = path.resolve()
    directory_key = path_key(directory)
    return any(
        same_file(parent, directory) or path_key(parent) == directory_key for parent in (resolved, *resolved.parents)
    )


def identity(path: Path) -> tuple[int, int]:
    """Return the device and inode of ``path`` itself, not following a symlink."""
    info = path.lstat()
    return info.st_dev, info.st_ino


def marked_work_dir(path: Path) -> Path | None:
    """Return the nearest directory at or above ``path``, taken as it is, that holds ``WORK_DIR_MARKER``.

    Such a directory is the work dir of a build that is running, or that failed and left it; its
    cleanup could delete an output there, and its matching could read one as its own part.
    """
    return next(
        (directory for directory in (path, *path.parents) if os.path.lexists(directory / WORK_DIR_MARKER)),
        None,
    )


def live_work_dir_problems(paths: Sequence[Path]) -> list[str]:
    """Return one problem for each of the (resolved) ``paths`` that lies inside a marked work dir."""
    return [
        f"{path} is inside {work_dir}, the work dir of a running or failed build"
        for path in paths
        if (work_dir := marked_work_dir(path)) is not None
    ]


def refuse_outputs_in_live_work_dirs(paths: Sequence[Path]) -> None:
    """Raise ValueError if one of the resolved output ``paths`` lies inside a marked work dir.

    ``refuse_unsafe_outputs`` checks every rule before the build starts. Another build may claim a
    work dir around an output after that, so this one rule, and only this one, is checked again
    once the locks are taken and again just before the publish moves anything into place.
    """
    problems = live_work_dir_problems(paths)
    if problems:
        raise ValueError(f"unsafe output paths: {'; '.join(problems)}")


def refuse_unsafe_outputs(
    outputs: "ResolvedOutputs", inputs: Sequence[Path], directories: Mapping[str, Path], code: Sequence[Path] = ()
) -> None:
    """Raise ValueError when an output could destroy an input, another output or what a directory holds.

    Every rule is checked on ``outputs.paths``, the paths resolved at preflight, which are the ones
    the build will lock, stage and publish. Refused: two outputs that are one file or have the same
    ``path_key``, an output that is the same file as, or has the same ``path_key`` as, any of
    ``inputs`` or the build's ``code`` files (its own source, which it would replace), an output
    inside any of ``directories``, which maps a command-line flag to its directory, two of
    ``directories`` inside or equal to one another (a work dir inside the export dir would change
    the export, and the export dir cannot lie inside a work dir the build creates), an output whose
    name ends with ``LOCK_SUFFIX`` (it could be an output's lock, which ``output_locks`` deletes, or
    another build's) or that is an output's lock file, an output that is a directory (a staged file
    cannot replace it), an output inside a ``marked_work_dir`` (another build's, which could delete
    or read it), an output named like ``WORK_DIR_MARKER`` (it would mark its directory as a work
    dir), an output that would contain the other output, and a directory of ``directories`` inside
    or equal to an output or an output's lock: creating the directory for a lock, or the work dir,
    would turn that path into a directory. The locks and staged files lie next to their output, so
    an output that would contain one of them contains that output too, and the staged and backup
    files have unique names created before the work dir. ``same_file`` compares the files symlinks
    lead to, and ``path_key`` and ``is_inside`` resolve the paths first.
    """
    paths = outputs.paths
    problems = [
        f"{first} and {second} are one path"
        for first, second in combinations(paths, 2)
        if same_file(first, second) or path_key(first) == path_key(second)
    ]
    problems += [
        f"{flag} {path} is inside or equal to {other_flag} {other}"
        for (flag, path), (other_flag, other) in permutations(directories.items(), 2)
        if is_inside(path, other)
    ]
    locks = [lock_beside(path) for path in paths]
    for index, output in enumerate(paths):
        if any(is_inside(other, output) for other in [*paths[:index], *paths[index + 1 :]]):
            problems.append(f"{output} would contain another output")
        output_key = path_key(output)
        problems += [
            f"{output} is the {role} {source}"
            for role, protected in (("input", inputs), ("build's code file", code))
            for source in protected
            if same_file(output, source) or path_key(source) == output_key
        ]
        problems += [
            f"{output} is inside {flag} {path}" for flag, path in directories.items() if is_inside(output, path)
        ]
        if path_key(output).endswith(LOCK_SUFFIX) or any(same_file(output, lock) for lock in locks):
            problems.append(f"{output} is or is named like an output lock, which the build deletes")
        if any(is_inside(output, lock) for lock in locks):
            problems.append(f"{output} is inside an output's lock path, which the build creates as a file")
        if output.is_dir():
            problems.append(f"{output} is a directory")
        if output.name.casefold() == WORK_DIR_MARKER.casefold():
            problems.append(f"{output} is named like the work dir marker, which would block later builds")
    problems += live_work_dir_problems(paths)
    problems += [
        f"{flag} {path} is inside or equal to the output or lock {target}"
        for flag, path in directories.items()
        for target in [*paths, *locks]
        if is_inside(path, target)
    ]
    if problems:
        raise ValueError(f"unsafe output paths: {'; '.join(problems)}")


def lock_beside(path: Path) -> Path:
    """Return the lock file of the resolved output ``path``: ``<path>.lock`` next to it."""
    return path.with_name(path.name + LOCK_SUFFIX)


def remove_if_same(path: Path, created: tuple[int, int]) -> None:
    """Remove ``path`` if it still is the file created as ``created`` (device, inode), else leave it."""
    if os.path.lexists(path) and identity(path) == created:
        path.unlink()


@dataclass(frozen=True)
class ResolvedOutputs:
    """The outputs, resolved once at preflight, and the identity of each one's directory.

    The locks, the staged files and the publish use only these paths and never resolve again, so a
    symlink moved while the build runs cannot send the outputs elsewhere. A directory that exists
    at preflight is identified then, and one that does not is identified when the locks create it;
    ``check_directories`` stops the build if a directory at one of these paths is no longer the one
    identified, which means it was replaced.
    """

    paths: tuple[Path, ...]
    directories: dict[Path, tuple[int, int]]

    @classmethod
    def at_preflight(cls, outputs: Sequence[Path]) -> "ResolvedOutputs":
        """Resolve ``outputs`` and identify the directories of theirs that exist now."""
        paths = tuple(output.resolve() for output in outputs)
        return cls(paths, {path.parent: identity(path.parent) for path in paths if path.parent.is_dir()})

    def identify_new_directories(self) -> None:
        """Check the directories identified so far, then identify those created since the preflight."""
        self.check_directories()
        for path in self.paths:
            self.directories.setdefault(path.parent, identity(path.parent))

    def check_directories(self) -> None:
        """Raise ValueError if a directory at one of the output paths is not the one identified."""
        replaced = [
            str(directory)
            for directory, created in self.directories.items()
            if not os.path.lexists(directory) or identity(directory) != created
        ]
        if replaced:
            raise ValueError(f"output directories replaced while the build ran: {replaced}")


@contextmanager
def output_locks(outputs: ResolvedOutputs) -> Iterator[None]:
    """Hold an exclusive lock next to each resolved output until the block ends, failed or not.

    Each lock is created with ``open(..., "x")``, so a build whose outputs another running build
    writes, under any path that resolves to the same lock, stops at once with FileExistsError naming
    the lock, before it writes anything. A lock is set to be removed as soon as it is created, so
    one whose write fails (a full disk, say) is removed too, and it is removed only while its path
    still holds it. Once every lock is taken, the output directories created for them are
    identified, and an output found inside another build's live work dir stops the build
    (``refuse_outputs_in_live_work_dirs``), releasing the locks. A lock left by a killed build must
    be deleted by hand.
    """
    with ExitStack() as held:
        for path in outputs.paths:
            lock = lock_beside(path)
            lock.parent.mkdir(parents=True, exist_ok=True)
            with lock.open("x", encoding="utf-8") as handle:
                held.callback(remove_if_same, lock, identity(lock))
                handle.write(f"text-location build, pid {os.getpid()}\n")
        outputs.identify_new_directories()
        refuse_outputs_in_live_work_dirs(outputs.paths)
        yield


def staged_path(output: Path) -> Path:
    """Return a new, unique hidden name next to ``output`` to write it under before it is moved into place."""
    return output.with_name(f".{output.name}.{uuid.uuid4().hex}{STAGED_SUFFIX}")


def restore(backup: Path, output: Path) -> None:
    """Put an output's backup back in its place and drop the backup's name.

    The rename is atomic, so the output path never disappears. When the output still is the backed
    up file (its replacement failed), the rename leaves both names, so the backup name is removed.
    """
    os.replace(backup, output)
    backup.unlink(missing_ok=True)


@contextmanager
def deferred_signals() -> Iterator[None]:
    """Hold back ``DEFERRED_SIGNALS`` until the block has ended, then deliver them to the old handlers.

    A Python-level handler records each signal instead of acting on it; when the block ends, failed
    or not, the previous handlers are put back and each recorded signal is raised again, so the
    default SIGTERM action ends the process only then. ``signal.pthread_sigmask`` would not do:
    it blocks a signal for the calling thread only, and a signal sent to the process goes to
    another thread that does not block it, such as one of polars' worker threads, whose default
    action ends the whole process. Holds nest: an inner one raises its signals again into the outer
    one, so they wait until the outermost hold ends; the build holds them from the end of the build
    until its locks are released. This does not cover SIGKILL or a power loss. A signal whose
    handler was set outside Python is left alone. It must run in the main thread, as
    ``signal.signal`` requires.
    """
    received: list[signal.Signals] = []

    def record(number: int, _frame: FrameType | None) -> None:
        received.append(signal.Signals(number))

    def deliver() -> None:
        for number in dict.fromkeys(received):
            signal.raise_signal(number)

    with ExitStack() as held:
        held.callback(deliver)
        for number in DEFERRED_SIGNALS:
            previous = signal.getsignal(number)
            if previous is not None:
                signal.signal(number, record)
                held.callback(signal.signal, number, previous)
        yield


def publishing(moves: Sequence[tuple[Path, Path]]) -> Iterator[None]:
    """Move each staged file onto its output, in order, yielding between filesystem steps.

    Each ``(staged, output)`` but the last first keeps an existing output as a hard link under a
    staged backup name, then renames the staged file onto the output, so every output path holds
    the old or the new file at every step; a reader never finds it missing. If a step fails, or the
    caller stops before the end, the steps are undone in reverse: backups go back in place and a
    new output that had no old one goes back to its staged name; an error is raised again. The last
    rename, run when the caller asks for the step after the last yield, either happens or changes
    nothing, so it is the commit; the backups are then deleted. SIGINT, SIGTERM and SIGHUP are held
    back from the first step until the backups are deleted or the steps undone
    (``deferred_signals``), so a signal never leaves a backup behind. SIGKILL or a
    power loss between the steps can still leave a new output next to an old one, and a backup or a
    staged file behind, and a reader that opens both files during the short window between the
    renames can see a new parquet with the old manifest. The outputs' filesystem must support hard
    links.
    """
    *before_last, (last_staged, last_output) = moves
    backups: list[Path] = []
    with deferred_signals(), ExitStack() as rollback:
        for staged, output in before_last:
            if output.exists():
                backup = staged_path(output)
                os.link(output, backup)
                backups.append(backup)
                rollback.callback(restore, backup, output)
                yield
                os.replace(staged, output)
            else:
                os.replace(staged, output)
                rollback.callback(os.replace, output, staged)
            yield
        os.replace(last_staged, last_output)
        rollback.pop_all()
        for backup in backups:
            backup.unlink()


def publish(moves: Sequence[tuple[Path, Path]]) -> None:
    """Run every step of ``publishing``: move each staged file onto its output, undoing all on failure."""
    for _ in publishing(moves):
        pass


@contextmanager
def staged_outputs(outputs: ResolvedOutputs) -> Iterator[list[Path]]:
    """Yield an empty staged file next to each resolved output; when the block succeeds, publish them.

    Each staged file has a unique name, is created exclusively and lies in its output's directory,
    so ``publish`` can move it into place with an atomic ``os.replace``, in the order of
    ``outputs``. A rename replaces the directory entry instead of writing into the old file, so a
    hard link to an old output keeps its content. When the block raises, or ``publish`` fails and
    undoes its renames, the staged files are removed and every output is left as it was. Just
    before publishing, a replaced output directory (``ResolvedOutputs.check_directories``) or an
    output found inside another build's live work dir (``refuse_outputs_in_live_work_dirs``) stops
    it the same way. The outputs' directories must exist; ``output_locks`` creates them.
    """
    finals = list(outputs.paths)
    with ExitStack() as staged:
        temps: list[Path] = []
        for final in finals:
            temp = staged_path(final)
            temp.touch(exist_ok=False)
            staged.callback(temp.unlink, missing_ok=True)
            temps.append(temp)
        yield temps
        outputs.check_directories()
        refuse_outputs_in_live_work_dirs(outputs.paths)
        publish(list(zip(temps, finals, strict=True)))
