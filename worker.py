"""Process identity for item claims, based on an OS file lock.

Each process holds an exclusive lock on its own file for as long as it runs. The
operating system drops the lock however the process ends, so a lock that can be
acquired means its owner is gone. A live process's claims are therefore never taken
over, while a dead process's claims can be recovered immediately.
"""

from __future__ import annotations

import os
import time
import uuid
from pathlib import Path
from typing import IO, Optional

STALE_LOCK_MIN_AGE_SECONDS = 60

if os.name == "nt":
    import msvcrt

    def _lock(handle: IO[bytes]) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)

    def _unlock(handle: IO[bytes]) -> None:
        handle.seek(0)
        msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
else:
    import fcntl

    def _lock(handle: IO[bytes]) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    def _unlock(handle: IO[bytes]) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _path(directory: Path, worker_id: str) -> Path:
    return directory / f"{worker_id}.lock"


def _remove(path: Path) -> None:
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass  # a leftover lock file is harmless


class WorkerLock:
    """Held for the lifetime of one process's JobManager."""

    def __init__(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.directory = directory
        self.id = uuid.uuid4().hex
        self._handle: Optional[IO[bytes]] = open(_path(directory, self.id), "a+b")
        _lock(self._handle)

    def release(self) -> None:
        if self._handle is None:
            return
        _unlock(self._handle)
        self._handle.close()
        self._handle = None
        _remove(_path(self.directory, self.id))


def owner_is_alive(directory: Path, worker_id: Optional[str]) -> bool:
    """False when the claim's owner has stopped. Phase 1 claims have no owner."""
    if not worker_id or not worker_id.isalnum():
        return False
    path = _path(directory, worker_id)
    try:
        handle = open(path, "r+b")
    except FileNotFoundError:
        return False
    except OSError:
        return True  # cannot tell, so leave the claim alone
    with handle:
        try:
            _lock(handle)
        except OSError:
            return True
        _unlock(handle)
    _remove(path)
    return False


def sweep_stale_locks(
    directory: Path, keep: str, min_age: float = STALE_LOCK_MIN_AGE_SECONDS
) -> int:
    """Remove lock files left by processes that have ended. Returns how many went.

    Young files are skipped: a process that has just created its file may not hold the
    lock yet, and must not lose the file or find it locked by this check.
    """
    removed = 0
    now = time.time()
    for path in directory.glob("*.lock"):
        worker_id = path.stem
        if worker_id == keep:
            continue
        try:
            if now - path.stat().st_mtime < min_age:
                continue
        except OSError:
            continue
        if not owner_is_alive(directory, worker_id) and not path.exists():
            removed += 1
    return removed
