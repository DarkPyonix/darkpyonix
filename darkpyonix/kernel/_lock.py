"""One live kernel per file per machine (FR-K3): a non-blocking exclusive OS lock.

Standard library only; Python 3.8+. The lock lives on an open file descriptor, so the OS
releases it when the process dies for any reason, including ``kill -9``.
"""
from __future__ import annotations

import os
from typing import Optional

from darkpyonix import _home


def lock_path(kernel_id: str) -> str:
    """``<home>/locks/<kernel_id>.lock`` (PROTOCOL §1)."""
    return os.path.join(_home.locks_dir(), kernel_id + ".lock")


class FileLock:
    """Non-blocking exclusive lock on ``path``; the descriptor stays open while held."""

    def __init__(self, path: str) -> None:
        self.path = path
        self._fd = None  # type: Optional[int]

    @property
    def held(self) -> bool:
        return self._fd is not None

    def acquire(self) -> bool:
        """Take the lock without blocking. Return False if another process holds it."""
        if self._fd is not None:
            return True
        fd = os.open(self.path, os.O_RDWR | os.O_CREAT, 0o600)
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            return False
        self._fd = fd
        return True

    def release(self) -> None:
        fd, self._fd = self._fd, None
        if fd is None:
            return
        try:
            if os.name == "nt":
                import msvcrt
                os.lseek(fd, 0, os.SEEK_SET)
                msvcrt.locking(fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_UN)
        except OSError:
            pass
        finally:
            os.close(fd)

    def __enter__(self) -> "FileLock":
        return self

    def __exit__(self, *exc) -> None:
        self.release()
