from __future__ import annotations

import contextlib
import errno
import hashlib
import json
import os
import socket
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

import pandas as pd


def atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".json",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(payload, temporary, indent=2, ensure_ascii=False)
            temporary.write("\n")
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".parquet",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
        frame.to_parquet(temporary_path, index=False)
        pd.read_parquet(temporary_path)
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _process_alive(pid: int) -> bool:
    if os.name == "nt":
        import ctypes
        from ctypes import wintypes

        kernel = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel.OpenProcess.argtypes = (wintypes.DWORD, wintypes.BOOL, wintypes.DWORD)
        kernel.OpenProcess.restype = wintypes.HANDLE
        kernel.WaitForSingleObject.argtypes = (wintypes.HANDLE, wintypes.DWORD)
        kernel.WaitForSingleObject.restype = wintypes.DWORD
        kernel.CloseHandle.argtypes = (wintypes.HANDLE,)
        handle = kernel.OpenProcess(0x00100000, False, pid)
        if not handle:
            return ctypes.get_last_error() != 87
        try:
            return kernel.WaitForSingleObject(handle, 0) != 0
        finally:
            kernel.CloseHandle(handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        pass
    return True


def _lock_owner(lock_path: Path) -> dict[str, Any]:
    try:
        owner = json.loads(lock_path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return owner if isinstance(owner, dict) else {}


def _abandoned_lock(lock_path: Path) -> bool:
    owner = _lock_owner(lock_path)
    pid = owner.get("pid")
    return (
        owner.get("host") == socket.gethostname()
        and type(pid) is int
        and pid > 0
        and not _process_alive(pid)
    )


def file_lock_active(path: Path) -> bool:
    lock_path = path.with_suffix(f"{path.suffix}.lock")
    return lock_path.exists() and not _abandoned_lock(lock_path)


@contextlib.contextmanager
def _lock_guard(path: Path, *, timeout_seconds: float) -> Iterator[None]:
    deadline = time.monotonic() + timeout_seconds
    with path.open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if stream.tell() == 0:
            stream.write(b"\0")
            stream.flush()
        while True:
            stream.seek(0)
            try:
                if os.name == "nt":
                    import msvcrt

                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError as exc:
                if exc.errno not in {errno.EACCES, errno.EAGAIN, errno.EDEADLK}:
                    raise
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Could not acquire lock guard for {path}.") from exc
                time.sleep(0.05)
        try:
            yield
        finally:
            stream.seek(0)
            if os.name == "nt":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


@contextlib.contextmanager
def file_lock(path: Path, *, timeout_seconds: float = 30.0) -> Iterator[None]:
    """Serialize concurrent access to a shared file via an exclusive marker file.

    Two independent pipeline jobs (for example, different race weekends in the
    same season) can otherwise read-modify-write the same season-level
    dimension file at the same time; whichever job writes last silently
    discards the other job's update. This mirrors the polling exclusive-create
    lock already used by ``geometry.py::write_geometry_record`` so every
    shared-file writer in the pipeline uses the same, well-tested pattern.
    """
    lock_path = path.with_suffix(f"{path.suffix}.lock")
    guard_path = lock_path.with_suffix(f"{lock_path.suffix}.guard")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + timeout_seconds
    token = uuid.uuid4().hex
    owner = {"pid": os.getpid(), "host": socket.gethostname(), "token": token}
    while True:
        with _lock_guard(guard_path, timeout_seconds=max(0.0, deadline - time.monotonic())):
            if lock_path.exists() and _abandoned_lock(lock_path):
                lock_path.unlink()
            try:
                descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                descriptor = None
            if descriptor is not None:
                try:
                    with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                        json.dump(owner, stream)
                except BaseException:
                    lock_path.unlink(missing_ok=True)
                    raise
                break
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Could not acquire lock for {path} within {timeout_seconds}s.")
        time.sleep(0.05)
    try:
        yield
    finally:
        with _lock_guard(guard_path, timeout_seconds=30.0):
            if _lock_owner(lock_path).get("token") == token:
                lock_path.unlink(missing_ok=True)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
