from __future__ import annotations

import tempfile
import threading
import os
import unittest
from pathlib import Path
from unittest.mock import patch

from f1_pipeline.persistence import file_lock


class FileLockTest(unittest.TestCase):
    def test_serializes_concurrent_critical_sections(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "shared.parquet"
            order: list[str] = []

            entered = threading.Event()
            contended = threading.Event()
            release = threading.Event()
            errors = []
            original_open = os.open

            def observed_open(*args, **kwargs):
                try:
                    return original_open(*args, **kwargs)
                except FileExistsError:
                    contended.set()
                    raise

            def hold(name: str) -> None:
                try:
                    with file_lock(target, timeout_seconds=3.0):
                        order.append(f"{name}-start")
                        if name == "first":
                            entered.set()
                            if not release.wait(3.0):
                                raise TimeoutError("First lock was not released")
                        order.append(f"{name}-end")
                except Exception as exc:
                    errors.append(exc)

            first = threading.Thread(target=hold, args=("first",))
            second = threading.Thread(target=hold, args=("second",))
            with patch("f1_pipeline.persistence.os.open", side_effect=observed_open):
                first.start()
                try:
                    self.assertTrue(entered.wait(2.0))
                    second.start()
                    self.assertTrue(contended.wait(2.0))
                finally:
                    release.set()
                    first.join(4.0)
                    if second.ident is not None:
                        second.join(4.0)

            self.assertFalse(first.is_alive() or second.is_alive())
            self.assertEqual(errors, [])
            self.assertEqual(
                order, ["first-start", "first-end", "second-start", "second-end"]
            )
            self.assertFalse(target.with_suffix(".parquet.lock").exists())

    def test_times_out_when_lock_is_held(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "shared.parquet"
            with file_lock(target, timeout_seconds=5.0):
                with self.assertRaises(TimeoutError):
                    with file_lock(target, timeout_seconds=0.2):
                        pass  # pragma: no cover - must time out before entering

    def test_releases_lock_even_when_the_critical_section_raises(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "shared.parquet"
            with self.assertRaises(ValueError):
                with file_lock(target, timeout_seconds=5.0):
                    raise ValueError("boom")
            # A fresh lock must be acquirable immediately afterward.
            with file_lock(target, timeout_seconds=1.0):
                pass


if __name__ == "__main__":
    unittest.main()
