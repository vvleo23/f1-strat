from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path

from f1_pipeline.persistence import file_lock


class FileLockTest(unittest.TestCase):
    def test_serializes_concurrent_critical_sections(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / "shared.parquet"
            order: list[str] = []

            def hold(name: str, hold_seconds: float) -> None:
                with file_lock(target, timeout_seconds=5.0):
                    order.append(f"{name}-start")
                    time.sleep(hold_seconds)
                    order.append(f"{name}-end")

            first = threading.Thread(target=hold, args=("first", 0.2))
            second = threading.Thread(target=hold, args=("second", 0.0))
            first.start()
            time.sleep(0.05)  # let "first" acquire the lock before "second" tries
            second.start()
            first.join()
            second.join()

            # "second" must never start while "first" still holds the lock.
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
