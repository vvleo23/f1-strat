from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from f1_pipeline.dashboard.read_models import (
    DashboardDataError,
    available_seasons,
    load_master_table,
    load_session_bundle,
)


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class DashboardReadModelsTest(unittest.TestCase):
    def test_reads_authoritative_master_table(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_dir = root / "manifests"
            manifest_dir.mkdir()
            meeting_path = root / "meeting.parquet"
            pd.DataFrame([{"meeting_id": "openf1:meeting:7"}]).to_parquet(
                meeting_path, index=False
            )
            (manifest_dir / "master_data_2026.json").write_text(
                json.dumps(
                    {
                        "season": 2026,
                        "validation": {"status": "valid"},
                        "tables": {
                            "meeting": {"path": str(meeting_path), "row_count": 1}
                        },
                    }
                ),
                encoding="utf-8",
            )

            self.assertEqual(available_seasons(manifest_dir), (2026,))
            frame = load_master_table(2026, "meeting", manifest_dir=manifest_dir)
            self.assertEqual(frame.iloc[0]["meeting_id"], "openf1:meeting:7")
            manifest = json.loads(
                (manifest_dir / "master_data_2026.json").read_text(encoding="utf-8")
            )
            manifest["tables"]["meeting"]["sha256"] = file_hash(meeting_path)
            (manifest_dir / "master_data_2026.json").write_text(
                json.dumps(manifest), encoding="utf-8"
            )
            meeting_path.write_bytes(b"corrupt")
            with self.assertRaisesRegex(DashboardDataError, "hash check"):
                load_master_table(2026, "meeting", manifest_dir=manifest_dir)

    def test_selects_latest_matching_manifest_and_checks_hash(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_dir = root / "manifests"
            session_dir = manifest_dir / "openf1_sessions"
            session_dir.mkdir(parents=True)
            old_path = root / "old.parquet"
            current_path = root / "current.parquet"
            pd.DataFrame([{"position": 2}]).to_parquet(old_path, index=False)
            pd.DataFrame([{"position": 1}]).to_parquet(current_path, index=False)

            for name, retrieved_at, data_path in (
                    ("old", "2026-08-01T00:00:00Z", old_path),
                    ("current", "2026-08-02T00:00:00Z", current_path),
            ):
                (session_dir / f"session_42_{name}.json").write_text(
                    json.dumps(
                        {
                            "session_key": 42,
                            "status": "available",
                            "endpoints": {
                                "session_result": {
                                    "status": "available",
                                    "retrieved_at": retrieved_at,
                                    "silver_path": str(data_path),
                                    "silver_sha256": file_hash(data_path),
                                }
                            },
                        }
                    ),
                    encoding="utf-8",
                )

            bundle = load_session_bundle(
                42, ("session_result",), manifest_dir=manifest_dir
            )
            self.assertEqual(bundle.frames["session_result"].iloc[0]["position"], 1)
            current_path.write_bytes(b"corrupt")
            with self.assertRaisesRegex(DashboardDataError, "hash check"):
                load_session_bundle(42, ("session_result",), manifest_dir=manifest_dir)

    def test_reports_optional_missing_data_as_partial(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            manifest_dir = root / "manifests"
            session_dir = manifest_dir / "openf1_sessions"
            session_dir.mkdir(parents=True)
            laps_path = root / "laps.parquet"
            pd.DataFrame([{"lap_number": 1}]).to_parquet(laps_path, index=False)
            (session_dir / "session_42_only.json").write_text(
                json.dumps(
                    {
                        "session_key": 42,
                        "status": "available",
                        "endpoints": {
                            "laps": {
                                "status": "available",
                                "retrieved_at": "2026-08-01T00:00:00Z",
                                "silver_path": str(laps_path),
                                "silver_sha256": file_hash(laps_path),
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )

            bundle = load_session_bundle(
                42,
                ("laps",),
                optional_endpoints=("weather",),
                manifest_dir=manifest_dir,
            )
            self.assertEqual(bundle.status, "partial")
            self.assertEqual(bundle.missing, ("weather",))


if __name__ == "__main__":
    unittest.main()
