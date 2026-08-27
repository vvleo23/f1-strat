"""Tests for reusable local circuit geometry."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from f1_pipeline.geometry import (
    TrackGeometryError,
    build_centerline,
    build_geometry_record,
    build_manifest_geometry,
    geometry_table_path,
    load_track_geometry,
    write_geometry_record,
)
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256


class GeometryTest(unittest.TestCase):
    def setUp(self) -> None:
        start = pd.Timestamp("2026-07-26T13:00:00Z")
        laps = []
        locations = []
        angles = np.linspace(0, 2 * np.pi, 21)
        for driver in (1, 2, 3):
            laps.extend(
                [
                    {
                        "driver_number": driver,
                        "lap_number": 1,
                        "date_start": start,
                        "is_pit_out_lap": False,
                    },
                    {
                        "driver_number": driver,
                        "lap_number": 2,
                        "date_start": start + timedelta(seconds=10),
                        "is_pit_out_lap": False,
                    },
                ]
            )
            for index, angle in enumerate(angles):
                locations.append(
                    {
                        "driver_number": driver,
                        "date": start + timedelta(seconds=index / 2),
                        "x": np.cos(angle) + driver * 0.01,
                        "y": np.sin(angle),
                        "z": 0.0,
                    }
                )
        self.laps = pd.DataFrame(laps)
        self.locations = pd.DataFrame(locations)

    def test_builds_closed_centerline_from_multiple_drivers(self) -> None:
        centerline, samples, quality = build_centerline(
            self.locations,
            self.laps,
            sample_laps=3,
            point_count=41,
        )

        self.assertEqual(centerline.shape, (41, 3))
        self.assertEqual(len(samples), 3)
        self.assertEqual(quality["status"], "available")
        self.assertEqual(quality["selection_basis"], "first_race_lap")
        self.assertTrue(all(sample["lap_number"] == 1 for sample in samples))
        np.testing.assert_allclose(centerline[0], centerline[-1])

    def test_ignores_double_length_orientation_candidate(self) -> None:
        angles = np.linspace(0, 4 * np.pi, 41)
        double_lap = pd.DataFrame(
            [
                {
                    "driver_number": 3,
                    "date": pd.Timestamp("2026-07-26T13:00:00Z")
                            + timedelta(seconds=index / 4),
                    "x": np.cos(angle),
                    "y": np.sin(angle),
                    "z": 0.0,
                }
                for index, angle in enumerate(angles)
            ]
        )
        normal_driver = self.locations[self.locations["driver_number"] == 1].copy()
        normal_driver["driver_number"] = 4
        locations = pd.concat(
            [self.locations[self.locations["driver_number"] != 3], double_lap, normal_driver],
            ignore_index=True,
        )
        normal_laps = self.laps[self.laps["driver_number"] == 1].copy()
        normal_laps["driver_number"] = 4
        laps = pd.concat(
            [self.laps[self.laps["driver_number"] != 3], normal_laps],
            ignore_index=True,
        )

        _, samples, quality = build_centerline(
            locations,
            laps,
            sample_laps=3,
            point_count=41,
        )

        self.assertEqual(quality["selection_basis"], "first_race_lap")
        self.assertEqual(len(samples), 3)
        self.assertNotIn(3, [sample["driver_number"] for sample in samples])

    def test_manifest_geometry_uses_verified_inputs_and_season_partition(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raw_paths = {
                "sessions": root / "raw" / "sessions.parquet",
                "laps": root / "raw" / "laps.parquet",
                "location": root / "raw" / "location.parquet",
            }
            atomic_parquet(pd.DataFrame([{"session_key": 42}]), raw_paths["sessions"])
            atomic_parquet(self.laps, raw_paths["laps"])
            atomic_parquet(self.locations, raw_paths["location"])
            manifest = {
                "session_key": 42,
                "manifest_path": str(root / "session.json"),
                "manifest_sha256": "session-hash",
                "endpoints": {
                    name: {
                        "status": "available",
                        "raw_path": str(path),
                        "raw_sha256": sha256(path),
                        "retrieved_at": "2025-06-01T12:00:00+00:00",
                    }
                    for name, path in raw_paths.items()
                },
            }
            curated_dir = root / "curated"
            master_manifest_path = curated_dir / "manifests" / "master_data_2025.json"
            atomic_json({"tables": {}}, master_manifest_path)

            result, manifest_path = build_manifest_geometry(
                manifest,
                season=2025,
                meeting_key=10,
                circuit_id="openf1:circuit:7",
                curated_dir=curated_dir,
            )
            geometry = load_track_geometry(
                42,
                season=2025,
                circuit_id="openf1:circuit:7",
                curated_dir=curated_dir,
            )

            self.assertEqual(
                Path(result["curated_path"]),
                geometry_table_path(2025, curated_dir),
            )
            self.assertTrue(manifest_path.is_file())
            self.assertIsNotNone(geometry)
            self.assertEqual(result["inputs"]["location"]["raw_sha256"], sha256(raw_paths["location"]))
            master_manifest = json.loads(master_manifest_path.read_text(encoding="utf-8"))
            master_geometry = master_manifest["tables"]["circuit_geometry"]
            self.assertEqual(master_geometry["row_count"], 1)
            self.assertEqual(master_geometry["sha256"], result["curated_sha256"])
            raw_paths["location"].write_bytes(b"corrupt")
            with self.assertRaisesRegex(TrackGeometryError, "hash validation"):
                build_manifest_geometry(
                    manifest,
                    season=2025,
                    meeting_key=10,
                    circuit_id="openf1:circuit:7",
                    curated_dir=curated_dir,
                )

    def test_circuit_fallback_selects_latest_geometry(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "geometry.parquet"
            for session_key, timestamp in (
                    (10, "2025-01-01T00:00:00Z"),
                    (20, "2025-02-01T00:00:00Z"),
            ):
                record = build_geometry_record(
                    self.locations,
                    self.laps,
                    session_key=session_key,
                    meeting_key=session_key,
                    circuit_id="openf1:circuit:7",
                    sample_laps=3,
                    point_count=41,
                    ingested_at=pd.Timestamp(timestamp),
                )
                write_geometry_record(record, path)

            geometry = load_track_geometry(
                99,
                circuit_id="openf1:circuit:7",
                path=path,
            )

            self.assertIsNotNone(geometry)
            self.assertEqual(geometry.source_session_key, 20)


if __name__ == "__main__":
    unittest.main()
