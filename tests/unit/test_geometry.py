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
    load_track_geometry,
    point_at_progress,
    write_geometry_record,
)


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

    def test_interpolates_progress_and_wraps_at_one(self) -> None:
        points = ((0.0, 1.0), (1.0, 0.0), (0.0, -1.0), (-1.0, 0.0), (0.0, 1.0))

        self.assertEqual(point_at_progress(points, 0.0), (0.0, 1.0))
        self.assertEqual(point_at_progress(points, 1.0), (0.0, 1.0))
        midpoint = point_at_progress(points, 0.125)
        self.assertAlmostEqual(midpoint[0], 0.5)
        self.assertAlmostEqual(midpoint[1], 0.5)

    def test_geometry_record_survives_write_and_read(self) -> None:
        record = build_geometry_record(
            self.locations,
            self.laps,
            session_key=11342,
            meeting_key=1291,
            circuit_id="openf1:circuit:4",
            sample_laps=3,
            point_count=41,
            ingested_at=pd.Timestamp("2026-08-18T12:00:00Z"),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "circuit_geometry.parquet"
            write_geometry_record(record, path)
            geometry = load_track_geometry(
                11342,
                meeting_key=1291,
                circuit_id="openf1:circuit:4",
                path=path,
            )

        self.assertIsNotNone(geometry)
        assert geometry is not None
        self.assertEqual(geometry.source, "openf1")
        self.assertEqual(geometry.source_session_key, 11342)
        self.assertGreaterEqual(len(geometry.points), 3)
        self.assertEqual(geometry.points[0], geometry.points[-1])

    def test_rejects_geometry_with_invalid_source_keys(self) -> None:
        record = build_geometry_record(
            self.locations,
            self.laps,
            session_key=11342,
            meeting_key=1291,
            circuit_id="openf1:circuit:4",
            sample_laps=3,
            point_count=41,
        )
        data = json.loads(record["geometry_data"])
        data.pop("source_session_key")
        record["geometry_data"] = json.dumps(data)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "circuit_geometry.parquet"
            write_geometry_record(record, path)
            with self.assertRaisesRegex(TrackGeometryError, "invalid source keys"):
                load_track_geometry(11342, path=path)


if __name__ == "__main__":
    unittest.main()

