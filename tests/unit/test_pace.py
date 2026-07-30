"""Tests for the first reproducible pace analysis."""

from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.analysis.pace import (
    build_openf1_pace_by_stint,
    build_source_pace_comparison,
)


class PaceAnalysisTest(unittest.TestCase):
    def setUp(self) -> None:
        self.openf1_laps = pd.DataFrame(
            {
                "session_key": [11342, 11342, 11342],
                "driver_number": [1, 1, 44],
                "lap_number": [1, 2, 1],
                "date_start": [
                    "2026-07-26T13:03:18.360000+00:00",
                    "2026-07-26T13:04:43.360000+00:00",
                    "2026-07-26T13:03:18.360000+00:00",
                ],
                "lap_duration": [85.0, 86.0, 87.0],
            }
        )
        self.openf1_stints = pd.DataFrame(
            {
                "session_key": [11342, 11342],
                "driver_number": [1, 44],
                "stint_number": [1, 1],
                "lap_start": [1, 1],
                "lap_end": [2, 1],
                "compound": ["MEDIUM", "HARD"],
                "tyre_age_at_start": [0, 0],
            }
        )
        self.fastf1_laps = pd.DataFrame(
            {
                "Driver": ["VER", "VER", "HAM"],
                "DriverNumber": ["1", "1", "44"],
                "LapNumber": [1, 2, 1],
                "LapTime": [
                    pd.Timedelta(85, unit="s"),
                    pd.Timedelta(86, unit="s"),
                    pd.Timedelta(87, unit="s"),
                ],
                "Deleted": [False, False, False],
            }
        )

    def test_assigns_each_valid_openf1_lap_to_one_stint(self) -> None:
        pace, diagnostics = build_openf1_pace_by_stint(
            self.openf1_laps, self.openf1_stints
        )

        self.assertEqual(len(pace), 2)
        self.assertEqual(int(pace["measured_laps"].sum()), 3)
        self.assertEqual(diagnostics["openf1_unassigned_valid_lap_rows"], 0)
        self.assertTrue((pace["source_system"] == "openf1").all())

    def test_keeps_sources_separate_in_driver_comparison(self) -> None:
        comparison = build_source_pace_comparison(
            self.openf1_laps, self.fastf1_laps
        )

        self.assertEqual(len(comparison), 2)
        self.assertEqual(comparison["openf1_lap_count"].sum(), 3)
        self.assertEqual(comparison["fastf1_lap_count"].sum(), 3)
        self.assertTrue((comparison["comparison_status"] == "comparable").all())


if __name__ == "__main__":
    unittest.main()


