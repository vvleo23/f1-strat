from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.analysis.teammate_comparison import (
    COMPARISON_METRICS,
    driver_comparison_metrics,
    session_conditions,
)


class TeammateComparisonTest(unittest.TestCase):
    def test_wet_requires_observed_evidence_and_missing_data_defaults_to_dry(self) -> None:
        empty = pd.DataFrame()
        cases = (
            (empty, empty, empty, "Dry"),
            (pd.DataFrame({"rainfall": [0, None]}), empty, empty, "Dry"),
            (pd.DataFrame({"rainfall": [0, 1]}), empty, empty, "Wet"),
            (empty, pd.DataFrame({"compound": ["INTERMEDIATE"], "lap_start": [1], "lap_end": [5]}), empty, "Wet"),
            (empty, pd.DataFrame({"compound": ["WET"], "lap_start": [1], "lap_end": [None]}), empty, "Dry"),
            (empty, empty, pd.DataFrame({"message": ["TRACK DECLARED WET"]}), "Wet"),
            (empty, empty, pd.DataFrame({"message": ["RAIN REPORTED IN SECTOR 2"]}), "Wet"),
            (empty, empty, pd.DataFrame({"message": ["DRS DISABLED", "SAFETY CAR DEPLOYED", "NO RAIN REPORTED", "RAIN EXPECTED"]}), "Dry"),
        )
        for weather, stints, control, expected in cases:
            with self.subTest(expected=expected, weather=weather, stints=stints, control=control):
                self.assertEqual(session_conditions(weather, stints, control)[0], expected)
        self.assertEqual(session_conditions(empty, empty, empty)[1], None)
        self.assertEqual(
            session_conditions(pd.DataFrame({"air_temperature": [16, None, 20]}), empty, empty)[1],
            18.0,
        )

    def test_metrics_split_races_and_qualifying_independently(self) -> None:
        races = pd.DataFrame([
            {"session_id": "race1", "driver_number": 10, "position": 3, "points": 15, "dnf": False, "condition": "Wet", "circuit_name": "Spa", "air_temperature": 18.0},
            {"session_id": "race2", "driver_number": 10, "position": 18, "points": 0, "dnf": True, "condition": "Dry", "circuit_name": "Monza", "air_temperature": None},
            {"session_id": "race1", "driver_number": 43, "position": 1, "points": 25, "dnf": False, "condition": "Wet", "circuit_name": "Spa", "air_temperature": 18.0},
        ])
        qualifying = pd.DataFrame([
            {"session_id": "quali1", "driver_number": 10, "position": 5, "condition": "Dry", "circuit_name": "Spa", "air_temperature": 21.0},
            {"session_id": "quali2", "driver_number": 10, "position": 10, "condition": "Wet", "circuit_name": "Monza", "air_temperature": None},
        ])
        comparison = driver_comparison_metrics(races, qualifying, 10)
        self.assertEqual(tuple(comparison.index), COMPARISON_METRICS)
        for metric, values in {
            "Races": ["2", "1", "1"],
            "Points": ["15", "0", "15"],
            "Wins": ["0", "", ""],
            "Podiums": ["1", "", ""],
            "Best finish": ["P3 · Spa · 18°C · Wet", "", ""],
            "Average finish": ["10.5", "18.0", "3.0"],
            "DNFs": ["1", "1", "0"],
            "DNF rate": ["50%", "100%", "0%"],
            "Qualifying sessions": ["2", "1", "1"],
            "Best qualifying": ["P5 · Spa · 21°C · Dry", "", ""],
            "Average qualifying position": ["7.5", "5.0", "10.0"],
        }.items():
            with self.subTest(metric=metric):
                self.assertEqual(comparison.loc[metric].tolist(), values)

    def test_empty_groups_and_missing_results_do_not_invent_zeroes(self) -> None:
        races = pd.DataFrame([
            {"session_id": "race1", "driver_number": 10, "position": None, "points": None, "dnf": None, "condition": "Dry"},
        ])
        comparison = driver_comparison_metrics(races, pd.DataFrame(), 10)
        self.assertEqual(comparison.loc["Races"].tolist(), ["1", "1", "—"])
        for metric in ("Points", "Best finish", "Average finish", "DNFs", "DNF rate", "Qualifying sessions", "Best qualifying", "Average qualifying position"):
            with self.subTest(metric=metric):
                self.assertEqual(comparison.loc[metric, "Overall"], "—")
        self.assertEqual(comparison.loc["DNF rate", "Wet"], "—")


if __name__ == "__main__":
    unittest.main()
