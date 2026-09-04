from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.analysis.qualifying_prediction import (
    Calibration,
    PredictionParameters,
    SessionInput,
    calculate_prediction,
)


class QualifyingPredictionTest(unittest.TestCase):
    def session(
        self,
        key: int,
        start: str,
        *,
        wet: bool,
        reverse: bool,
    ) -> SessionInput:
        numbers = list(range(1, 17))
        rows = []
        for number in numbers:
            rank = 17 - number if reverse else number
            best = 80.0 + rank * 0.1
            rows.extend(
                [
                    {
                        "driver_number": number,
                        "lap_number": 1,
                        "lap_duration_seconds": best,
                        "is_pit_out_lap": False,
                        "event_time": "2026-05-01T10:10:00Z",
                        "available_at": "2026-05-01T10:10:00Z",
                    },
                    {
                        "driver_number": number,
                        "lap_number": 2,
                        "lap_duration_seconds": best + 0.2,
                        "is_pit_out_lap": False,
                        "event_time": "2026-05-01T10:12:00Z",
                        "available_at": "2026-05-01T10:12:00Z",
                    },
                ]
            )
        stints = pd.DataFrame(
            {
                "driver_number": numbers,
                "lap_start": 1,
                "lap_end": 2,
                "compound": ["SOFT"] * len(numbers),
                "tyre_age_at_start": [0] * len(numbers),
                "event_time": ["2026-05-01T10:20:00Z"] * len(numbers),
                "available_at": ["2026-05-01T10:20:00Z"] * len(numbers),
            }
        )
        entries = pd.DataFrame(
            {
                "driver_number": numbers,
                "name_acronym": [f"D{number}" for number in numbers],
                "full_name": [f"Driver {number}" for number in numbers],
                "team_name": [f"Team {(number + 1) // 2}" for number in numbers],
            }
        )
        weather = pd.DataFrame(
            {
                "air_temperature": [20.0],
                "track_temperature": [30.0],
                "rainfall": [1 if wet else 0],
                "event_time": ["2026-05-01T10:15:00Z"],
                "available_at": ["2026-05-01T10:15:00Z"],
            }
        )
        return SessionInput(
            key,
            "practice",
            pd.Timestamp(start),
            pd.DataFrame(rows),
            stints,
            entries,
            weather,
        )

    def forecast(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "valid_time": ["2026-05-02T14:00:00Z"],
                "temperature_2m": [22.0],
                "rain": [0.0],
                "precipitation": [0.0],
            }
        )

    def test_dry_session_outweighs_newer_wet_session_and_probabilities_are_coherent(self) -> None:
        result = calculate_prediction(
            [
                self.session(1, "2026-05-01T10:00:00Z", wet=False, reverse=False),
                self.session(2, "2026-05-01T12:00:00Z", wet=True, reverse=True),
            ],
            target_session_id="openf1:session:3",
            target_start=pd.Timestamp("2026-05-02T14:00:00Z"),
            target_end=pd.Timestamp("2026-05-02T15:00:00Z"),
            decision_time=pd.Timestamp("2026-05-02T13:00:00Z"),
            forecast=self.forecast(),
            calibration=Calibration(pd.DataFrame(), {}, {}, {}),
            calculation_id="0123456789abcdef0123",
        )

        self.assertEqual(result.status, "partial")
        self.assertEqual(int(result.rows.iloc[0]["driver_number"]), 1)
        self.assertEqual(result.rows.iloc[0]["compound_on_fastest_laps"], ["SOFT", "SOFT"])
        self.assertEqual(result.rows.iloc[0]["tyre_age_on_fastest_laps"], [0, 0])
        for row in result.rows.itertuples():
            self.assertGreaterEqual(row.top_15_probability, row.top_10_probability)
            self.assertGreaterEqual(row.top_10_probability, row.top_5_probability)
        self.assertAlmostEqual(result.rows["top_15_probability"].sum(), 15.0, places=8)
        self.assertAlmostEqual(result.rows["top_10_probability"].sum(), 10.0, places=8)
        self.assertAlmostEqual(result.rows["top_5_probability"].sum(), 5.0, places=8)

    def test_same_calculation_id_is_deterministic_and_missing_weather_is_partial(self) -> None:
        arguments = {
            "target_session_id": "openf1:session:3",
            "target_start": pd.Timestamp("2026-05-02T14:00:00Z"),
            "target_end": pd.Timestamp("2026-05-02T15:00:00Z"),
            "decision_time": pd.Timestamp("2026-05-02T13:00:00Z"),
            "forecast": pd.DataFrame(),
            "calibration": Calibration(pd.DataFrame(), {}, {}, {}),
            "calculation_id": "fedcba98765432100123",
        }
        session = self.session(1, "2026-05-01T10:00:00Z", wet=False, reverse=False)
        first = calculate_prediction([session], **arguments)
        second = calculate_prediction([session], **arguments)

        self.assertEqual(first.status, "partial")
        pd.testing.assert_frame_equal(first.rows, second.rows)
        self.assertIn("Target weather forecast is unavailable.", first.diagnostics["reasons"])
        self.assertEqual(first.diagnostics["uncertainty_multiplier"], 1.5)

    def test_lap_after_decision_time_cannot_change_prediction(self) -> None:
        session = self.session(1, "2026-05-01T10:00:00Z", wet=False, reverse=False)
        future = pd.DataFrame(
            [
                {
                    "driver_number": 16,
                    "lap_number": 3,
                    "lap_duration_seconds": 1.0,
                    "is_pit_out_lap": False,
                    "event_time": "2026-05-02T14:01:00Z",
                    "available_at": "2026-05-02T14:01:00Z",
                }
            ]
        )
        session = SessionInput(
            session.session_key,
            session.session_type,
            session.scheduled_start,
            pd.concat([session.laps, future], ignore_index=True),
            session.stints,
            session.entries,
            session.weather,
        )
        result = calculate_prediction(
            [session],
            target_session_id="openf1:session:3",
            target_start=pd.Timestamp("2026-05-02T14:00:00Z"),
            target_end=pd.Timestamp("2026-05-02T15:00:00Z"),
            decision_time=pd.Timestamp("2026-05-02T14:00:00Z"),
            forecast=self.forecast(),
            calibration=Calibration(pd.DataFrame(), {}, {}, {}),
            calculation_id="abcdef01234567890123",
        )

        self.assertEqual(int(result.rows.iloc[0]["driver_number"]), 1)

    def test_no_eligible_session_is_unavailable(self) -> None:
        result = calculate_prediction(
            [],
            target_session_id="openf1:session:3",
            target_start=pd.Timestamp("2026-05-02T14:00:00Z"),
            target_end=pd.Timestamp("2026-05-02T15:00:00Z"),
            decision_time=pd.Timestamp("2026-05-02T13:00:00Z"),
            forecast=pd.DataFrame(),
            calibration=Calibration(pd.DataFrame(), {}, {}, {}),
            calculation_id="0123456789abcdef0123",
        )

        self.assertEqual(result.status, "unavailable")
        self.assertTrue(result.rows.empty)

    def test_practice_decay_can_isolate_the_latest_session_for_backtesting(self) -> None:
        sessions = [
            self.session(1, "2026-05-01T08:00:00Z", wet=False, reverse=True),
            self.session(2, "2026-05-01T10:00:00Z", wet=False, reverse=True),
            self.session(3, "2026-05-01T12:00:00Z", wet=False, reverse=False),
        ]
        arguments = {
            "target_session_id": "openf1:session:4",
            "target_start": pd.Timestamp("2026-05-02T14:00:00Z"),
            "target_end": pd.Timestamp("2026-05-02T15:00:00Z"),
            "decision_time": pd.Timestamp("2026-05-02T13:00:00Z"),
            "forecast": self.forecast(),
            "calibration": Calibration(pd.DataFrame(), {}, {}, {}),
            "calculation_id": "0123456789abcdef0123",
        }

        equal = calculate_prediction(
            sessions,
            parameters=PredictionParameters(practice_decay=1.0),
            **arguments,
        )
        latest = calculate_prediction(
            sessions,
            parameters=PredictionParameters(practice_decay=0.0),
            **arguments,
        )

        self.assertEqual(int(equal.rows.iloc[0]["driver_number"]), 16)
        self.assertEqual(int(latest.rows.iloc[0]["driver_number"]), 1)


if __name__ == "__main__":
    unittest.main()
