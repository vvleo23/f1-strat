from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.weather import build_weather_cut


class WeatherCutTest(unittest.TestCase):
    def test_selects_latest_available_forecast_and_only_past_observations(self) -> None:
        forecasts = pd.DataFrame(
            [
                {
                    "snapshot_id": snapshot,
                    "run_initialized_at": run,
                    "available_at": available,
                    "valid_time": valid,
                    "temperature_2m": value,
                }
                for snapshot, run, available, valid, value in (
                    (
                        "old",
                        "2026-07-26T00:00:00Z",
                        "2026-07-26T06:00:00Z",
                        "2026-07-26T13:00:00Z",
                        20.0,
                    ),
                    (
                        "future",
                        "2026-07-26T06:00:00Z",
                        "2026-07-26T12:00:00Z",
                        "2026-07-26T13:00:00Z",
                        25.0,
                    ),
                )
            ]
        )
        observations = pd.DataFrame(
            [
                {
                    "fact_id": "past",
                    "event_time": "2026-07-26T09:59:00Z",
                    "available_at": "2026-07-26T09:59:00Z",
                    "air_temperature": 19.0,
                },
                {
                    "fact_id": "future",
                    "event_time": "2026-07-26T10:01:00Z",
                    "available_at": "2026-07-26T10:01:00Z",
                    "air_temperature": 21.0,
                },
            ]
        )

        result = build_weather_cut(
            forecasts,
            observations,
            decision_time="2026-07-26T10:00:00Z",
        )

        self.assertEqual(result.status, "available")
        self.assertEqual(result.forecast["snapshot_id"].unique().tolist(), ["old"])
        self.assertEqual(result.observations["fact_id"].tolist(), ["past"])


if __name__ == "__main__":
    unittest.main()

