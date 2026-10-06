from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.dashboard.app import (
    _circular_mean_degrees,
    _interpolate_forecast,
    _job_payload,
    _session_downloadable,
    _session_requires_catalog_refresh,
    _weekend_job_payload,
    _weekend_weather_table,
)


def _forecast_frame() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "valid_time": "2026-07-26T13:00:00Z",
                "temperature_2m": 30.0,
                "rain": 0.0,
                "precipitation": 0.0,
                "wind_speed_10m": 18.0,
                "wind_direction_10m": 350.0,
            },
            {
                "valid_time": "2026-07-26T14:00:00Z",
                "temperature_2m": 32.0,
                "rain": 1.0,
                "precipitation": 1.0,
                "wind_speed_10m": 36.0,
                "wind_direction_10m": 10.0,
            },
        ]
    )


class CircularMeanDegreesTest(unittest.TestCase):
    def test_blends_across_the_north_wrap(self) -> None:
        # 350 deg and 10 deg average to (approximately) north, not 180 deg.
        blended = _circular_mean_degrees(350.0, 10.0, 0.5)
        self.assertLess(min(blended, 360 - blended), 1.0)

    def test_returns_endpoint_at_fraction_zero_and_one(self) -> None:
        self.assertAlmostEqual(_circular_mean_degrees(90.0, 200.0, 0.0), 90.0, places=6)
        self.assertAlmostEqual(_circular_mean_degrees(90.0, 200.0, 1.0), 200.0, places=6)


class SessionDownloadableTest(unittest.TestCase):
    def test_allows_past_scheduled_replacement_session(self) -> None:
        session = pd.Series(
            {
                "scheduled_end_utc": "2026-10-04T09:00:00Z",
                "status": "scheduled",
                "is_cancelled": False,
            }
        )

        self.assertTrue(
            _session_downloadable(session, pd.Timestamp("2026-10-06T12:00:00Z"))
        )
        self.assertTrue(
            _session_requires_catalog_refresh(
                session, pd.Timestamp("2026-10-06T12:00:00Z")
            )
        )

    def test_rejects_cancelled_or_future_session(self) -> None:
        cancelled = pd.Series(
            {
                "scheduled_end_utc": "2026-10-04T09:00:00Z",
                "status": "postponed",
                "is_cancelled": True,
            }
        )
        future = pd.Series(
            {
                "scheduled_end_utc": "2026-10-10T09:00:00Z",
                "status": "completed",
                "is_cancelled": False,
            }
        )
        now = pd.Timestamp("2026-10-06T12:00:00Z")

        self.assertFalse(_session_downloadable(cancelled, now))
        self.assertFalse(_session_downloadable(future, now))

    def test_session_job_refreshes_stale_catalog_status(self) -> None:
        session = pd.Series(
            {
                "session_id": "openf1:session:11731",
                "meeting_id": "openf1:meeting:1308",
                "scheduled_end_utc": "2026-10-04T09:00:00Z",
                "status": "scheduled",
                "is_cancelled": False,
            }
        )

        payload = _job_payload(
            2026,
            session,
            purpose="weekend",
            refresh=False,
        )

        self.assertTrue(payload["refresh"])

    def test_weekend_job_refreshes_stale_catalog_status(self) -> None:
        meeting = pd.Series(
            {
                "meeting_id": "openf1:meeting:1308",
                "planned_end_utc": "2000-10-04T09:00:00Z",
            }
        )
        sessions = pd.DataFrame(
            [
                {
                    "session_id": "openf1:session:11731",
                    "session_name": "Race",
                    "scheduled_end_utc": "2000-10-04T09:00:00Z",
                    "status": "scheduled",
                    "is_cancelled": False,
                }
            ]
        )

        payload = _weekend_job_payload(2026, meeting, sessions)

        self.assertTrue(payload["refresh"])


class InterpolateForecastTest(unittest.TestCase):
    def test_interpolates_between_two_hourly_points(self) -> None:
        point = _interpolate_forecast(
            _forecast_frame(), pd.Timestamp("2026-07-26T13:30:00Z")
        )
        self.assertIsNotNone(point)
        assert point is not None
        self.assertAlmostEqual(point["temperature"], 31.0, places=6)
        self.assertAlmostEqual(point["rain"], 0.5, places=6)
        # 18 km/h -> 27 km/h halfway, converted to m/s.
        self.assertAlmostEqual(point["wind_speed_ms"], 27.0 / 3.6, places=6)
        self.assertTrue(point["interpolated"])

    def test_exact_hour_is_not_flagged_as_interpolated(self) -> None:
        point = _interpolate_forecast(
            _forecast_frame(), pd.Timestamp("2026-07-26T13:00:00Z")
        )
        self.assertIsNotNone(point)
        assert point is not None
        self.assertAlmostEqual(point["temperature"], 30.0, places=6)
        self.assertFalse(point["interpolated"])

    def test_returns_none_outside_the_forecast_horizon(self) -> None:
        self.assertIsNone(
            _interpolate_forecast(_forecast_frame(), pd.Timestamp("2026-07-26T15:00:00Z"))
        )
        self.assertIsNone(
            _interpolate_forecast(_forecast_frame(), pd.Timestamp("2026-07-26T12:00:00Z"))
        )

    def test_returns_none_for_empty_forecast(self) -> None:
        self.assertIsNone(
            _interpolate_forecast(pd.DataFrame(), pd.Timestamp("2026-07-26T13:00:00Z"))
        )


class WeekendWeatherTableTest(unittest.TestCase):
    def test_builds_one_row_per_session_with_a_start_time(self) -> None:
        sessions = pd.DataFrame(
            [
                {"session_name": "Practice 1", "scheduled_start_utc": "2026-07-26T13:00:00Z"},
                {"session_name": "Race", "scheduled_start_utc": "2026-07-26T13:30:00Z"},
            ]
        )
        table = _weekend_weather_table(sessions, _forecast_frame())
        self.assertEqual(list(table["Session"]), ["FP1", "Race"])
        self.assertEqual(table.iloc[0]["Air °C"], 30.0)
        self.assertEqual(table.iloc[1]["Air °C"], 31.0)
        self.assertIn("≈", table.iloc[1]["Start (UTC)"])
        self.assertNotIn("≈", table.iloc[0]["Start (UTC)"])

    def test_empty_forecast_yields_empty_table(self) -> None:
        sessions = pd.DataFrame(
            [{"session_name": "Race", "scheduled_start_utc": "2026-07-26T13:30:00Z"}]
        )
        self.assertTrue(_weekend_weather_table(sessions, pd.DataFrame()).empty)

    def test_session_without_a_start_time_is_skipped(self) -> None:
        sessions = pd.DataFrame(
            [{"session_name": "Race", "scheduled_start_utc": None}]
        )
        self.assertTrue(_weekend_weather_table(sessions, _forecast_frame()).empty)


if __name__ == "__main__":
    unittest.main()
