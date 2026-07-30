"""Tests for runtime dataframe validation."""

from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.data_validation import DataValidationError, validate_frame


class DataValidationTest(unittest.TestCase):
    def test_accepts_mixed_iso_timestamp_formats(self) -> None:
        frame = pd.DataFrame(
            {
                "session_key": [11342, 11342],
                "date": [
                    "2026-07-26T13:03:20.314000+00:00",
                    "2026-07-26T13:03:21+00:00",
                ],
            }
        )

        validate_frame(
            frame,
            name="intervals",
            required_columns={"session_key", "date"},
            key_columns=("session_key", "date"),
            datetime_columns=("date",),
            expected_session_key=11342,
        )

    def test_rejects_duplicate_business_keys(self) -> None:
        frame = pd.DataFrame(
            {
                "session_key": [11342, 11342],
                "driver_number": [1, 1],
                "lap_number": [10, 10],
            }
        )

        with self.assertRaises(DataValidationError):
            validate_frame(
                frame,
                name="laps",
                required_columns={"session_key", "driver_number", "lap_number"},
                key_columns=("session_key", "driver_number", "lap_number"),
                expected_session_key=11342,
            )

    def test_rejects_invalid_timestamp(self) -> None:
        frame = pd.DataFrame(
            {"session_key": [11342], "date": ["not-a-timestamp"]}
        )

        with self.assertRaises(DataValidationError):
            validate_frame(
                frame,
                name="position",
                required_columns={"session_key", "date"},
                datetime_columns=("date",),
                expected_session_key=11342,
            )


if __name__ == "__main__":
    unittest.main()

