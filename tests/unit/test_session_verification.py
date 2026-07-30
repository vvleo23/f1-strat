"""Tests for the historical session verification rules."""

from __future__ import annotations

import unittest

from f1_pipeline.sources.session_verification import (
    SessionVerificationError,
    _overall_status,
    select_hungary_meeting,
    select_hungary_race,
)


class SessionVerificationTest(unittest.TestCase):
    def test_selects_the_hungary_meeting_by_country_and_location(self) -> None:
        meeting = select_hungary_meeting(
            [
                {
                    "meeting_key": 10,
                    "country_name": "Belgium",
                    "location": "Spa-Francorchamps",
                },
                {
                    "meeting_key": 20,
                    "country_name": "Hungary",
                    "location": "Hungaroring",
                },
            ]
        )

        self.assertEqual(meeting["meeting_key"], 20)

    def test_rejects_ambiguous_hungary_meetings(self) -> None:
        with self.assertRaises(SessionVerificationError):
            select_hungary_meeting(
                [
                    {"meeting_key": 20, "country_name": "Hungary"},
                    {"meeting_key": 21, "location": "Hungaroring"},
                ]
            )

    def test_selects_only_the_race_inside_the_target_window(self) -> None:
        session = select_hungary_race(
            [
                {
                    "meeting_key": 20,
                    "session_key": 201,
                    "session_name": "Qualifying",
                    "date_start": "2026-07-25T13:00:00Z",
                    "date_end": "2026-07-25T14:00:00Z",
                },
                {
                    "meeting_key": 20,
                    "session_key": 202,
                    "session_name": "Race",
                    "date_start": "2026-07-26T13:00:00Z",
                    "date_end": "2026-07-26T15:00:00Z",
                },
            ],
            20,
        )

        self.assertEqual(session["session_key"], 202)

    def test_required_data_can_be_partial_without_fabricated_values(self) -> None:
        status = _overall_status(
            {
                "sessions": {"status": "available"},
                "drivers": {"status": "available"},
                "laps": {"status": "available"},
                "intervals": {"status": "available"},
                "position": {"status": "available"},
                "location": {"status": "available"},
                "weather": {"status": "unavailable"},
            }
        )

        self.assertEqual(status, "partial")

    def test_all_stale_data_is_usable_but_not_current(self) -> None:
        status = _overall_status(
            {
                "sessions": {"status": "stale"},
                "drivers": {"status": "stale"},
                "laps": {"status": "stale"},
                "intervals": {"status": "stale"},
                "position": {"status": "stale"},
                "location": {"status": "stale"},
                "weather": {"status": "stale"},
            }
        )

        self.assertEqual(status, "stale")


if __name__ == "__main__":
    unittest.main()

