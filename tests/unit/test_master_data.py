"""Tests for the season master-data model."""

from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.master_data import build_tables


class MasterDataTest(unittest.TestCase):
    def test_postponed_meeting_links_to_replacement_and_geometry_stays_empty(self) -> None:
        ingested_at = pd.Timestamp("2026-08-13T11:00:00Z")
        meetings = [
            {
                "meeting_key": 1,
                "meeting_name": "Bahrain Grand Prix",
                "meeting_official_name": "Bahrain Grand Prix 2026",
                "location": "Sakhir",
                "country_key": 36,
                "country_code": "BRN",
                "country_name": "Bahrain",
                "country_flag": None,
                "circuit_key": 63,
                "circuit_short_name": "Sakhir",
                "circuit_type": "Permanent",
                "circuit_info_url": None,
                "circuit_image": None,
                "date_start": "2026-04-10T11:30:00Z",
                "date_end": "2026-04-12T17:00:00Z",
                "is_cancelled": True,
            },
            {
                "meeting_key": 2,
                "meeting_name": "Bahrain Grand Prix",
                "meeting_official_name": "Bahrain Grand Prix 2026 rescheduled",
                "location": "Sakhir",
                "country_key": 36,
                "country_code": "BRN",
                "country_name": "Bahrain",
                "country_flag": None,
                "circuit_key": 63,
                "circuit_short_name": "Sakhir",
                "circuit_type": "Permanent",
                "circuit_info_url": None,
                "circuit_image": None,
                "date_start": "2026-10-02T11:30:00Z",
                "date_end": "2026-10-04T17:00:00Z",
                "is_cancelled": False,
            },
        ]
        sessions = [
            {
                "session_key": 11,
                "meeting_key": 1,
                "session_type": "Race",
                "session_name": "Race",
                "date_start": "2026-04-12T15:00:00Z",
                "date_end": "2026-04-12T17:00:00Z",
                "is_cancelled": True,
            },
            {
                "session_key": 22,
                "meeting_key": 2,
                "session_type": "Race",
                "session_name": "Race",
                "date_start": "2026-10-04T15:00:00Z",
                "date_end": "2026-10-04T17:00:00Z",
                "is_cancelled": False,
            },
        ]
        drivers = [
            {
                "driver_number": 1,
                "name_acronym": "VER",
                "full_name": "Max VERSTAPPEN",
                "first_name": "Max",
                "last_name": "Verstappen",
                "broadcast_name": "M VERSTAPPEN",
                "country_code": None,
                "team_name": "Red Bull Racing",
                "team_colour": "3671C6",
                "headshot_url": None,
            }
        ]

        tables = build_tables(meetings, sessions, drivers, 2026, ingested_at, 22)

        old_meeting = next(row for row in tables["meeting"] if row["meeting_id"].endswith(":1"))
        new_session = next(row for row in tables["session"] if row["session_id"].endswith(":22"))
        self.assertEqual(old_meeting["status"], "postponed")
        self.assertEqual(old_meeting["superseded_by_meeting_id"], "openf1:meeting:2")
        self.assertEqual(new_session["status"], "scheduled")
        self.assertEqual(new_session["is_cancelled"], False)
        self.assertEqual(len(tables["circuit_geometry"]), 0)
        self.assertEqual(tables["driver"][0]["team_id"], "openf1:team:red-bull-racing")


if __name__ == "__main__":
    unittest.main()
