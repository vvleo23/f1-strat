from __future__ import annotations

import unittest
from pathlib import Path

import pandas as pd

from f1_pipeline.session_facts import normalize_session_fact


class SessionFactsTest(unittest.TestCase):
    def test_preserves_interval_semantics_and_releases_laps_and_stints_at_end(self) -> None:
        laps = pd.DataFrame(
            [
                {
                    "session_key": 42,
                    "driver_number": 1,
                    "lap_number": 1,
                    "date_start": "2026-07-26T13:00:00Z",
                    "lap_duration": 90.0,
                }
            ]
        )
        intervals = pd.DataFrame(
            [
                {
                    "session_key": 42,
                    "driver_number": 1,
                    "date": "2026-07-26T13:01:31Z",
                    "gap_to_leader": "+1 LAP",
                    "interval": "12.500",
                }
            ]
        )
        stints = pd.DataFrame(
            [
                {
                    "session_key": 42,
                    "driver_number": 1,
                    "stint_number": 1,
                    "lap_start": 1,
                    "lap_end": 1,
                    "compound": "MEDIUM",
                    "tyre_age_at_start": 0,
                }
            ]
        )
        metadata = {
            "session_key": 42,
            "retrieved_at": pd.Timestamp("2026-08-25T00:00:00Z"),
            "ingested_at": pd.Timestamp("2026-08-25T00:01:00Z"),
            "raw_path": Path("data/raw/example.parquet"),
            "raw_sha256": "abc123",
        }

        _, lap_facts = normalize_session_fact("laps", laps, **metadata)
        _, interval_facts = normalize_session_fact("intervals", intervals, **metadata)
        _, stint_facts = normalize_session_fact("stints", stints, laps=laps, **metadata)

        lap_end = pd.Timestamp("2026-07-26T13:01:30Z")
        self.assertEqual(lap_facts.iloc[0]["event_time"], lap_end)
        self.assertEqual(stint_facts.iloc[0]["available_at"], lap_end)
        self.assertEqual(interval_facts.iloc[0]["gap_to_leader_raw"], "+1 LAP")
        self.assertEqual(interval_facts.iloc[0]["laps_behind"], 1)
        self.assertEqual(interval_facts.iloc[0]["interval_seconds"], 12.5)

    def test_normalizes_results_and_championship_standings(self) -> None:
        metadata = {
            "session_key": 42,
            "retrieved_at": pd.Timestamp("2026-08-25T00:00:00Z"),
            "ingested_at": pd.Timestamp("2026-08-25T00:01:00Z"),
            "raw_path": Path("data/raw/results.parquet"),
            "raw_sha256": "result123",
        }
        results = pd.DataFrame(
            [
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "driver_number": 1,
                    "position": 1,
                    "number_of_laps": 70,
                    "points": 25.0,
                    "dnf": False,
                    "dns": "false",
                    "dsq": False,
                    "duration": 5996.18,
                    "gap_to_leader": 0,
                },
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "driver_number": 2,
                    "position": 2,
                    "number_of_laps": 69,
                    "points": 18.0,
                    "dnf": "true",
                    "dns": False,
                    "dsq": False,
                    "duration": [80.1, 79.8],
                    "gap_to_leader": "+1 LAP",
                },
            ]
        )
        driver_standing = pd.DataFrame(
            [
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "driver_number": 1,
                    "position_start": 2,
                    "position_current": 1,
                    "points_start": 90.5,
                    "points_current": 115.5,
                }
            ]
        )
        team_standing = pd.DataFrame(
            [
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "team_name": "Example Racing",
                    "position_start": 2,
                    "position_current": 1,
                    "points_start": 140.0,
                    "points_current": 165.0,
                }
            ]
        )

        _, result_facts = normalize_session_fact("session_result", results, **metadata)
        _, driver_facts = normalize_session_fact(
            "championship_drivers", driver_standing, **metadata
        )
        _, team_facts = normalize_session_fact(
            "championship_teams", team_standing, **metadata
        )

        self.assertEqual(result_facts.iloc[0]["meeting_id"], "openf1:meeting:7")
        self.assertEqual(result_facts.iloc[0]["duration_seconds"], 5996.18)
        self.assertTrue(pd.isna(result_facts.iloc[1]["duration_seconds"]))
        self.assertEqual(result_facts.iloc[1]["duration_raw"], "[80.1,79.8]")
        self.assertEqual(result_facts.iloc[1]["laps_behind"], 1)
        self.assertTrue(result_facts.iloc[1]["dnf"])
        self.assertFalse(result_facts.iloc[0]["dns"])
        self.assertEqual(driver_facts.iloc[0]["points_current"], 115.5)
        self.assertEqual(team_facts.iloc[0]["team_id"], "openf1:team:example-racing")
        self.assertTrue(
            (result_facts["availability_basis"] == "observed_retrieval").all()
        )


if __name__ == "__main__":
    unittest.main()
