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


if __name__ == "__main__":
    unittest.main()
