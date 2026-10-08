from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.dashboard.session_replay import _pit_records
from f1_pipeline.dashboard.replay_service import replay_decision_time



class DashboardReplayServiceTest(unittest.TestCase):
    def test_replay_uses_actual_lap_end_after_delayed_session_end(self) -> None:
        laps = pd.DataFrame(
            {
                "available_at": [
                    "2026-10-04T08:35:32.315Z",
                    "2026-10-04T10:20:44.702Z",
                ]
            }
        )

        decision_time = replay_decision_time(
            "2026-10-04T09:00:00Z",
            laps,
        )

        self.assertEqual(decision_time, pd.Timestamp("2026-10-04T10:20:44.702Z"))

    def test_pit_records_reconstruct_lane_interval_before_exit_time(self) -> None:
        pits = pd.DataFrame(
            [
                {
                    "event_time": "2026-08-23T13:33:05.260000+00:00",
                    "available_at": "2026-08-23T13:33:05.260000+00:00",
                    "driver_number": 1,
                    "lap_number": 2,
                    "pit_duration_seconds": 1571.5,
                    "lane_duration_seconds": 1571.5,
                    "stop_duration_seconds": None,
                }
            ]
        )

        records = _pit_records(pits)

        self.assertEqual(len(records), 1)
        self.assertEqual(
            pd.Timestamp(records[0]["entry_time"]),
            pd.Timestamp("2026-08-23T13:06:53.760000+00:00"),
        )
        self.assertEqual(
            pd.Timestamp(records[0]["exit_time"]),
            pd.Timestamp("2026-08-23T13:33:05.260000+00:00"),
        )



if __name__ == "__main__":
    unittest.main()
