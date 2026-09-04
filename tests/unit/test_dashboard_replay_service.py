from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.dashboard.session_replay import _pit_records
from f1_pipeline.dashboard.replay_service import (
    final_reconstructed_positions,
    stint_history_payload,
)
from f1_pipeline.replay.circle_of_doom import CarState, ReplayFrame, ReplayResult


def car(driver_number: int, acronym: str, position: int, lap: int) -> CarState:
    return CarState(
        driver_number=driver_number,
        acronym=acronym,
        team_colour="#123456",
        position=position,
        lap_number=lap,
        lap_progress=0.5,
        absolute_gap=float(position - 1),
        displayed_gap="LEADER" if position == 1 else f"+{position - 1:.1f}s",
        interval=None,
        compound="MEDIUM",
        tyre_age=lap,
        recently_pitted=False,
    )


class DashboardReplayServiceTest(unittest.TestCase):
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

    def test_stint_history_payload_orders_tyres_and_preserves_missing_values(
            self,
    ) -> None:
        stints = pd.DataFrame(
            [
                {
                    "driver_number": 1,
                    "stint_number": 2,
                    "lap_start": 21,
                    "lap_end": 40,
                    "compound": "medium",
                    "tyre_age_at_start": 1,
                },
                {
                    "driver_number": 1,
                    "stint_number": 1,
                    "lap_start": 1,
                    "lap_end": 20,
                    "compound": "soft",
                    "tyre_age_at_start": 0,
                },
                {
                    "driver_number": 2,
                    "stint_number": 1,
                    "lap_start": 1,
                    "lap_end": None,
                    "compound": None,
                    "tyre_age_at_start": None,
                },
            ]
        )

        history = stint_history_payload(stints)

        self.assertEqual(
            [(row["driver"], row["stint"], row["compound"]) for row in history],
            [(1, 1, "SOFT"), (1, 2, "MEDIUM"), (2, 1, "UNKNOWN")],
        )
        self.assertIsNone(history[-1]["end_lap"])
        self.assertIsNone(history[-1]["tyre_age_at_start"])

    def test_display_order_uses_fullest_final_lap_frame_and_appends_last_seen_drivers(
            self,
    ) -> None:
        frames = (
            ReplayFrame(
                date=pd.Timestamp("2026-07-26T13:00:00Z"),
                lap_number=1,
                status="GREEN",
                cars=(car(1, "ONE", 1, 1), car(2, "TWO", 2, 1), car(3, "THR", 3, 1)),
                projection=None,
            ),
            ReplayFrame(
                date=pd.Timestamp("2026-07-26T14:40:00Z"),
                lap_number=70,
                status="GREEN",
                cars=(car(1, "ONE", 1, 70), car(3, "THR", 2, 70)),
                projection=None,
            ),
            ReplayFrame(
                date=pd.Timestamp("2026-07-26T14:41:00Z"),
                lap_number=70,
                status="GREEN",
                cars=(car(3, "THR", 2, 70),),
                projection=None,
            ),
        )
        replay = ReplayResult(
            frames=frames,
            reference_lap_time=90.0,
            race_start=frames[0].date,
            race_end=frames[-1].date,
        )

        positions = final_reconstructed_positions(replay)

        self.assertEqual(positions["Driver"].tolist(), ["ONE", "THR", "TWO"])
        self.assertEqual(positions["Position"].tolist(), [1, 2, 3])
        self.assertEqual(
            positions["State"].tolist(), ["Finish frame", "Finish frame", "Last seen"]
        )


if __name__ == "__main__":
    unittest.main()
