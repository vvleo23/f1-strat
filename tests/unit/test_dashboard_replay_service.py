from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.dashboard.replay_service import final_reconstructed_positions
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
    def test_final_positions_use_fullest_frame_on_final_lap_and_append_retirements(
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
