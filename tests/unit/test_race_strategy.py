from __future__ import annotations

import json
import unittest
from datetime import timedelta

import pandas as pd

from f1_pipeline.analysis.race_strategy import calculate_strategy_timeline
from f1_pipeline.pit_loss import PitLoss


def fact_rows(rows: list[dict]) -> pd.DataFrame:
    frame = pd.DataFrame(rows)
    if frame.empty:
        return frame
    frame["event_time"] = pd.to_datetime(frame["event_time"], utc=True)
    frame["available_at"] = pd.to_datetime(frame["available_at"], utc=True)
    return frame


def empty_race() -> dict[str, pd.DataFrame]:
    return {
        name: pd.DataFrame()
        for name in (
            "drivers",
            "laps",
            "intervals",
            "position",
            "pit",
            "stints",
            "race_control",
            "weather",
        )
    }


class RaceStrategyTest(unittest.TestCase):
    def setUp(self) -> None:
        self.start = pd.Timestamp("2026-07-26T13:00:00Z")
        self.loss = PitLoss(
            key="hungary",
            green_seconds=20.0,
            vsc_seconds=12.0,
            sc_seconds=10.0,
            source_asset="source.png",
            neutralized_basis="assumption",
        )
        prior_laps = []
        for driver, base in ((1, 90.0), (2, 91.0)):
            for lap in range(1, 7):
                lap = int(lap)
                time = self.start - timedelta(hours=24) + timedelta(minutes=2 * lap)
                prior_laps.append(
                    {
                        "driver_number": driver,
                        "lap_number": lap,
                        "lap_duration_seconds": base + 0.05 * lap,
                        "is_pit_out_lap": False,
                        "event_time": time,
                        "available_at": time,
                    }
                )
        self.priors = [
            {
                "laps": fact_rows(prior_laps),
                "stints": pd.DataFrame(
                    [
                        {"driver_number": 1, "stint_number": 1, "lap_start": 1, "lap_end": 3, "compound": "MEDIUM", "tyre_age_at_start": 0},
                        {"driver_number": 1, "stint_number": 2, "lap_start": 4, "lap_end": 6, "compound": "HARD", "tyre_age_at_start": 0},
                        {"driver_number": 2, "stint_number": 1, "lap_start": 1, "lap_end": 3, "compound": "MEDIUM", "tyre_age_at_start": 0},
                        {"driver_number": 2, "stint_number": 2, "lap_start": 4, "lap_end": 6, "compound": "HARD", "tyre_age_at_start": 0},
                    ]
                ),
            }
        ]

    def race(self, lap_count: int) -> dict[str, pd.DataFrame]:
        race = empty_race()
        lap_rows = []
        position_rows = []
        interval_rows = []
        for lap in range(1, lap_count + 1):
            lap = int(lap)
            time = self.start + timedelta(seconds=90 * lap)
            for driver, duration, position, gap in ((1, 90.0, 1, 0.0), (2, 91.0, 2, 3.0 + lap)):
                lap_rows.append(
                    {
                        "driver_number": driver,
                        "lap_number": lap,
                        "lap_duration_seconds": duration,
                        "is_pit_out_lap": False,
                        "event_time": time,
                        "available_at": time,
                    }
                )
                position_rows.append(
                    {"driver_number": driver, "position": position, "event_time": time, "available_at": time}
                )
                interval_rows.append(
                    {"driver_number": driver, "gap_to_leader_seconds": gap, "event_time": time, "available_at": time}
                )
        race["laps"] = fact_rows(lap_rows)
        race["position"] = fact_rows(position_rows)
        race["intervals"] = fact_rows(interval_rows)
        race["stints"] = pd.DataFrame(
            [
                {
                    "driver_number": driver,
                    "stint_number": 1,
                    "lap_start": 1,
                    "lap_end": lap_count,
                    "compound": "MEDIUM",
                    "tyre_age_at_start": 0,
                    "stint_started_at": self.start,
                    "event_time": self.start + timedelta(seconds=90 * lap_count),
                    "available_at": self.start + timedelta(seconds=90 * lap_count),
                }
                for driver in (1, 2)
            ]
            + [
                {
                    "driver_number": 1,
                    "stint_number": 2,
                    "lap_start": 8,
                    "lap_end": 12,
                    "compound": "HARD",
                    "tyre_age_at_start": 0,
                    "stint_started_at": self.start + timedelta(seconds=90 * 7),
                    "event_time": self.start + timedelta(seconds=90 * 12),
                    "available_at": self.start + timedelta(seconds=90 * 12),
                }
            ]
        )
        return race

    def test_pre_race_plan_uses_prior_sessions(self) -> None:
        result = calculate_strategy_timeline(
            self.race(2),
            self.priors,
            forecast=pd.DataFrame(),
            focus_driver=1,
            race_start=self.start,
            decision_time=self.start,
            planned_laps=12,
            pit_loss=self.loss,
            input_manifest_hash="input",
        )

        self.assertEqual(result.recommendations.iloc[0]["action"], "START")
        self.assertIn(result.recommendations.iloc[0]["next_compound"], {"MEDIUM", "HARD"})

    def test_future_laps_and_future_stint_do_not_change_earlier_recommendation(self) -> None:
        cutoff = self.start + timedelta(seconds=180)
        short = calculate_strategy_timeline(
            self.race(2),
            self.priors,
            forecast=pd.DataFrame(),
            focus_driver=1,
            race_start=self.start,
            decision_time=cutoff,
            planned_laps=12,
            pit_loss=self.loss,
            input_manifest_hash="input",
        )
        expanded = calculate_strategy_timeline(
            self.race(10),
            self.priors,
            forecast=pd.DataFrame(),
            focus_driver=1,
            race_start=self.start,
            decision_time=self.start + timedelta(seconds=900),
            planned_laps=12,
            pit_loss=self.loss,
            input_manifest_hash="input",
        )
        expected = short.recommendations[short.recommendations["decision_time"].eq(cutoff.isoformat())].iloc[0]
        actual = expanded.recommendations[expanded.recommendations["decision_time"].eq(cutoff.isoformat())].iloc[0]

        self.assertEqual(expected["calculation_id"], actual["calculation_id"])
        self.assertEqual(expected["action"], actual["action"])
        self.assertEqual(expected["current_compound"], "MEDIUM")
        self.assertEqual(actual["current_compound"], "MEDIUM")

    def test_neutralized_losses_are_total_race_time_losses(self) -> None:
        self.assertEqual(self.loss.for_status("GREEN"), 20.0)
        self.assertEqual(self.loss.for_status("YELLOW"), 20.0)
        self.assertEqual(self.loss.for_status("VSC"), 12.0)
        self.assertEqual(self.loss.for_status("SC"), 10.0)

    def test_pre_race_plan_respects_default_tyre_life(self) -> None:
        result = calculate_strategy_timeline(
            self.race(2),
            self.priors,
            forecast=pd.DataFrame(),
            focus_driver=1,
            race_start=self.start,
            decision_time=self.start,
            planned_laps=70,
            pit_loss=self.loss,
            input_manifest_hash="input",
        )

        plan = json.loads(result.recommendations.iloc[0]["plan_json"])

        self.assertTrue(plan)
        limits = {"SOFT": 25, "MEDIUM": 35, "HARD": 50}
        self.assertTrue(
            all(
                stint["end_lap"] - stint["start_lap"] + 1
                <= limits[stint["compound"]]
                for stint in plan
            )
        )

    def test_completed_distance_wins_over_late_red_flag(self) -> None:
        race = self.race(12)
        finish = self.start + timedelta(seconds=90 * 12)
        race["race_control"] = fact_rows(
            [
                {
                    "message": "RED FLAG",
                    "flag": "RED",
                    "category": "Flag",
                    "event_time": finish,
                    "available_at": finish,
                }
            ]
        )
        result = calculate_strategy_timeline(
            race,
            self.priors,
            forecast=pd.DataFrame(),
            focus_driver=1,
            race_start=self.start,
            decision_time=finish,
            planned_laps=12,
            pit_loss=self.loss,
            input_manifest_hash="input",
        )

        self.assertEqual(result.recommendations.iloc[-1]["action"], "FINISH")


if __name__ == "__main__":
    unittest.main()
