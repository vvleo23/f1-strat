"""Tests for temporal replay reconstruction and rendering."""

from __future__ import annotations

import unittest
from datetime import timedelta
from typing import cast

import pandas as pd

from f1_pipeline.replay.circle_of_doom import (
    build_location_progress,
    build_replay,
    estimate_reference_lap_time,
    infer_race_window,
    make_parquet_safe,
    reconstruct_absolute_gaps,
    status_events,
)


class CircleOfDoomTest(unittest.TestCase):
    def test_make_parquet_safe_preserves_numeric_and_lapped_gaps(self) -> None:
        frame = pd.DataFrame(
            {"gap_to_leader": [0, 12.5, "+1 LAP", None], "interval": [0, 2.5, 5, None]}
        )

        safe = make_parquet_safe("intervals", frame)

        self.assertEqual(safe["gap_to_leader"].tolist(), ["0", "12.5", "+1 LAP", None])
        self.assertEqual(safe["interval"].tolist(), ["0.0", "2.5", "5.0", None])
        result_safe = make_parquet_safe(
            "session_result", frame[["gap_to_leader"]]
        )
        self.assertEqual(
            result_safe["gap_to_leader"].tolist(), ["0", "12.5", "+1 LAP", None]
        )
        qualifying_safe = make_parquet_safe(
            "session_result",
            pd.DataFrame({"gap_to_leader": [[0.0, 1.2, None]]}),
        )
        self.assertEqual(
            qualifying_safe.iloc[0]["gap_to_leader"], "[0.0,1.2,null]"
        )

    def test_reference_lap_time_allows_missing_pit_out_column(self) -> None:
        laps = pd.DataFrame(
            {
                "driver_number": [1, 2],
                "lap_number": [5, 5],
                "lap_duration": [90.0, 92.0],
            }
        )

        reference = estimate_reference_lap_time(laps, pd.DataFrame())

        self.assertAlmostEqual(reference, 90.5)

    def test_reconstruct_absolute_gaps_handles_lapped_driver(self) -> None:
        rows = pd.DataFrame(
            [
                {"driver_number": 1, "position": 1, "gap_to_leader": 0, "interval": 0},
                {"driver_number": 2, "position": 2, "gap_to_leader": 10.0, "interval": 10.0},
                {
                    "driver_number": 3,
                    "position": 3,
                    "gap_to_leader": "+1 LAP",
                    "interval": 5.0,
                },
            ]
        )

        result = reconstruct_absolute_gaps(rows, reference_lap_time=100.0)

        self.assertEqual([row["driver_number"] for row in result], [1, 2, 3])
        self.assertEqual(result[0]["absolute_gap"], 0.0)
        self.assertEqual(result[1]["absolute_gap"], 10.0)
        self.assertEqual(result[2]["absolute_gap"], 100.0)

    def test_location_progress_uses_geometric_distance(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        laps = pd.DataFrame(
            [
                {"date_start": start, "driver_number": 1, "lap_number": 1},
                {
                    "date_start": start + timedelta(seconds=10),
                    "driver_number": 1,
                    "lap_number": 2,
                },
                {
                    "date_start": start + timedelta(seconds=20),
                    "driver_number": 1,
                    "lap_number": 3,
                },
            ]
        )
        location = pd.DataFrame(
            [
                {"date": start, "driver_number": 1, "x": 0, "y": 0, "z": 0},
                {
                    "date": start + timedelta(seconds=5),
                    "driver_number": 1,
                    "x": 5,
                    "y": 0,
                    "z": 0,
                },
                {
                    "date": start + timedelta(seconds=10),
                    "driver_number": 1,
                    "x": 10,
                    "y": 0,
                    "z": 0,
                },
                {
                    "date": start + timedelta(seconds=15),
                    "driver_number": 1,
                    "x": 15,
                    "y": 0,
                    "z": 0,
                },
                {
                    "date": start + timedelta(seconds=20),
                    "driver_number": 1,
                    "x": 20,
                    "y": 0,
                    "z": 0,
                },
            ]
        )

        progress = build_location_progress(location, laps)

        self.assertEqual(
            progress["track_progress"].tolist(), [0.0, 0.5, 0.0, 0.5, 0.0]
        )

    def test_location_progress_keeps_moving_across_missing_lap_number(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-08-23T13:00:00Z"))
        laps = pd.DataFrame(
            [
                {"date_start": start, "driver_number": 1, "lap_number": 1},
                {
                    "date_start": start + timedelta(seconds=4),
                    "driver_number": 1,
                    "lap_number": 2,
                },
                {
                    "date_start": start + timedelta(seconds=12),
                    "driver_number": 1,
                    "lap_number": 4,
                },
                {
                    "date_start": start + timedelta(seconds=16),
                    "driver_number": 1,
                    "lap_number": 5,
                },
            ]
        )
        coordinates = [0, 1, 2, 1, 0, 1, 2, 1, 0, 1, 2, 1, 0, 1, 2, 1, 0]
        location = pd.DataFrame(
            [
                {
                    "date": start + timedelta(seconds=seconds),
                    "driver_number": 1,
                    "x": coordinate,
                    "y": 0,
                    "z": 0,
                }
                for seconds, coordinate in enumerate(coordinates)
            ]
        )

        progress = build_location_progress(location, laps)

        self.assertEqual(
            progress["track_progress"].round(2).tolist(),
            [0.0, 0.25, 0.5, 0.75] * 4 + [0.0],
        )

    def test_location_progress_does_not_treat_start_acceleration_as_outlier(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        laps = pd.DataFrame(
            [
                {"date_start": start, "driver_number": 1, "lap_number": 1},
                {
                    "date_start": start + timedelta(seconds=4),
                    "driver_number": 1,
                    "lap_number": 2,
                },
                {
                    "date_start": start + timedelta(seconds=8),
                    "driver_number": 1,
                    "lap_number": 3,
                },
            ]
        )
        coordinates = [0, 0.01, 1.01, 2.01, 3.01, 4.01, 5.01, 6.01, 7.01]
        location = pd.DataFrame(
            [
                {
                    "date": start + timedelta(seconds=seconds),
                    "driver_number": 1,
                    "x": coordinate,
                    "y": 0,
                    "z": 0,
                }
                for seconds, coordinate in enumerate(coordinates)
            ]
        )

        progress = build_location_progress(location, laps)
        second_lap = progress[
            progress["location_at"].between(
                start + timedelta(seconds=4),
                start + timedelta(seconds=8),
                inclusive="left",
            )
        ]

        self.assertAlmostEqual(second_lap.iloc[1]["track_progress"], 0.25)
        self.assertTrue(second_lap["track_progress"].lt(1).all())

    def test_location_progress_merges_spurious_intermediate_lap_start(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-08-23T13:00:00Z"))
        laps = pd.DataFrame(
            [
                {"date_start": start, "driver_number": 1, "lap_number": 1},
                {
                    "date_start": start + timedelta(seconds=6),
                    "driver_number": 1,
                    "lap_number": 2,
                },
                {
                    "date_start": start + timedelta(seconds=9),
                    "driver_number": 1,
                    "lap_number": 3,
                },
                {
                    "date_start": start + timedelta(seconds=12),
                    "driver_number": 1,
                    "lap_number": 4,
                },
            ]
        )
        location = pd.DataFrame(
            [
                {
                    "date": start + timedelta(seconds=seconds),
                    "driver_number": 1,
                    "x": seconds,
                    "y": 0,
                    "z": 0,
                }
                for seconds in range(13)
            ]
        )

        progress = build_location_progress(location, laps)

        self.assertAlmostEqual(
            progress.loc[
                progress["location_at"].eq(start + timedelta(seconds=9)),
                "track_progress",
            ].iloc[0],
            0.5,
        )

    def test_location_progress_is_stable_when_future_samples_are_appended(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        laps = pd.DataFrame(
            [
                {"date_start": start, "driver_number": 1, "lap_number": 1},
                {
                    "date_start": start + timedelta(seconds=10),
                    "driver_number": 1,
                    "lap_number": 2,
                },
            ]
        )
        prefix = pd.DataFrame(
            [
                {"date": start, "driver_number": 1, "x": 0, "y": 0, "z": 0},
                {
                    "date": start + timedelta(seconds=5),
                    "driver_number": 1,
                    "x": 500,
                    "y": 0,
                    "z": 0,
                },
                {
                    "date": start + timedelta(seconds=10),
                    "driver_number": 1,
                    "x": 1000,
                    "y": 0,
                    "z": 0,
                },
                {
                    "date": start + timedelta(seconds=15),
                    "driver_number": 1,
                    "x": 1500,
                    "y": 0,
                    "z": 0,
                },
            ]
        )
        future = pd.DataFrame(
            [
                {
                    "date": start + timedelta(seconds=seconds),
                    "driver_number": 1,
                    "x": 1500 + seconds - 15,
                    "y": 0,
                    "z": 0,
                }
                for seconds in range(16, 23)
            ]
        )

        before = build_location_progress(prefix, laps)
        after = build_location_progress(pd.concat([prefix, future]), laps)

        pd.testing.assert_frame_equal(
            before,
            after[after["location_at"].le(start + timedelta(seconds=15))].reset_index(
                drop=True
            ),
        )

    def test_infer_race_window_prefers_session_status(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(minutes=90))
        laps = pd.DataFrame(
            [
                {
                    "driver_number": 1,
                    "lap_number": 1,
                    "date_start": start + timedelta(seconds=2),
                    "lap_duration": 100.0,
                }
            ]
        )
        race_control = pd.DataFrame(
            [
                {"date": start, "message": "SESSION STARTED"},
                {"date": finish, "message": "SESSION FINISHED"},
            ]
        )

        self.assertEqual(infer_race_window(laps, race_control), (start, finish))

    def test_safety_car_in_this_lap_turns_green_on_next_lap(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        next_lap = cast(pd.Timestamp, start + timedelta(minutes=4))
        control = pd.DataFrame(
            [
                {
                    "date": start + timedelta(seconds=30),
                    "lap_number": 1,
                    "message": "SAFETY CAR DEPLOYED",
                    "flag": None,
                    "category": "SafetyCar",
                },
                {
                    "date": start + timedelta(minutes=3),
                    "lap_number": 4,
                    "message": "SAFETY CAR IN THIS LAP",
                    "flag": None,
                    "category": "SafetyCar",
                },
            ]
        )
        laps = pd.DataFrame(
            [
                {"date_start": start, "lap_number": 1},
                {"date_start": next_lap, "lap_number": 5},
            ]
        )

        events = status_events(control, start, laps)

        self.assertEqual(events.iloc[-1]["status"], "GREEN")
        self.assertEqual(events.iloc[-1]["status_at"], next_lap)

    def test_build_replay_uses_only_past_measurements(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(seconds=120))
        datasets = self._datasets(start, finish)

        replay = build_replay(
            datasets,
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=10,
            max_staleness_seconds=5,
        )

        self.assertEqual(len(replay.frames), 13)
        middle = replay.frames[1]
        focus = next(car for car in middle.cars if car.driver_number == 2)
        # Der letzte Abstand ist 10 s alt, die Location-Messung jedoch aktuell.
        # OpenF1 sendet unveränderte Abstände nicht in garantierter Frequenz.
        self.assertEqual(focus.absolute_gap, 10.0)
        self.assertEqual(middle.status, "SC")

        final_focus = next(car for car in replay.frames[-1].cars if car.driver_number == 2)
        self.assertEqual(final_focus.absolute_gap, 30.0)

    def test_build_replay_keeps_drivers_when_intervals_are_unavailable(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(seconds=120))
        datasets = self._datasets(start, finish)
        datasets["intervals"] = pd.DataFrame(
            columns=["date", "driver_number", "gap_to_leader", "interval"]
        )

        replay = build_replay(
            datasets,
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=10,
            max_staleness_seconds=5,
        )

        final = replay.frames[-1]
        self.assertEqual({car.driver_number for car in final.cars}, {1, 2, 3})
        self.assertEqual(
            {car.displayed_gap for car in final.cars if car.position > 1},
            {"UNAVAILABLE"},
        )
        self.assertIsNone(final.projection)

    def test_neutralized_status_suppresses_optional_pit_projection(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(seconds=120))
        datasets = self._datasets(start, finish)

        replay = build_replay(
            datasets,
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=None,
            frame_seconds=10,
            max_staleness_seconds=5,
        )

        neutralized = next(frame for frame in replay.frames if frame.status == "SC")
        self.assertIsNone(neutralized.projection)

    def test_replay_cut_is_unchanged_by_future_laps_locations_and_stints(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(seconds=120))
        decision_time = cast(pd.Timestamp, start + timedelta(seconds=70))
        datasets = self._datasets(start, finish)
        baseline = build_replay(
            datasets,
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=10,
            max_staleness_seconds=15,
            decision_time=decision_time,
        )
        expanded = {name: frame.copy() for name, frame in datasets.items()}
        expanded["laps"] = pd.concat(
            [
                expanded["laps"],
                pd.DataFrame(
                    [
                        {
                            "date_start": finish + timedelta(seconds=10),
                            "driver_number": 1,
                            "lap_number": 4,
                            "lap_duration": 61.0,
                            "is_pit_out_lap": False,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        expanded["location"] = pd.concat(
            [
                expanded["location"],
                pd.DataFrame(
                    [
                        {
                            "date": decision_time + timedelta(seconds=5),
                            "driver_number": 2,
                            "x": 25,
                            "y": 0,
                            "z": 0,
                        }
                    ]
                ),
            ],
            ignore_index=True,
        )
        expanded["stints"].loc[:, "available_at"] = finish + timedelta(seconds=30)

        replay = build_replay(
            expanded,
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=10,
            max_staleness_seconds=15,
            decision_time=decision_time,
        )

        self.assertEqual(replay.race_end, decision_time)
        self.assertEqual(replay.frames, baseline.frames)
        self.assertTrue(
            all(car.compound is None for frame in replay.frames for car in frame.cars)
        )

    @staticmethod
    def _datasets(start: pd.Timestamp, finish: pd.Timestamp) -> dict[str, pd.DataFrame]:
        drivers = pd.DataFrame(
            [
                {"driver_number": 1, "name_acronym": "LEA", "team_colour": "111111"},
                {"driver_number": 2, "name_acronym": "FOC", "team_colour": "222222"},
                {"driver_number": 3, "name_acronym": "OTH", "team_colour": "333333"},
            ]
        )
        interval_rows = []
        for seconds in (0, 10, 20):
            date = start + timedelta(seconds=seconds)
            interval_rows.extend(
                [
                    {
                        "date": date,
                        "driver_number": 1,
                        "gap_to_leader": 0,
                        "interval": 0,
                    },
                    {
                        "date": date,
                        "driver_number": 3,
                        "gap_to_leader": 40.0,
                        "interval": 10.0,
                    },
                ]
            )
        interval_rows.extend(
            [
                {
                    "date": start,
                    "driver_number": 2,
                    "gap_to_leader": 10.0,
                    "interval": 10.0,
                },
                {
                    "date": finish,
                    "driver_number": 2,
                    "gap_to_leader": 30.0,
                    "interval": 30.0,
                },
            ]
        )
        positions = pd.DataFrame(
            [
                {"date": start, "driver_number": 1, "position": 1},
                {"date": start, "driver_number": 2, "position": 2},
                {"date": start, "driver_number": 3, "position": 3},
            ]
        )
        middle = start + (finish - start) / 2
        lap_duration = (finish - start).total_seconds() / 2
        laps = pd.DataFrame(
            [
                {
                    "date_start": date,
                    "driver_number": driver,
                    "lap_number": lap,
                    "lap_duration": duration if lap < 3 else None,
                    "is_pit_out_lap": False,
                }
                for driver, duration in (
                (1, lap_duration),
                (2, lap_duration + 1),
                (3, lap_duration + 2),
            )
                for lap, date in ((1, start), (2, middle), (3, finish))
            ]
        )
        location = pd.DataFrame(
            [
                {
                    "date": date,
                    "driver_number": driver,
                    "x": distance,
                    "y": 0,
                    "z": 0,
                }
                for driver in (1, 2, 3)
                for date, distance in (
                (
                    start + timedelta(seconds=seconds),
                    seconds / 3,
                )
                for seconds in range(
                0, int((finish - start).total_seconds()) + 1, 10
            )
            )
            ]
        )
        race_control = pd.DataFrame(
            [
                {
                    "date": start,
                    "message": "SESSION STARTED",
                    "flag": None,
                    "category": "SessionStatus",
                },
                {
                    "date": start + timedelta(seconds=5),
                    "message": "SAFETY CAR DEPLOYED",
                    "flag": None,
                    "category": "SafetyCar",
                },
                {
                    "date": finish,
                    "message": "SESSION FINISHED",
                    "flag": None,
                    "category": "SessionStatus",
                },
            ]
        )
        stints = pd.DataFrame(
            [
                {
                    "driver_number": driver,
                    "lap_start": 1,
                    "lap_end": 44,
                    "compound": "MEDIUM",
                    "tyre_age_at_start": 0,
                    "available_at": finish,
                }
                for driver in (1, 2, 3)
            ]
        )
        return {
            "sessions": pd.DataFrame([{"session_key": 1}]),
            "drivers": drivers,
            "laps": laps,
            "intervals": pd.DataFrame(interval_rows),
            "position": positions,
            "pit": pd.DataFrame(),
            "stints": stints,
            "race_control": race_control,
            "location": location,
        }


if __name__ == "__main__":
    unittest.main()
