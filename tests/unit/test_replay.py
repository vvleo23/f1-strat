"""Tests for temporal replay reconstruction and rendering."""

from __future__ import annotations

import math
import unittest
from datetime import timedelta
from typing import cast

import pandas as pd

from f1_pipeline.replay.circle_of_doom import (
    _active_gaps_known,
    CarState,
    build_location_progress,
    build_replay,
    driver_is_inactive,
    estimate_reference_lap_time,
    field_is_moving,
    infer_race_window,
    make_parquet_safe,
    project_pit_exit,
    RetirementSignals,
    _in_red_flag_restart,
    _retirement_context,
    reconstruct_absolute_gaps,
    retirement_signals,
    status_events,
)


class CircleOfDoomTest(unittest.TestCase):
    @staticmethod
    def _lap_starts(start, samples):
        return pd.DataFrame([
            {"date_start": start + timedelta(seconds=seconds),
             "driver_number": 1, "lap_number": lap}
            for seconds, lap in samples
        ])

    @staticmethod
    def _replay(datasets, **overrides):
        options = dict(
            focus_driver=2, green_pit_loss=20.0, neutralized_pit_loss=12.0,
            frame_seconds=10, max_staleness_seconds=5,
        )
        options.update(overrides)
        return build_replay(datasets, **options)

    def test_pit_projection_excludes_inactive_drivers(self) -> None:
        def car(
                driver: int,
                acronym: str,
                position: int,
                gap: float,
                inactive: bool = False,
        ) -> CarState:
            return CarState(
                driver_number=driver,
                acronym=acronym,
                team_colour="#808080",
                position=position,
                lap_number=10,
                lap_progress=0.5,
                absolute_gap=gap,
                displayed_gap=f"+{gap:.1f}s",
                interval=None,
                compound=None,
                tyre_age=None,
                recently_pitted=False,
                inactive=inactive,
            )

        projection = project_pit_exit(
            (
                car(1, "LEA", 1, 0.0),
                car(3, "OUT", 6, 5.0, inactive=True),
                car(87, "DNF", 5, 15.0, inactive=True),
                car(23, "ALB", 2, 20.0),
                car(43, "COL", 3, 25.0),
                car(11, "PER", 4, 40.0),
            ),
            focus_driver=23,
            pit_loss=10.0,
            reference_lap_time=80.0,
        )

        self.assertIsNotNone(projection)
        assert projection is not None
        self.assertEqual(projection.projected_position, 3)
        self.assertEqual(projection.ahead, "COL")
        self.assertEqual(projection.behind, "PER")

    def test_inactive_car_without_gap_does_not_block_pit_projection(self) -> None:
        def car(driver: int, gap: float, inactive: bool = False) -> CarState:
            return CarState(
                driver_number=driver, acronym=str(driver), team_colour="#808080",
                position=driver, lap_number=10, lap_progress=0.5,
                absolute_gap=gap, displayed_gap="", interval=None, compound=None,
                tyre_age=None, recently_pitted=False, inactive=inactive,
            )

        # A non-starter (e.g. Stroll, Sao Paulo 2024) never receives a gap.
        self.assertTrue(_active_gaps_known([car(1, 0.0), car(18, math.nan, inactive=True)]))
        self.assertFalse(_active_gaps_known([car(1, 0.0), car(2, math.nan)]))

    def test_field_must_resume_before_inactivity_timer_runs(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-08-23T13:00:00Z"))
        activity = {driver: (2.5, start, False) for driver in range(1, 11)}

        self.assertFalse(
            field_is_moving(
                activity,
                {driver: 2.5 for driver in range(1, 11)},
                "GREEN",
            )
        )
        self.assertTrue(
            field_is_moving(
                activity,
                {
                    driver: 2.51 if driver <= 5 else 2.5
                    for driver in range(1, 11)
                },
                "GREEN",
            )
        )

    def test_driver_becomes_inactive_only_after_green_stationary_timeout(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-08-23T13:00:00Z"))
        activity: dict[int, tuple[float, pd.Timestamp, bool]] = {}

        self.assertFalse(
            driver_is_inactive(
                activity,
                driver_number=1,
                race_progress=2.5,
                frame_time=start,
                status="GREEN",
            )
        )
        self.assertFalse(
            driver_is_inactive(
                activity,
                driver_number=1,
                race_progress=2.5,
                frame_time=start + timedelta(seconds=60),
                status="RED",
            )
        )
        self.assertFalse(
            driver_is_inactive(
                activity,
                driver_number=1,
                race_progress=2.5,
                frame_time=start + timedelta(seconds=149),
                status="GREEN",
            )
        )
        self.assertTrue(
            driver_is_inactive(
                activity,
                driver_number=1,
                race_progress=2.5,
                frame_time=start + timedelta(seconds=150),
                status="GREEN",
            )
        )
        self.assertFalse(
            driver_is_inactive(
                activity,
                driver_number=1,
                race_progress=2.51,
                frame_time=start + timedelta(seconds=151),
                status="GREEN",
            )
        )

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
        laps = self._lap_starts(start, [(0, 1), (10, 2), (20, 3)])
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

    def test_gap_reconstruction_does_not_bridge_missing_predecessor_gap(self) -> None:
        rows = pd.DataFrame([
            {"driver_number": 1, "position": 1, "gap_to_leader": 0, "interval": 0},
            {"driver_number": 2, "position": 2, "gap_to_leader": None, "interval": None},
            {"driver_number": 3, "position": 3, "gap_to_leader": None, "interval": 2},
        ])

        result = reconstruct_absolute_gaps(rows, reference_lap_time=100)

        self.assertTrue(pd.isna(result[2]["absolute_gap"]))

    def test_location_progress_keeps_moving_across_missing_lap_number(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-08-23T13:00:00Z"))
        laps = self._lap_starts(start, [(0, 1), (4, 2), (12, 4), (16, 5)])
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
        laps = self._lap_starts(start, [(0, 1), (4, 2), (8, 3)])
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
        laps = self._lap_starts(start, [(0, 1), (6, 2), (9, 3), (12, 4)])
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
        laps = self._lap_starts(start, [(0, 1), (10, 2)])
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

    def test_short_vsc_messages_create_vsc_phase(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        deployed = start + timedelta(minutes=30)
        ending = start + timedelta(minutes=31)
        control = pd.DataFrame(
            [
                {"date": deployed, "message": "VSC DEPLOYED", "flag": None, "category": "SafetyCar"},
                {"date": ending, "message": "VSC ENDING", "flag": None, "category": "SafetyCar"},
            ]
        )

        events = status_events(control, start)

        self.assertEqual(
            list(zip(events["status_at"], events["status"])),
            [(start, "GREEN"), (deployed, "VSC"), (ending, "GREEN")],
        )

    def test_second_vsc_after_vsc_ending_creates_new_phase(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        times = [start + timedelta(minutes=minute) for minute in (30, 31, 40, 42)]
        for deployed, ending in (
            ("VSC DEPLOYED", "VSC ENDING"),
            ("VIRTUAL SAFETY CAR DEPLOYED", "VIRTUAL SAFETY CAR ENDING"),
        ):
            with self.subTest(deployed=deployed):
                control = pd.DataFrame(
                    [
                        {"date": at, "message": message, "flag": None, "category": "SafetyCar"}
                        for at, message in zip(times, (deployed, ending, deployed, ending))
                    ]
                )

                events = status_events(control, start)

                self.assertEqual(
                    list(zip(events["status_at"], events["status"])),
                    [
                        (start, "GREEN"),
                        (times[0], "VSC"),
                        (times[1], "GREEN"),
                        (times[2], "VSC"),
                        (times[3], "GREEN"),
                    ],
                )

    def test_vsc_after_safety_car_in_this_lap_creates_new_phase(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        sc = start + timedelta(minutes=5)
        sc_in = start + timedelta(minutes=10)
        restart = start + timedelta(minutes=11)
        vsc = start + timedelta(minutes=30)
        second_sc = start + timedelta(minutes=50)
        control = pd.DataFrame(
            [
                {"date": sc, "lap_number": 3, "message": "SAFETY CAR DEPLOYED", "flag": None, "category": "SafetyCar"},
                {"date": sc_in, "lap_number": 5, "message": "SAFETY CAR IN THIS LAP", "flag": None, "category": "SafetyCar"},
                {"date": vsc, "lap_number": 15, "message": "VSC DEPLOYED", "flag": None, "category": "SafetyCar"},
                {"date": vsc + timedelta(minutes=1), "lap_number": 15, "message": "VSC ENDING", "flag": None, "category": "SafetyCar"},
                {"date": second_sc, "lap_number": 25, "message": "SAFETY CAR DEPLOYED", "flag": None, "category": "SafetyCar"},
            ]
        )
        laps = pd.DataFrame(
            [{"driver_number": 1, "lap_number": 6, "date_start": restart}]
        )

        events = status_events(control, start, laps)

        self.assertEqual(
            list(zip(events["status_at"], events["status"])),
            [
                (start, "GREEN"),
                (sc, "SC"),
                (restart, "GREEN"),
                (vsc, "VSC"),
                (vsc + timedelta(minutes=1), "GREEN"),
                (second_sc, "SC"),
            ],
        )

    def test_safety_car_in_this_lap_without_next_lap_keeps_safety_car(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        sc = start + timedelta(minutes=5)
        vsc = start + timedelta(minutes=30)
        control = pd.DataFrame(
            [
                {"date": sc, "lap_number": 3, "message": "SAFETY CAR DEPLOYED", "flag": None, "category": "SafetyCar"},
                {"date": sc + timedelta(minutes=5), "lap_number": 5, "message": "SAFETY CAR IN THIS LAP", "flag": None, "category": "SafetyCar"},
                {"date": vsc, "lap_number": 15, "message": "VSC DEPLOYED", "flag": None, "category": "SafetyCar"},
            ]
        )

        events = status_events(control, start)

        # Without the next lap start the end of the SC phase cannot be placed in time.
        self.assertEqual(
            list(zip(events["status_at"], events["status"])),
            [(start, "GREEN"), (sc, "SC"), (vsc, "VSC")],
        )

    def test_chequered_flag_does_not_create_red_phase(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        red = start + timedelta(minutes=20)
        resumed = start + timedelta(minutes=40)
        chequered = start + timedelta(minutes=90)
        control = pd.DataFrame(
            [
                {"date": red, "message": "RED FLAG", "flag": "RED", "category": "Flag"},
                {"date": resumed, "message": "GREEN LIGHT - PIT EXIT OPEN", "flag": "GREEN", "category": "Flag"},
                {"date": resumed + timedelta(minutes=5), "message": "INCIDENT INVOLVING CAR 6 (HAD) NOTED - RED FLAG INFRINGEMENT", "flag": None, "category": "Other"},
                {"date": chequered, "message": "CHEQUERED FLAG", "flag": "CHEQUERED", "category": "Flag"},
            ]
        )

        events = status_events(control, start)

        self.assertEqual(
            list(zip(events["status_at"], events["status"])),
            [(start, "GREEN"), (red, "RED"), (resumed, "GREEN")],
        )

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

        replay = self._replay(datasets)

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

        replay = self._replay(datasets)

        final = replay.frames[-1]
        self.assertEqual({car.driver_number for car in final.cars}, {1, 2, 3})
        self.assertEqual(
            {car.displayed_gap for car in final.cars if car.position > 1},
            {"UNAVAILABLE"},
        )
        self.assertIsNone(final.projection)

    def test_lap_without_start_time_does_not_break_replay(self) -> None:
        # OpenF1 keeps a lap-1 row without date_start for non-starters and
        # pit-lane starters (e.g. Sainz, Sao Paulo 2024).
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        datasets = self._datasets(start, start + timedelta(seconds=120))
        laps = datasets["laps"]
        missing = laps[laps["driver_number"].eq(3) & laps["lap_number"].eq(1)].copy()
        missing["date_start"] = None
        datasets["laps"] = pd.concat(
            [laps.drop(missing.index), missing], ignore_index=True
        )

        replay = self._replay(datasets)

        self.assertEqual({car.driver_number for car in replay.frames[-1].cars}, {1, 2, 3})

    def test_start_keeps_drivers_before_first_gap_and_position_updates(self) -> None:
        start = pd.Timestamp("2026-07-19T13:00:00Z")
        datasets = self._datasets(start, start + timedelta(seconds=120))
        datasets["intervals"]["date"] += timedelta(seconds=20)
        datasets["position"]["date"] += timedelta(seconds=20)

        replay = self._replay(datasets)

        self.assertEqual({car.driver_number for car in replay.frames[0].cars}, {1, 2, 3})
        self.assertEqual({car.displayed_gap for car in replay.frames[0].cars}, {"UNAVAILABLE"})
        self.assertIsNone(replay.frames[0].projection)

    def test_start_positions_share_reference_before_start_line_crossing(self) -> None:
        start = pd.Timestamp("2026-07-19T13:00:00Z")
        laps = pd.DataFrame([
            {"driver_number": driver, "lap_number": lap,
             "date_start": start + timedelta(seconds=seconds)}
            for driver, starts in [(1, [(0, 1), (10, 2), (20, 3)]),
                                   (2, [(2, 1), (12, 2), (22, 3)])]
            for seconds, lap in starts
        ])
        locations = pd.DataFrame([
            {"driver_number": driver, "date": start + timedelta(seconds=seconds),
             "x": x, "y": y, "z": 0}
            for driver, samples in [
                (1, [(0, 0, 0), (2, 8, 0), (3, 10, 0), (5, 10, 10),
                     (8, 0, 10), (10, 0, 0), (12, 10, 0), (15, 10, 10),
                     (18, 0, 10), (20, 0, 0)]),
                (2, [(0, 0, 2), (1, 0, 1), (2, 0, 0), (3, 10, 0),
                     (6, 10, 10), (9, 0, 10), (12, 0, 0),
                     (14, 10, 0), (17, 10, 10), (20, 0, 10), (22, 0, 0)]),
            ]
            for seconds, x, y in samples
        ])

        progress = build_location_progress(locations, laps, start)
        follower = progress[progress.driver_number.eq(2)].set_index("location_at")

        self.assertAlmostEqual(follower.loc[start, "track_progress"], 0.95)
        self.assertEqual(follower.loc[start, "lap_number_from_location"], 0)
        self.assertAlmostEqual(follower.loc[start + timedelta(seconds=2), "track_progress"], 0)
        same_position = progress[progress.location_at.eq(start + timedelta(seconds=3))]
        self.assertEqual(same_position.track_progress.tolist(), [0.25, 0.25])

    @classmethod
    def _open_lap_progress(cls, open_lap):
        # Two closed laps around a 10 x 10 square at (100, 100), then open lap 3.
        start = pd.Timestamp("2026-07-19T13:00:00Z")
        laps = cls._lap_starts(start, [(0, 1), (10, 2), (20, 3)])
        square = [(0, 100, 100), (2.5, 110, 100), (5, 110, 110), (7.5, 100, 110)]
        samples = [(lap * 10 + seconds, x, y) for lap in (0, 1) for seconds, x, y in square]
        locations = pd.DataFrame([
            {"driver_number": 1, "date": start + timedelta(seconds=seconds),
             "x": x, "y": y, "z": 0}
            for seconds, x, y in samples + [(20 + seconds, x, y) for seconds, x, y in open_lap]
        ])
        progress = build_location_progress(locations, laps, start)
        lap = progress[progress.lap_number_from_location.eq(3)]
        return lap.set_index((lap.location_at - start).dt.total_seconds() - 20)

    def test_open_lap_uses_position_on_reference_line(self) -> None:
        # A spin adds driven distance but no progress along the track.
        lap = self._open_lap_progress(
            [(0, 100, 100), (2, 110, 100), (3, 110, 105), (4, 110, 103), (5, 110, 105)]
        )

        self.assertEqual(lap.track_progress.round(3).tolist(), [0.0, 0.25, 0.375, 0.375, 0.375])
        self.assertAlmostEqual(lap.loc[5, "lap_distance_ratio"], 0.475)

    def test_open_lap_holds_position_for_off_line_and_placeholder_samples(self) -> None:
        lap = self._open_lap_progress(
            [(0, 100, 100), (2, 110, 100), (4, 500, 500), (6, 0, 0), (8, 110, 105)]
        )

        self.assertEqual(lap.track_progress.round(3).tolist(), [0.0, 0.25, 0.25, 0.25, 0.375])

    def test_open_lap_does_not_jump_back_behind_the_line(self) -> None:
        lap = self._open_lap_progress([(0, 100, 100), (0.5, 100, 100.4), (2, 110, 100)])

        self.assertEqual(lap.track_progress.round(3).tolist(), [0.0, 0.0, 0.25])

    def test_neutralized_status_suppresses_optional_pit_projection(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(seconds=120))
        datasets = self._datasets(start, finish)

        replay = self._replay(
            datasets,
            neutralized_pit_loss=None,
        )

        neutralized = next(frame for frame in replay.frames if frame.status == "SC")
        self.assertIsNone(neutralized.projection)

    def test_replay_uses_status_specific_safety_car_loss(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(seconds=120))
        datasets = self._datasets(start, finish)

        replay = self._replay(
            datasets,
            neutralized_pit_loss=None,
            sc_pit_loss=9.0,
            vsc_pit_loss=11.0,
        )

        neutralized = next(
            frame
            for frame in replay.frames
            if frame.status == "SC" and frame.projection is not None
        )
        self.assertEqual(neutralized.projection.pit_loss, 9.0)

    def test_replay_cut_is_unchanged_by_future_laps_locations_and_stints(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        finish = cast(pd.Timestamp, start + timedelta(seconds=120))
        decision_time = cast(pd.Timestamp, start + timedelta(seconds=70))
        datasets = self._datasets(start, finish)
        baseline = self._replay(
            datasets,
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

        replay = self._replay(
            expanded,
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


class RetirementRuleTest(unittest.TestCase):
    """Cross-source special rule: location keeps a crashed car moving."""

    START = cast(pd.Timestamp, pd.Timestamp("2026-09-06T13:00:00Z"))
    LAP = 40.0

    def test_rule_needs_timing_and_position_or_all_three_other_signals(self) -> None:
        self.assertFalse(RetirementSignals(line_contradiction=True).retired)
        self.assertFalse(
            RetirementSignals(position_collapse=True, incident_reported=True).retired
        )
        self.assertFalse(
            RetirementSignals(line_contradiction=True, incident_reported=True).retired
        )
        self.assertTrue(
            RetirementSignals(line_contradiction=True, position_collapse=True).retired
        )
        self.assertTrue(
            RetirementSignals(lap_overdue=True, position_collapse=True).retired
        )
        self.assertTrue(
            RetirementSignals(
                line_contradiction=True, lap_overdue=True, incident_reported=True
            ).retired
        )

    def _signals(self, **overrides):
        options = dict(
            frame_time=self.START + timedelta(seconds=100),
            status="GREEN",
            lap_started_at=self.START + timedelta(seconds=40),
            lap_distance_ratio=1.10,
            reference_lap_time=self.LAP,
            position_history=[
                (self.START + timedelta(seconds=90), 2),
                (self.START + timedelta(seconds=100), 9),
            ],
            incident_times=[self.START + timedelta(seconds=95)],
            excluded=False,
        )
        options.update(overrides)
        return retirement_signals(**options)

    def test_signals_for_crashed_car(self) -> None:
        signals = self._signals()
        self.assertTrue(signals.line_contradiction)
        self.assertFalse(signals.lap_overdue)
        self.assertTrue(signals.position_collapse)
        self.assertTrue(signals.incident_reported)
        self.assertTrue(signals.retired)

    def test_pit_window_chequered_flag_and_red_flag_exclude_the_rule(self) -> None:
        self.assertFalse(self._signals(excluded=True).retired)
        self.assertFalse(self._signals(status="RED").retired)
        self.assertFalse(self._signals(status="STOPPED").retired)

    def test_neutralized_laps_are_not_overdue_too_early(self) -> None:
        late = self.START + timedelta(seconds=40 + 80)
        self.assertTrue(self._signals(frame_time=late).lap_overdue)
        self.assertFalse(self._signals(frame_time=late, status="SC").lap_overdue)

    def test_old_position_drop_and_old_incident_expire(self) -> None:
        signals = self._signals(
            position_history=[
                (self.START + timedelta(seconds=40), 2),
                (self.START + timedelta(seconds=100), 9),
            ],
            incident_times=[self.START],
        )
        self.assertFalse(signals.position_collapse)
        self.assertFalse(signals.incident_reported)

    def test_red_flag_restart_suspends_rule_until_field_restarted(self) -> None:
        red = self.START + timedelta(minutes=5)
        restart = self.START + timedelta(minutes=30)
        field_lap = self.START + timedelta(minutes=35)
        lap_starts = sorted(
            [(self.START, driver) for driver in range(1, 5)]
            # Laps started while entering the pit lane under red.
            + [(red + timedelta(seconds=20), driver) for driver in range(1, 5)]
            # A single car right after the restart message does not end it.
            + [(restart + timedelta(seconds=5), 3)]
            + [(field_lap, 1), (field_lap + timedelta(seconds=3), 2)]
        )
        check = lambda at: _in_red_flag_restart(at, [red], [self.START, restart], lap_starts)
        self.assertFalse(check(red - timedelta(seconds=1)))
        self.assertTrue(check(red + timedelta(minutes=10)))
        self.assertTrue(check(restart + timedelta(minutes=2)))
        # Half the field restarted, grace period, then the rule resumes.
        self.assertTrue(check(field_lap + timedelta(seconds=60)))
        self.assertFalse(check(field_lap + timedelta(seconds=100)))

    def test_pit_window_opens_only_when_the_pit_entry_is_published(self) -> None:
        pit_at = self.START + timedelta(seconds=200)
        pits = pd.DataFrame([{"date": pit_at, "driver_number": 6}])

        _, _, windows, _, _ = _retirement_context(pd.DataFrame(), pits)

        # No look-ahead: nothing before the pit record is excluded.
        self.assertEqual(windows, {6: [(pit_at, pit_at + timedelta(seconds=150))]})

    def test_unknown_lap_start_gives_no_timing_signal(self) -> None:
        signals = self._signals(lap_started_at=None)
        self.assertFalse(signals.line_contradiction)
        self.assertFalse(signals.lap_overdue)

    def test_crashed_car_with_moving_location_becomes_inactive(self) -> None:
        crash = self.START + timedelta(seconds=1 + 78)
        replay = build_replay(
            self._season_like_datasets(crash),
            focus_driver=1,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=2,
            max_staleness_seconds=5,
        )
        inactive_since: dict[str, pd.Timestamp] = {}
        for frame in replay.frames:
            for car in frame.cars:
                if car.inactive:
                    inactive_since.setdefault(car.acronym, frame.date)
        self.assertIn("CRA", inactive_since)
        self.assertGreater(inactive_since["CRA"], crash)
        self.assertLess(inactive_since["CRA"], crash + timedelta(seconds=25))
        # The retired car stays frozen where its position collapse began
        # (just before the crash), not where the gliding location data went.
        crashed = [
            (frame.date, car)
            for frame in replay.frames
            for car in frame.cars
            if car.acronym == "CRA"
        ]
        frozen = [car for date, car in crashed if car.retired]
        self.assertTrue(frozen)
        self.assertEqual({(car.lap_number, car.lap_progress) for car in frozen}, {
            (frozen[0].lap_number, frozen[0].lap_progress)
        })
        before_crash = [car for date, car in crashed if date <= crash]
        self.assertIn(
            (frozen[0].lap_number, frozen[0].lap_progress),
            {(car.lap_number, car.lap_progress) for car in before_crash},
        )
        self.assertTrue(all(car.inactive for car in frozen))
        self.assertFalse(any(car.retired for date, car in crashed if date <= crash))
        # A missing OpenF1 lap row alone and a regular pit stop never retire a car.
        self.assertNotIn("MIS", inactive_since)
        self.assertNotIn("PIT", inactive_since)

    def _season_like_datasets(self, crash: pd.Timestamp) -> dict[str, pd.DataFrame]:
        start, lap = self.START, self.LAP
        names = {1: "LEA", 2: "CRA", 3: "THR", 4: "FOU", 5: "MIS", 6: "PIT", 7: "SEV", 8: "EIG"}
        radius = 1000.0
        end = start + timedelta(seconds=3 * lap + 20)
        laps, locations, positions, intervals = [], [], [], []
        for number in names:
            offset = float(number - 1)
            for lap_number in (1, 2, 3, 4):
                if number == 2 and lap_number >= 3:
                    continue  # crashed: timing never starts lap 3
                if number == 5 and lap_number == 3:
                    continue  # OpenF1 lap row missing, car keeps racing
                laps.append({
                    "date_start": start + timedelta(seconds=offset + lap * (lap_number - 1)),
                    "driver_number": number,
                    "lap_number": lap_number,
                    "lap_duration": lap,
                    "is_pit_out_lap": False,
                })
            seconds = 0.0
            driven = 0.0
            while start + timedelta(seconds=seconds) <= end:
                at = start + timedelta(seconds=seconds)
                if seconds > offset:
                    driven += 0.5 if number == 2 and at > crash else 1.0
                angle = 2 * math.pi * driven / lap
                locations.append({
                    "date": at,
                    "driver_number": number,
                    "x": radius * math.cos(angle),
                    "y": radius * math.sin(angle),
                    "z": 0.0,
                })
                seconds += 1.0
            positions.append({"date": start, "driver_number": number, "position": number})
            intervals.append({
                "date": start,
                "driver_number": number,
                "gap_to_leader": offset,
                "interval": 1.0 if number > 1 else 0.0,
            })
        for step, number in enumerate((3, 4, 5, 6, 7, 8), start=1):
            positions.append({
                "date": crash + timedelta(seconds=step),
                "driver_number": number,
                "position": number - 1,
            })
            # As each car passes, the crashed car loses one place (OpenF1 feed).
            positions.append({
                "date": crash + timedelta(seconds=step),
                "driver_number": 2,
                "position": 2 + step,
            })
        # Regular pit stop: PIT drops to last place, covered by the pit record.
        pit_at = start + timedelta(seconds=50)
        positions.append({"date": pit_at + timedelta(seconds=5), "driver_number": 6, "position": 8})
        race_control = pd.DataFrame([
            {"date": start, "message": "SESSION STARTED", "flag": None, "category": "SessionStatus"},
            {
                "date": crash + timedelta(seconds=3),
                "message": "DOUBLE YELLOW IN TRACK SECTOR 15",
                "flag": "DOUBLE YELLOW",
                "category": "Flag",
            },
            {"date": end, "message": "SESSION FINISHED", "flag": None, "category": "SessionStatus"},
        ])
        return {
            "sessions": pd.DataFrame([{"session_key": 1}]),
            "drivers": pd.DataFrame([
                {"driver_number": number, "name_acronym": acronym, "team_colour": "111111"}
                for number, acronym in names.items()
            ]),
            "laps": pd.DataFrame(laps),
            "intervals": pd.DataFrame(intervals),
            "position": pd.DataFrame(positions),
            "pit": pd.DataFrame([{"date": pit_at, "driver_number": 6, "lap_number": 2}]),
            "stints": pd.DataFrame(
                columns=["driver_number", "lap_start", "lap_end", "compound", "tyre_age_at_start"]
            ),
            "race_control": race_control,
            "location": pd.DataFrame(locations),
        }


if __name__ == "__main__":
    unittest.main()
