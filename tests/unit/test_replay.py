"""Tests for temporal replay reconstruction and rendering."""

from __future__ import annotations

import unittest
from datetime import timedelta
from typing import Any, cast

import pandas as pd

from f1_pipeline.replay.circle_of_doom import (
    DEFAULT_FRAME_SECONDS,
    DEFAULT_MAX_STALENESS_SECONDS,
    PLAYBACK_SPEEDS,
    CarState,
    build_animation_post_script,
    build_location_progress,
    build_replay,
    create_figure,
    estimate_reference_lap_time,
    infer_race_window,
    make_parquet_safe,
    parse_openf1_datetimes,
    parse_lap_deficit,
    project_pit_exit,
    reconstruct_absolute_gaps,
    status_events,
)


class CircleOfDoomTest(unittest.TestCase):
    def test_smooth_replay_defaults_match_openf1_frequency(self) -> None:
        self.assertEqual(DEFAULT_FRAME_SECONDS, 4)
        self.assertEqual(DEFAULT_MAX_STALENESS_SECONDS, 8)
        self.assertEqual(PLAYBACK_SPEEDS, (1, 2, 5, 10))

    def test_parse_openf1_datetimes_accepts_mixed_iso_precision(self) -> None:
        values = pd.Series(
            ["2026-07-19T13:03:52+00:00", "2026-07-19T13:03:52.073000+00:00"]
        )

        parsed = parse_openf1_datetimes(values)

        self.assertTrue(parsed.notna().all())
        self.assertEqual((parsed.iloc[1] - parsed.iloc[0]).total_seconds(), 0.073)

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

    def test_parse_lap_deficit(self) -> None:
        self.assertEqual(parse_lap_deficit("+1 LAP"), 1)
        self.assertEqual(parse_lap_deficit(" +2 laps "), 2)
        self.assertIsNone(parse_lap_deficit(12.5))
        self.assertIsNone(parse_lap_deficit("+12.5"))

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

    def test_project_pit_exit_finds_neighbours_and_position(self) -> None:
        cars = tuple(
            self._car(number, acronym, position, gap)
            for number, acronym, position, gap in (
                (1, "LEA", 1, 0.0),
                (2, "FOC", 2, 10.0),
                (3, "AHD", 3, 25.0),
                (4, "BHD", 4, 35.0),
            )
        )

        projection = project_pit_exit(
            cars, focus_driver=2, pit_loss=20.0, reference_lap_time=100.0
        )

        self.assertIsNotNone(projection)
        assert projection is not None
        self.assertEqual(projection.projected_gap, 30.0)
        self.assertAlmostEqual(projection.projected_progress, 0.9)
        self.assertEqual(projection.projected_position, 3)
        self.assertEqual(projection.ahead, "AHD")
        self.assertEqual(projection.gap_ahead, 5.0)
        self.assertEqual(projection.behind, "BHD")
        self.assertEqual(projection.gap_behind, 5.0)

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

        self.assertEqual(progress["track_progress"].tolist(), [0.0, 0.5, 0.0])

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

    def test_build_replay_uses_only_past_measurements_and_sc_pit_loss(self) -> None:
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

        self.assertEqual(len(replay.frames), 7)
        middle = replay.frames[1]
        focus = next(car for car in middle.cars if car.driver_number == 2)
        # Der letzte Abstand ist 10 s alt, die Location-Messung jedoch aktuell.
        # OpenF1 sendet unveränderte Abstände nicht in garantierter Frequenz.
        self.assertEqual(focus.absolute_gap, 10.0)
        self.assertEqual(middle.status, "SC")
        self.assertIsNotNone(middle.projection)
        assert middle.projection is not None
        self.assertEqual(middle.projection.pit_loss, 12.0)
        self.assertEqual(middle.projection.projected_gap, 22.0)

        final_focus = next(car for car in replay.frames[-1].cars if car.driver_number == 2)
        self.assertEqual(final_focus.absolute_gap, 30.0)

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

    def test_plotly_figure_contains_all_replay_frames(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        replay = build_replay(
            self._datasets(
                start, cast(pd.Timestamp, start + timedelta(seconds=120))
            ),
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=10,
            max_staleness_seconds=15,
        )

        figure = create_figure(
            replay,
            focus_driver=2,
            focus_acronym="FOC",
            frame_seconds=10,
        )

        self.assertEqual(len(figure.frames), len(replay.frames))
        self.assertEqual(len(figure.data), 6)
        self.assertIn("Race replay", figure.layout.title.text)

        driver_order = list(cast(Any, figure.data[3]).customdata)
        self.assertEqual(driver_order, sorted(driver_order))
        self.assertTrue(driver_order)
        for frame in figure.frames:
            cars_trace = cast(Any, frame.data[0])
            self.assertEqual(list(cars_trace.customdata), driver_order)
            self.assertEqual(len(cars_trace.x), len(driver_order))
            self.assertEqual(len(cars_trace.y), len(driver_order))

        buttons = figure.layout.updatemenus[0].buttons
        self.assertEqual(
            [button.label for button in buttons],
            ["▶ 1×", "▶ 2×", "▶ 5×", "▶ 10×", "Ⅱ Pause"],
        )
        self.assertTrue(all(button.method == "skip" for button in buttons))
        self.assertTrue(
            all(step.method == "skip" for step in figure.layout.sliders[0].steps)
        )
        first_slider_command = figure.layout.sliders[0].steps[0].args[0]
        self.assertEqual(first_slider_command["frameIndex"], 0)

        script = build_animation_post_script(replay, frame_seconds=10)
        self.assertIn("requestAnimationFrame", script)
        self.assertIn('"fallbackFrameMilliseconds":10000', script)
        self.assertIn('"speeds":[1,2,5,10]', script)
        self.assertIn("interpolateProgress", script)
        self.assertIn("plotly_sliderchange", script)

    def test_plotly_figure_can_hide_pit_projection(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        replay = build_replay(
            self._datasets(
                start, cast(pd.Timestamp, start + timedelta(seconds=120))
            ),
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=10,
            max_staleness_seconds=15,
        )

        figure = create_figure(
            replay,
            focus_driver=2,
            focus_acronym="FOC",
            frame_seconds=10,
            show_pit_projection=False,
        )

        self.assertEqual(len(figure.data), 6)
        self.assertEqual(len(cast(Any, figure.data[4]).x), 0)
        self.assertEqual(len(cast(Any, figure.data[5]).x), 0)
        content = figure.to_json().casefold()
        self.assertNotIn("pit projection", content)
        self.assertNotIn("immediate stop", content)
        self.assertNotIn("pit→", content)

    @staticmethod
    def _car(number: int, acronym: str, position: int, gap: float) -> CarState:
        return CarState(
            driver_number=number,
            acronym=acronym,
            team_colour="#123456",
            position=position,
            lap_number=10,
            lap_progress=0.1,
            absolute_gap=gap,
            displayed_gap="LEADER" if gap == 0 else f"+{gap:.1f}s",
            interval=None,
            compound="MEDIUM",
            tyre_age=5,
            recently_pitted=False,
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
