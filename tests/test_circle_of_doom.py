"""Tests für Rekonstruktion und Darstellung des Circle of Doom."""

from __future__ import annotations

import unittest
from datetime import timedelta
from typing import Any, cast

import pandas as pd

from src.circle_of_doom import (
    DEFAULT_FRAME_SECONDS,
    DEFAULT_MAX_STALENESS_SECONDS,
    PLAYBACK_SPEEDS,
    CarState,
    _xy_for_progress,
    build_animation_post_script,
    build_location_progress,
    build_replay,
    create_figure,
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

    def test_round_clock_places_start_top_and_halfway_bottom(self) -> None:
        start_x, start_y = _xy_for_progress(0.0)
        halfway_x, halfway_y = _xy_for_progress(0.5)

        self.assertAlmostEqual(start_x, 0.0, places=7)
        self.assertAlmostEqual(start_y, 1.0, places=7)
        self.assertAlmostEqual(halfway_x, 0.0, places=7)
        self.assertAlmostEqual(halfway_y, -1.0, places=7)

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
            ]
        )

        progress = build_location_progress(location, laps)

        self.assertEqual(progress["track_progress"].tolist(), [0.0, 0.5, 1.0])

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
        finish = cast(pd.Timestamp, start + timedelta(seconds=20))
        datasets = self._datasets(start, finish)

        replay = build_replay(
            datasets,
            focus_driver=2,
            green_pit_loss=20.0,
            neutralized_pit_loss=12.0,
            frame_seconds=10,
            max_staleness_seconds=5,
        )

        self.assertEqual(len(replay.frames), 3)
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

        final_focus = next(car for car in replay.frames[2].cars if car.driver_number == 2)
        self.assertEqual(final_focus.absolute_gap, 30.0)

    def test_plotly_figure_contains_all_replay_frames(self) -> None:
        start = cast(pd.Timestamp, pd.Timestamp("2026-07-19T13:00:00Z"))
        replay = build_replay(
            self._datasets(
                start, cast(pd.Timestamp, start + timedelta(seconds=20))
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
        self.assertIn("Circle of Doom", figure.layout.title.text)

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
        laps = pd.DataFrame(
            [
                {
                    "date_start": date,
                    "driver_number": driver,
                    "lap_number": lap,
                    "lap_duration": duration if lap == 1 else None,
                    "is_pit_out_lap": False,
                }
                for driver, duration in ((1, 100.0), (2, 102.0), (3, 104.0))
                for lap, date in ((1, start), (2, finish))
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
                    (start, 0),
                    (start + (finish - start) / 2, 10),
                    (finish, 20),
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



