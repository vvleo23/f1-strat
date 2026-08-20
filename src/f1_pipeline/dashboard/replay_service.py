from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.dashboard.read_models import SessionBundle, load_session_bundle
from f1_pipeline.geometry import load_track_geometry, synthetic_track_geometry
from f1_pipeline.replay.circle_of_doom import (
    CarState,
    DATASET_ENDPOINTS,
    DEFAULT_FRAME_SECONDS,
    DEFAULT_GREEN_PIT_LOSS_SECONDS,
    DEFAULT_MAX_STALENESS_SECONDS,
    DEFAULT_NEUTRALIZED_PIT_LOSS_SECONDS,
    ReplayResult,
    build_animation_post_script,
    build_replay,
    create_figure,
)


@dataclass(frozen=True)
class ReplayView:
    session_key: int
    circle_html: str
    track_html: str | None
    positions: pd.DataFrame
    frame_count: int
    used_stored_geometry: bool
    manifest_path: Path


def replay_bundle(session_key: int) -> SessionBundle:
    return load_session_bundle(session_key, DATASET_ENDPOINTS, layer="raw")


def final_reconstructed_positions(replay: ReplayResult) -> pd.DataFrame:
    """Return a complete order without relying on the sparse final time frame."""
    terminal = max(
        replay.frames,
        key=lambda candidate_frame: (
            candidate_frame.lap_number,
            len(candidate_frame.cars),
            candidate_frame.date,
        ),
    )
    latest_by_driver: dict[int, CarState] = {}
    for frame in replay.frames:
        for car in frame.cars:
            latest_by_driver[car.driver_number] = car

    terminal_cars = sorted(
        terminal.cars, key=lambda candidate_car: candidate_car.position
    )
    terminal_drivers = {car.driver_number for car in terminal_cars}
    last_seen = sorted(
        (
            car
            for driver_number, car in latest_by_driver.items()
            if driver_number not in terminal_drivers
        ),
        key=lambda candidate_car: (
            -candidate_car.lap_number,
            candidate_car.position,
            candidate_car.driver_number,
        ),
    )
    rows: list[dict[str, Any]] = []
    for position, car in enumerate((*terminal_cars, *last_seen), start=1):
        rows.append(
            {
                "Position": position,
                "Driver": car.acronym,
                "Lap": car.lap_number,
                "Gap": car.displayed_gap,
                "Compound": car.compound,
                "Tyre age": car.tyre_age,
                "State": "Finish frame" if car.driver_number in terminal_drivers else "Last seen",
            }
        )
    return pd.DataFrame(rows)


def build_replay_view(
        session_key: int,
        *,
        focus_driver: int | None = None,
        frame_seconds: int = DEFAULT_FRAME_SECONDS,
) -> ReplayView:
    bundle = replay_bundle(session_key)
    datasets = bundle.frames
    driver_values = pd.Series(
        pd.to_numeric(datasets["drivers"]["driver_number"], errors="coerce"),
        index=datasets["drivers"].index,
    )
    driver_numbers = sorted(
        int(value) for value in driver_values.dropna().astype(int).unique()
    )
    if not driver_numbers:
        raise ValueError(f"Session {session_key} has no drivers.")
    selected_driver: int
    if focus_driver is not None and int(focus_driver) in driver_numbers:
        selected_driver = int(focus_driver)
    else:
        selected_driver = driver_numbers[0]
    driver_rows = datasets["drivers"][
        driver_values.eq(selected_driver)
    ]
    focus_acronym = (
        str(driver_rows.iloc[0].get("name_acronym") or selected_driver)
        if not driver_rows.empty
        else str(selected_driver)
    )
    replay = build_replay(
        datasets,
        focus_driver=selected_driver,
        green_pit_loss=DEFAULT_GREEN_PIT_LOSS_SECONDS,
        neutralized_pit_loss=DEFAULT_NEUTRALIZED_PIT_LOSS_SECONDS,
        frame_seconds=frame_seconds,
        max_staleness_seconds=DEFAULT_MAX_STALENESS_SECONDS,
    )
    circle = synthetic_track_geometry()
    stored = load_track_geometry(session_key)

    def render(geometry) -> str:
        figure = create_figure(
            replay,
            focus_driver=selected_driver,
            focus_acronym=focus_acronym,
            frame_seconds=frame_seconds,
            geometry=geometry,
            show_pit_projection=False,
        )
        return figure.to_html(
            full_html=False,
            include_plotlyjs=True,
            post_script=build_animation_post_script(
                replay,
                frame_seconds=frame_seconds,
                geometry=geometry,
            ),
        )

    circle_html = render(circle)
    track_html = render(stored) if stored is not None else None
    positions = final_reconstructed_positions(replay)
    return ReplayView(
        session_key=session_key,
        circle_html=circle_html,
        track_html=track_html,
        positions=positions,
        frame_count=len(replay.frames),
        used_stored_geometry=stored is not None,
        manifest_path=bundle.manifest_path,
    )
