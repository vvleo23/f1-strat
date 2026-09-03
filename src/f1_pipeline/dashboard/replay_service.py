from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.dashboard.read_models import SessionBundle, load_session_bundle
from f1_pipeline.geometry import (
    TrackGeometry,
    TrackGeometryError,
    load_track_geometry,
    synthetic_track_geometry,
)
from f1_pipeline.replay.circle_of_doom import (
    CarState,
    DATASET_ENDPOINTS,
    DEFAULT_FRAME_SECONDS,
    DEFAULT_MAX_STALENESS_SECONDS,
    ReplayResult,
    build_replay,
)

REPLAY_REQUIRED_ENDPOINTS = (
    "sessions",
    "drivers",
    "laps",
    "position",
    "stints",
    "location",
)
REPLAY_OPTIONAL_ENDPOINTS = ("intervals", "pit", "race_control")


@dataclass(frozen=True)
class ReplayView:
    session_key: int
    payload: dict[str, Any]
    frame_count: int
    used_stored_geometry: bool
    manifest_path: Path


def replay_bundle(session_key: int) -> SessionBundle:
    bundle = load_session_bundle(
        session_key,
        REPLAY_REQUIRED_ENDPOINTS,
        layer="raw",
        optional_endpoints=REPLAY_OPTIONAL_ENDPOINTS,
    )
    frames = dict(bundle.frames)
    empty_columns = {
        "intervals": ("date", "driver_number", "gap_to_leader", "interval"),
        "pit": ("date", "driver_number", "lap_number"),
        "race_control": ("date", "message", "category", "flag"),
    }
    for endpoint in DATASET_ENDPOINTS:
        if endpoint not in frames and endpoint in empty_columns:
            frames[endpoint] = pd.DataFrame(columns=empty_columns[endpoint])
    return SessionBundle(
        session_key=bundle.session_key,
        status=bundle.status,
        frames=frames,
        manifest=bundle.manifest,
        manifest_path=bundle.manifest_path,
        missing=bundle.missing,
    )


def final_reconstructed_positions(replay: ReplayResult) -> pd.DataFrame:
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
    terminal_drivers = {int(car.driver_number) for car in terminal_cars}
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
                "State": (
                    "Finish frame"
                    if car.driver_number in terminal_drivers
                    else "Last seen"
                ),
            }
        )
    return pd.DataFrame(rows)


def _number(value: Any, precision: int = 4) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return round(number, precision) if math.isfinite(number) else None


def _geometry_payload(geometry: TrackGeometry) -> dict[str, Any]:
    return {
        "label": geometry.label,
        "quality": geometry.quality_status,
        "points": [
            [round(float(x), 6), round(float(y), 6)] for x, y in geometry.points
        ],
    }


def replay_payload(
    replay: ReplayResult,
    *,
    focus_driver: int,
    focus_acronym: str,
    frame_seconds: int,
    circle: TrackGeometry,
    stored: TrackGeometry | None,
    pit_loss_seconds: float | None,
) -> dict[str, Any]:
    frames: list[dict[str, Any]] = []
    for frame in replay.frames:
        projection = frame.projection
        frames.append(
            {
                "date": frame.date.isoformat(),
                "lap": frame.lap_number,
                "status": frame.status,
                "reference_lap_time": _number(frame.reference_lap_time),
                "cars": [
                    [
                        car.driver_number,
                        car.acronym,
                        car.team_colour,
                        car.position,
                        car.lap_number,
                        round(car.lap_progress, 6),
                        _number(car.absolute_gap),
                        car.displayed_gap,
                        _number(car.interval),
                        car.compound,
                        car.tyre_age,
                        car.recently_pitted,
                    ]
                    for car in frame.cars
                ],
                "projection": (
                    {
                        "driver": projection.driver_number,
                        "loss": _number(projection.pit_loss),
                        "gap": _number(projection.projected_gap),
                        "progress": round(projection.projected_progress, 6),
                        "position": projection.projected_position,
                        "ahead": projection.ahead,
                        "gap_ahead": _number(projection.gap_ahead),
                        "behind": projection.behind,
                        "gap_behind": _number(projection.gap_behind),
                    }
                    if projection is not None
                    else None
                ),
            }
        )
    return {
        "frame_seconds": frame_seconds,
        "race_start": replay.race_start.isoformat(),
        "race_end": replay.race_end.isoformat(),
        "focus_driver": focus_driver,
        "focus_acronym": focus_acronym,
        "pit_loss_seconds": _number(pit_loss_seconds),
        "frames": frames,
        "geometries": {
            "circle": _geometry_payload(circle),
            "track": _geometry_payload(stored) if stored is not None else None,
        },
    }


def build_replay_view(
        session_key: int,
        *,
        season: int | None = None,
        meeting_key: int | None = None,
        circuit_id: str | None = None,
        focus_driver: int | None = None,
        frame_seconds: int = DEFAULT_FRAME_SECONDS,
        decision_time: str | pd.Timestamp | None = None,
        pit_loss_seconds: float | None = None,
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
    selected_driver = (
        int(focus_driver)
        if focus_driver is not None and int(focus_driver) in driver_numbers
        else driver_numbers[0]
    )
    driver_rows = datasets["drivers"][driver_values.eq(selected_driver)]
    focus_acronym = (
        str(driver_rows.iloc[0].get("name_acronym") or selected_driver)
        if not driver_rows.empty
        else str(selected_driver)
    )
    replay = build_replay(
        datasets,
        focus_driver=selected_driver,
        green_pit_loss=pit_loss_seconds,
        neutralized_pit_loss=None,
        frame_seconds=frame_seconds,
        max_staleness_seconds=DEFAULT_MAX_STALENESS_SECONDS,
        decision_time=decision_time,
    )
    circle = synthetic_track_geometry()
    try:
        stored = load_track_geometry(
            session_key,
            season=season,
            meeting_key=meeting_key,
            circuit_id=circuit_id,
        )
    except TrackGeometryError:
        stored = None
    payload = replay_payload(
        replay,
        focus_driver=selected_driver,
        focus_acronym=focus_acronym,
        frame_seconds=frame_seconds,
        circle=circle,
        stored=stored,
        pit_loss_seconds=pit_loss_seconds,
    )
    payload["replay_status"] = bundle.status
    payload["missing_endpoints"] = list(bundle.missing)
    return ReplayView(
        session_key=session_key,
        payload=payload,
        frame_count=len(replay.frames),
        used_stored_geometry=stored is not None,
        manifest_path=bundle.manifest_path,
    )
