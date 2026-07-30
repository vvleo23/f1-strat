"""Create an interactive OpenF1 race replay."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
import time
import webbrowser
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any, Iterable, cast

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from f1_pipeline.geometry import (
    TrackGeometry,
    TrackGeometryError,
    load_track_geometry,
    point_at_progress,
    synthetic_track_geometry,
)
from f1_pipeline.settings import ARTIFACTS_DIR, RAW_DATA_DIR

BASE_URL = "https://api.openf1.org/v1"
DEFAULT_SESSION_KEY = 11334
DEFAULT_FOCUS_DRIVER = "ANT"
DEFAULT_GREEN_PIT_LOSS_SECONDS = 20.0
DEFAULT_NEUTRALIZED_PIT_LOSS_SECONDS = 12.0
DEFAULT_FRAME_SECONDS = 4
DEFAULT_MAX_STALENESS_SECONDS = 8
PLAYBACK_SPEEDS = (1, 2, 5, 10)
REQUEST_TIMEOUT_SECONDS = 90
# Community limit: no more than 30 requests per minute.
MIN_REQUEST_INTERVAL_SECONDS = 2.1
DATASET_ENDPOINTS = (
    "sessions",
    "drivers",
    "laps",
    "intervals",
    "position",
    "pit",
    "stints",
    "race_control",
    "location",
)
LAPPED_GAP_PATTERN = re.compile(r"^\+(\d+)\s+LAPS?$", re.IGNORECASE)
FALLBACK_TEAM_COLOUR = "#808080"
SOURCE_TITLE = (
    "OpenF1: primary replay timeline · FastF1: laps/tyres/weather cross-check · "
    "Open-Meteo/Wikidata: planned · RainViewer: deferred"
)
SOURCE_ANNOTATION = (
    "<b>Why these sources?</b><br>"
    "<b>OpenF1</b> — primary timeline: session, gaps, positions, race control<br>"
    "<b>FastF1</b> — historical cross-check: laps, tyres, weather, telemetry<br>"
    "<b>Open-Meteo</b> — forecasts; planned, not used in this replay<br>"
    "<b>RainViewer</b> — radar and nowcast; deferred, not used<br>"
    "<b>Wikidata</b> — circuit reference coordinates; planned, not used in this replay"
)


class CircleOfDoomError(RuntimeError):
    """Describe a readable error while creating the replay."""


@dataclass(frozen=True)
class CarState:
    """Point-in-time state of a car in the replay."""

    driver_number: int
    acronym: str
    team_colour: str
    position: int
    lap_number: int
    lap_progress: float
    absolute_gap: float
    displayed_gap: str
    interval: float | None
    compound: str | None
    tyre_age: int | None
    recently_pitted: bool


@dataclass(frozen=True)
class PitProjection:
    """Hypothetical pit exit for the selected driver."""

    driver_number: int
    pit_loss: float
    projected_gap: float
    projected_progress: float
    projected_position: int
    ahead: str | None
    gap_ahead: float | None
    behind: str | None
    gap_behind: float | None


@dataclass(frozen=True)
class ReplayFrame:
    """All data rendered for one point in time."""

    date: pd.Timestamp
    lap_number: int
    status: str
    cars: tuple[CarState, ...]
    projection: PitProjection | None


@dataclass(frozen=True)
class ReplayResult:
    """Ergebnis der Replay-Rekonstruktion."""

    frames: tuple[ReplayFrame, ...]
    reference_lap_time: float
    race_start: pd.Timestamp
    race_end: pd.Timestamp


class OpenF1Client:
    """Kleiner OpenF1-Client mit Retries, Backoff und Rate-Limit-Abstand."""

    def __init__(self) -> None:
        self._enable_system_trust_store()
        retry = Retry(
            total=4,
            connect=4,
            read=4,
            status=4,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            respect_retry_after_header=True,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session = requests.Session()
        self.session.mount("https://", adapter)
        self.session.headers.update({"User-Agent": "f1-event-pipeline/1.0"})
        self._last_request_at = 0.0

    @staticmethod
    def _enable_system_trust_store() -> None:
        try:
            import truststore
        except ImportError as exc:
            raise CircleOfDoomError(
                "The 'truststore' package is missing. Run 'pip install -r requirements.txt'."
            ) from exc
        truststore.inject_into_ssl()

    def get_json(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Fetch an OpenF1 endpoint securely and validate its response."""
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)

        url = f"{BASE_URL}/{endpoint.lstrip('/')}"
        try:
            response: requests.Response | None = None
            for attempt in range(3):
                response = self.session.get(
                    url, params=params, timeout=REQUEST_TIMEOUT_SECONDS
                )
                self._last_request_at = time.monotonic()
                if response.status_code != 422 or endpoint != "location":
                    break
                if attempt < 2:
                    wait_seconds = 5 * (attempt + 1)
                    print(
                        f"location temporarily rejected; retrying in {wait_seconds}s ..."
                    )
                    time.sleep(wait_seconds)
            assert response is not None
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.SSLError as exc:
            raise CircleOfDoomError(
                "TLS certificate validation for OpenF1 failed. "
                "The system certificate store could not validate the connection."
            ) from exc
        except requests.exceptions.Timeout as exc:
            raise CircleOfDoomError(
                f"Request timed out while fetching endpoint '{endpoint}'."
            ) from exc
        except requests.exceptions.JSONDecodeError as exc:
            raise CircleOfDoomError(
                f"OpenF1 endpoint '{endpoint}' did not return valid JSON."
            ) from exc
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unbekannt"
            raise CircleOfDoomError(
                f"OpenF1 returned HTTP {status} for endpoint '{endpoint}'."
            ) from exc
        except requests.exceptions.RequestException as exc:
            raise CircleOfDoomError(
                f"Network error while fetching endpoint '{endpoint}': {exc}"
            ) from exc

        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise CircleOfDoomError(
                f"Unexpected response format from OpenF1 endpoint '{endpoint}'."
            )
        return payload


def cache_path(session_key: int, endpoint: str) -> Path:
    """Return the Parquet cache path for an OpenF1 dataset."""
    return RAW_DATA_DIR / f"openf1_{session_key}_{endpoint}.parquet"


def location_driver_cache_path(session_key: int, driver_number: int) -> Path:
    """Return the resumable location cache path for a driver."""
    return RAW_DATA_DIR / (
        f"openf1_{session_key}_location_driver_{driver_number}.parquet"
    )


def make_parquet_safe(endpoint: str, frame: pd.DataFrame) -> pd.DataFrame:
    """Normalisiert OpenF1-Spalten, die Zahlen und Statusstrings mischen."""
    safe = frame.copy()
    if endpoint == "intervals":
        for column in ("gap_to_leader", "interval"):
            if column in safe.columns:
                safe[column] = safe[column].map(
                    lambda value: None if pd.isna(value) else str(value)
                )
    return safe


def load_session_datasets(
    session_key: int,
    *,
    refresh: bool = False,
    client: OpenF1Client | None = None,
) -> dict[str, pd.DataFrame]:
    """Load all replay datasets from the cache or OpenF1."""
    api = client or OpenF1Client()
    datasets: dict[str, pd.DataFrame] = {}

    for endpoint in DATASET_ENDPOINTS:
        path = cache_path(session_key, endpoint)
        if path.exists() and not refresh:
            frame = pd.read_parquet(path)
            source = "Cache"
        else:
            if endpoint == "location":
                location_frames: list[pd.DataFrame] = []
                driver_numbers = sorted(
                    datasets["drivers"]["driver_number"].astype(int).unique()
                )
                for index, driver_number in enumerate(driver_numbers, start=1):
                    driver_path: Path = location_driver_cache_path(
                        session_key, driver_number
                    )
                    if driver_path.exists() and not refresh:
                        driver_frame = pd.read_parquet(driver_path)
                        driver_source = "Cache"
                    else:
                        driver_payload = api.get_json(
                            endpoint,
                            {
                                "session_key": session_key,
                                "driver_number": driver_number,
                            },
                        )
                        driver_frame = make_parquet_safe(
                            endpoint, pd.DataFrame(driver_payload)
                        )
                        driver_frame.to_parquet(driver_path, index=False)
                        driver_source = "OpenF1"
                    print(
                        f"location: Driver {driver_number} "
                        f"({index}/{len(driver_numbers)}, {driver_source}, "
                        f"{len(driver_frame):,} rows)"
                    )
                    location_frames.append(driver_frame)
                frame = pd.concat(location_frames, ignore_index=True)
            else:
                payload = api.get_json(endpoint, {"session_key": session_key})
                frame = make_parquet_safe(endpoint, pd.DataFrame(payload))
            frame.to_parquet(path, index=False)
            source = "OpenF1"
        datasets[endpoint] = frame
        print(f"{endpoint}: {len(frame):,} rows ({source})")

    required_non_empty = {
        "sessions",
        "drivers",
        "laps",
        "intervals",
        "position",
        "location",
    }
    empty_required = sorted(name for name in required_non_empty if datasets[name].empty)
    if empty_required:
        raise CircleOfDoomError(
            "Required datasets are empty: " + ", ".join(empty_required)
        )
    return datasets


def parse_lap_deficit(value: Any) -> int | None:
    """Parst OpenF1-Werte wie '+1 LAP' oder '+2 LAPS'."""
    if not isinstance(value, str):
        return None
    match = LAPPED_GAP_PATTERN.fullmatch(value.strip())
    return int(match.group(1)) if match else None


def as_finite_float(value: Any) -> float | None:
    """Konvertiert einen Wert nur dann, wenn er eine endliche Zahl ist."""
    if isinstance(value, (bool, np.bool_)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def normalize_team_colour(value: Any) -> str:
    """Normalisiert OpenF1-Teamfarben auf CSS-Hexwerte."""
    if isinstance(value, str):
        colour = value.strip().lstrip("#")
        if re.fullmatch(r"[0-9a-fA-F]{6}", colour):
            return f"#{colour.upper()}"
    return FALLBACK_TEAM_COLOUR


def parse_openf1_datetimes(values: pd.Series) -> pd.Series:
    """Parst gemischte OpenF1-ISO-Zeitstempel mit optionalen Mikrosekunden."""
    return pd.to_datetime(values, format="mixed", utc=True, errors="coerce")


def infer_race_window(
    laps: pd.DataFrame, race_control: pd.DataFrame
) -> tuple[pd.Timestamp, pd.Timestamp]:
    """Infer the actual race start and end from race control and laps."""
    parsed_laps = laps.copy()
    parsed_laps["date_start"] = parse_openf1_datetimes(parsed_laps["date_start"])

    parsed_control = race_control.copy()
    if not parsed_control.empty:
        parsed_control["date"] = parse_openf1_datetimes(parsed_control["date"])
        messages = parsed_control["message"].fillna("").astype(str).str.upper()
        starts = parsed_control.loc[messages.eq("SESSION STARTED"), "date"]
        finishes = parsed_control.loc[messages.eq("SESSION FINISHED"), "date"]
    else:
        starts = pd.Series(dtype="datetime64[ns, UTC]")
        finishes = pd.Series(dtype="datetime64[ns, UTC]")

    lap_one = parsed_laps.loc[parsed_laps["lap_number"].eq(1), "date_start"].dropna()
    all_starts = parsed_laps["date_start"].dropna()
    if not starts.empty:
        race_start = cast(pd.Timestamp, starts.min())
    elif not lap_one.empty:
        race_start = cast(pd.Timestamp, lap_one.min())
    elif not all_starts.empty:
        race_start = cast(pd.Timestamp, all_starts.min())
    else:
        raise CircleOfDoomError("Aus den Rundendaten konnte kein Rennstart ermittelt werden.")

    if not finishes.empty:
        race_end = cast(pd.Timestamp, finishes.max())
    else:
        if "lap_duration" not in parsed_laps.columns:
            raise CircleOfDoomError("Die Rundendaten enthalten keine Rundenzeiten.")
        durations = pd.to_numeric(parsed_laps["lap_duration"], errors="coerce")
        lap_ends = parsed_laps["date_start"] + pd.to_timedelta(durations, unit="s")
        valid_ends = lap_ends.dropna()
        if valid_ends.empty:
            raise CircleOfDoomError("Aus den Rundendaten konnte kein Rennende ermittelt werden.")
        race_end = cast(pd.Timestamp, valid_ends.max())

    if race_end <= race_start:
        raise CircleOfDoomError("Das ermittelte Rennende liegt nicht nach dem Rennstart.")
    return race_start, race_end


def estimate_reference_lap_time(laps: pd.DataFrame, pits: pd.DataFrame) -> float:
    """Estimate a robust representative lap time for the circular view."""
    working = laps.copy()
    working["lap_duration"] = pd.to_numeric(working["lap_duration"], errors="coerce")
    working["lap_number"] = pd.to_numeric(working["lap_number"], errors="coerce")
    working["driver_number"] = pd.to_numeric(working["driver_number"], errors="coerce")

    excluded: set[tuple[int, int]] = set()
    if not pits.empty:
        for row in pits[["driver_number", "lap_number"]].itertuples(index=False):
            driver = int(cast(Any, row.driver_number))
            lap = int(cast(Any, row.lap_number))
            excluded.add((driver, lap))
            excluded.add((driver, lap + 1))

    valid = working[
        working["lap_duration"].notna()
        & working["lap_duration"].between(60, 240)
        & working["lap_number"].ge(5)
        & ~working.get("is_pit_out_lap", False).fillna(False).astype(bool)
    ].copy()
    if excluded:
        keys = list(zip(valid["driver_number"].astype(int), valid["lap_number"].astype(int)))
        valid = valid[[key not in excluded for key in keys]]

    if valid.empty:
        valid = working[working["lap_duration"].notna() & working["lap_duration"].between(60, 240)]
    if valid.empty:
        raise CircleOfDoomError("No plausible reference lap time is available.")

    # Das untere Quartil reduziert Verkehrseinfluss, ohne eine Qualifying-Runde zu verwenden.
    return float(valid["lap_duration"].quantile(0.25))


def status_events(
    race_control: pd.DataFrame,
    race_start: pd.Timestamp,
    laps: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Build a compact status timeline for GREEN, SC, VSC, and RED."""
    events: list[dict[str, Any]] = [{"status_at": race_start, "status": "GREEN"}]
    if race_control.empty:
        return pd.DataFrame(events)

    control = race_control.copy()
    control["date"] = parse_openf1_datetimes(control["date"])
    control = control.sort_values("date")
    parsed_laps = laps.copy() if laps is not None else pd.DataFrame()
    if not parsed_laps.empty:
        parsed_laps["date_start"] = parse_openf1_datetimes(
            parsed_laps["date_start"]
        )
    current = "GREEN"

    for row in control.itertuples(index=False):
        event_date = cast(pd.Timestamp, cast(Any, row.date))
        message = str(getattr(row, "message", "") or "").upper()
        flag = str(getattr(row, "flag", "") or "").upper()
        category = str(getattr(row, "category", "") or "").upper()
        new_status = current

        if flag == "RED" or "RED FLAG" in message:
            new_status = "RED"
        elif "VIRTUAL SAFETY CAR DEPLOYED" in message:
            new_status = "VSC"
        elif "SAFETY CAR DEPLOYED" in message:
            new_status = "SC"
        elif "SAFETY CAR IN THIS LAP" in message:
            lap_number = as_finite_float(getattr(row, "lap_number", None))
            if lap_number is not None and not parsed_laps.empty:
                next_lap_starts = parsed_laps.loc[
                    pd.to_numeric(parsed_laps["lap_number"], errors="coerce").eq(
                        int(lap_number) + 1
                    ),
                    "date_start",
                ].dropna()
                if not next_lap_starts.empty:
                    events.append(
                        {
                            "status_at": cast(pd.Timestamp, next_lap_starts.min()),
                            "status": "GREEN",
                        }
                    )
        elif "VIRTUAL SAFETY CAR ENDING" in message:
            events.append({"status_at": event_date, "status": "GREEN"})
        elif flag == "GREEN" and event_date >= race_start:
            new_status = "GREEN"
        elif category == "SESSIONSTATUS" and "SESSION STARTED" in message:
            new_status = "GREEN"

        if new_status != current:
            current = new_status
            events.append({"status_at": event_date, "status": current})

    return pd.DataFrame(events).drop_duplicates("status_at", keep="last").sort_values("status_at")


def reconstruct_absolute_gaps(
    rows: pd.DataFrame, reference_lap_time: float
) -> list[dict[str, Any]]:
    """Reconstruct absolute gaps, including lapped cars."""
    records = cast(list[dict[str, Any]], rows.to_dict("records"))
    records.sort(
        key=lambda row: (
            as_finite_float(row.get("position"))
            if as_finite_float(row.get("position")) is not None
            else math.inf,
            int(row.get("driver_number", 999)),
        )
    )

    previous_gap: float | None = None
    for record in records:
        raw_gap = record.get("gap_to_leader")
        numeric_gap = as_finite_float(raw_gap)
        interval = as_finite_float(record.get("interval"))
        lap_deficit = parse_lap_deficit(raw_gap)
        position = as_finite_float(record.get("position"))

        if position == 1 or numeric_gap == 0:
            absolute_gap = 0.0
        elif numeric_gap is not None:
            absolute_gap = max(0.0, numeric_gap)
        elif lap_deficit is not None:
            candidates = [lap_deficit * reference_lap_time]
            if previous_gap is not None and interval is not None:
                candidates.append(previous_gap + max(0.0, interval))
            absolute_gap = max(candidates)
        elif previous_gap is not None and interval is not None:
            absolute_gap = previous_gap + max(0.0, interval)
        else:
            absolute_gap = math.nan

        record["absolute_gap"] = absolute_gap
        if math.isfinite(absolute_gap):
            previous_gap = absolute_gap

    finite_records = [record for record in records if math.isfinite(record["absolute_gap"])]
    finite_records.sort(key=lambda row: (row["absolute_gap"], int(row["driver_number"])))
    for derived_position, record in enumerate(finite_records, start=1):
        raw_position = as_finite_float(record.get("position"))
        record["resolved_position"] = int(raw_position) if raw_position else derived_position
    return finite_records


def project_pit_exit(
    cars: Iterable[CarState],
    focus_driver: int,
    pit_loss: float,
    reference_lap_time: float,
) -> PitProjection | None:
    """Projiziert Position und direkte Nachbarn nach einem sofortigen Stopp."""
    car_list = sorted(cars, key=lambda car: car.absolute_gap)
    focus = next((car for car in car_list if car.driver_number == focus_driver), None)
    if focus is None:
        return None

    projected_gap = focus.absolute_gap + pit_loss
    projected_progress = (
        focus.lap_progress - pit_loss / reference_lap_time
    ) % 1.0
    others = [car for car in car_list if car.driver_number != focus_driver]
    ahead_candidates = [car for car in others if car.absolute_gap <= projected_gap]
    behind_candidates = [car for car in others if car.absolute_gap > projected_gap]
    ahead = max(ahead_candidates, key=lambda car: car.absolute_gap, default=None)
    behind = min(behind_candidates, key=lambda car: car.absolute_gap, default=None)
    projected_position = 1 + sum(car.absolute_gap < projected_gap for car in others)

    return PitProjection(
        driver_number=focus_driver,
        pit_loss=pit_loss,
        projected_gap=projected_gap,
        projected_progress=projected_progress,
        projected_position=projected_position,
        ahead=ahead.acronym if ahead else None,
        gap_ahead=(projected_gap - ahead.absolute_gap) if ahead else None,
        behind=behind.acronym if behind else None,
        gap_behind=(behind.absolute_gap - projected_gap) if behind else None,
    )


def build_location_progress(location: pd.DataFrame, laps: pd.DataFrame) -> pd.DataFrame:
    """Calculate geometric lap progress for each location measurement.

    The normalization uses the observed x/y/z path length within the same lap.
    It is only used for visualization and does not affect strategy or pit-position calculations.
    """
    locations = location.copy()
    locations["location_at"] = parse_openf1_datetimes(locations["date"])
    locations["driver_number"] = locations["driver_number"].astype(int)
    for column in ("x", "y", "z"):
        locations[column] = pd.to_numeric(locations[column], errors="coerce")
    locations = locations.dropna(subset=["location_at", "x", "y", "z"])

    lap_starts = laps.copy()
    lap_starts["lap_started_at"] = parse_openf1_datetimes(lap_starts["date_start"])
    lap_starts["driver_number"] = lap_starts["driver_number"].astype(int)
    lap_starts = lap_starts.dropna(subset=["lap_started_at", "lap_number"])

    progress_parts: list[pd.DataFrame] = []
    for driver_number, driver_laps in lap_starts.groupby("driver_number"):
        driver_locations = locations[
            locations["driver_number"].eq(int(driver_number))
        ].sort_values("location_at")
        starts = driver_laps.sort_values("lap_started_at")[
            ["lap_started_at", "lap_number"]
        ].drop_duplicates("lap_started_at", keep="last")
        start_rows = list(starts.itertuples(index=False))

        for current, following in zip(start_rows, start_rows[1:]):
            start_at = cast(pd.Timestamp, cast(Any, current.lap_started_at))
            end_at = cast(pd.Timestamp, cast(Any, following.lap_started_at))
            samples = driver_locations[
                driver_locations["location_at"].between(
                    start_at, end_at, inclusive="both"
                )
            ].copy()
            if len(samples) < 3:
                continue

            coordinates = samples[["x", "y", "z"]].to_numpy(dtype=float)
            elapsed = samples["location_at"].diff().dt.total_seconds().to_numpy()
            segments = np.linalg.norm(np.diff(coordinates, axis=0), axis=1)
            segment_times = elapsed[1:]
            valid_speed = segment_times > 0
            speeds = np.divide(
                segments,
                segment_times,
                out=np.zeros_like(segments),
                where=valid_speed,
            )
            positive_speeds = speeds[(speeds > 0) & np.isfinite(speeds)]
            if positive_speeds.size:
                median_speed = float(np.median(positive_speeds))
                implausible = speeds > median_speed * 6
                segments[implausible] = median_speed * segment_times[implausible]

            cumulative = np.concatenate(([0.0], np.cumsum(segments)))
            total_distance = float(cumulative[-1])
            if not math.isfinite(total_distance) or total_distance <= 0:
                continue
            samples["track_progress"] = np.clip(cumulative / total_distance, 0, 1)
            samples["lap_number_from_location"] = int(current.lap_number)
            progress_parts.append(
                samples[
                    [
                        "location_at",
                        "driver_number",
                        "track_progress",
                        "lap_number_from_location",
                    ]
                ]
            )

    if not progress_parts:
        raise CircleOfDoomError(
            "Could not calculate lap progress from OpenF1 location data."
        )
    progress = pd.concat(progress_parts, ignore_index=True)
    return (
        progress.sort_values(["location_at", "driver_number"])
        .drop_duplicates(["location_at", "driver_number"], keep="last")
        .reset_index(drop=True)
    )


def _prepare_asof_grid(
    datasets: dict[str, pd.DataFrame],
    race_start: pd.Timestamp,
    race_end: pd.Timestamp,
    frame_seconds: int,
    max_staleness_seconds: int,
) -> pd.DataFrame:
    """Align all inputs to a shared replay timeline."""
    driver_numbers = sorted(datasets["drivers"]["driver_number"].astype(int).unique())
    frame_times = pd.date_range(race_start, race_end, freq=f"{frame_seconds}s", tz="UTC")
    if frame_times.empty or frame_times[-1] < race_end:
        frame_times = frame_times.append(pd.DatetimeIndex([race_end]))

    grid = pd.MultiIndex.from_product(
        [frame_times, driver_numbers], names=["date", "driver_number"]
    ).to_frame(index=False)
    grid = grid.sort_values(["date", "driver_number"]).reset_index(drop=True)

    intervals = datasets["intervals"].copy()
    intervals["observed_at"] = parse_openf1_datetimes(intervals["date"])
    intervals["driver_number"] = intervals["driver_number"].astype(int)
    intervals = intervals[
        ["observed_at", "driver_number", "gap_to_leader", "interval"]
    ].drop_duplicates(["observed_at", "driver_number"], keep="last")
    intervals = intervals.sort_values(["observed_at", "driver_number"])
    grid = pd.merge_asof(
        grid,
        intervals,
        left_on="date",
        right_on="observed_at",
        by="driver_number",
        direction="backward",
    )
    # OpenF1 sometimes repeats unchanged gaps only after roughly one lap
    # (for example, 0 s for the leader). The last value known from the past
    # therefore remains valid. Higher-frequency location data independently
    # determines whether a car can still be rendered on the circle.

    location_progress = build_location_progress(datasets["location"], datasets["laps"])
    grid = pd.merge_asof(
        grid.sort_values(["date", "driver_number"]),
        location_progress,
        left_on="date",
        right_on="location_at",
        by="driver_number",
        direction="backward",
        tolerance=timedelta(seconds=max_staleness_seconds),
    )

    positions = datasets["position"].copy()
    positions["position_at"] = parse_openf1_datetimes(positions["date"])
    positions["driver_number"] = positions["driver_number"].astype(int)
    positions = positions[
        ["position_at", "driver_number", "position"]
    ].drop_duplicates(["position_at", "driver_number"], keep="last")
    positions = positions.sort_values(["position_at", "driver_number"])
    grid = pd.merge_asof(
        grid.sort_values(["date", "driver_number"]),
        positions,
        left_on="date",
        right_on="position_at",
        by="driver_number",
        direction="backward",
    )

    laps = datasets["laps"].copy()
    laps["lap_started_at"] = parse_openf1_datetimes(laps["date_start"])
    laps["driver_number"] = laps["driver_number"].astype(int)
    laps = laps[
        ["lap_started_at", "driver_number", "lap_number"]
    ].drop_duplicates(["lap_started_at", "driver_number"], keep="last")
    laps = laps.sort_values(["lap_started_at", "driver_number"])
    grid = pd.merge_asof(
        grid.sort_values(["date", "driver_number"]),
        laps,
        left_on="date",
        right_on="lap_started_at",
        by="driver_number",
        direction="backward",
    )

    pits = datasets["pit"].copy()
    if not pits.empty:
        pits["pit_at"] = parse_openf1_datetimes(pits["date"])
        pits["driver_number"] = pits["driver_number"].astype(int)
        pit_events = pits[["pit_at", "driver_number"]].sort_values(
            ["pit_at", "driver_number"]
        )
        grid = pd.merge_asof(
            grid.sort_values(["date", "driver_number"]),
            pit_events,
            left_on="date",
            right_on="pit_at",
            by="driver_number",
            direction="backward",
            tolerance=timedelta(seconds=max(frame_seconds, 20)),
        )
    else:
        grid["pit_at"] = pd.NaT

    statuses = status_events(
        datasets["race_control"], race_start, datasets["laps"]
    )
    grid = pd.merge_asof(
        grid.sort_values("date"),
        statuses,
        left_on="date",
        right_on="status_at",
        direction="backward",
    )
    return grid.sort_values(["date", "driver_number"]).reset_index(drop=True)


def _build_stint_lookup(stints: pd.DataFrame) -> dict[int, list[dict[str, Any]]]:
    lookup: dict[int, list[dict[str, Any]]] = {}
    if stints.empty:
        return lookup
    for row in cast(list[dict[str, Any]], stints.to_dict("records")):
        lookup.setdefault(int(row["driver_number"]), []).append(row)
    for rows in lookup.values():
        rows.sort(key=lambda row: int(row.get("lap_start") or 0))
    return lookup


def _tyre_at_lap(
    stint_lookup: dict[int, list[dict[str, Any]]], driver: int, lap: int
) -> tuple[str | None, int | None]:
    for stint in stint_lookup.get(driver, []):
        start = int(stint.get("lap_start") or 0)
        end_value = as_finite_float(stint.get("lap_end"))
        end = int(end_value) if end_value is not None else math.inf
        if start <= lap <= end:
            compound = str(stint.get("compound") or "").upper() or None
            age_start = as_finite_float(stint.get("tyre_age_at_start"))
            tyre_age = int(age_start + lap - start) if age_start is not None else None
            return compound, tyre_age
    return None, None


def build_replay(
    datasets: dict[str, pd.DataFrame],
    *,
    focus_driver: int,
    green_pit_loss: float,
    neutralized_pit_loss: float,
    frame_seconds: int,
    max_staleness_seconds: int,
) -> ReplayResult:
    """Reconstruct all race replay frames without future measurements."""
    if frame_seconds <= 0:
        raise ValueError("frame_seconds must be greater than zero.")
    if max_staleness_seconds <= 0:
        raise ValueError("max_staleness_seconds must be greater than zero.")
    if green_pit_loss < 0 or neutralized_pit_loss < 0:
        raise ValueError("Pit-loss values must not be negative.")

    race_start, race_end = infer_race_window(datasets["laps"], datasets["race_control"])
    reference_lap_time = estimate_reference_lap_time(datasets["laps"], datasets["pit"])
    grid = _prepare_asof_grid(
        datasets,
        race_start,
        race_end,
        frame_seconds,
        max_staleness_seconds,
    )

    drivers = datasets["drivers"].drop_duplicates("driver_number", keep="last").copy()
    driver_meta = {
        int(cast(Any, row.driver_number)): {
            "acronym": str(row.name_acronym or row.driver_number),
            "team_colour": normalize_team_colour(row.team_colour),
        }
        for row in drivers.itertuples(index=False)
    }
    if focus_driver not in driver_meta:
        raise CircleOfDoomError(f"Driver {focus_driver} is not present in this session.")

    stint_lookup = _build_stint_lookup(datasets["stints"])
    frames: list[ReplayFrame] = []

    for date, snapshot in grid.groupby("date", sort=True):
        active = snapshot[
            snapshot["observed_at"].notna() & snapshot["track_progress"].notna()
        ].copy()
        if active.empty:
            continue
        reconstructed = reconstruct_absolute_gaps(active, reference_lap_time)
        cars: list[CarState] = []

        for record in reconstructed:
            driver_number = int(record["driver_number"])
            meta = driver_meta.get(
                driver_number,
                {"acronym": str(driver_number), "team_colour": FALLBACK_TEAM_COLOUR},
            )
            lap_value = as_finite_float(record.get("lap_number"))
            lap_number = max(1, int(lap_value)) if lap_value is not None else 1
            compound, tyre_age = _tyre_at_lap(stint_lookup, driver_number, lap_number)
            raw_gap = record.get("gap_to_leader")
            if isinstance(raw_gap, str) and parse_lap_deficit(raw_gap) is not None:
                displayed_gap = raw_gap.upper()
            elif record["absolute_gap"] == 0:
                displayed_gap = "LEADER"
            else:
                displayed_gap = f"+{record['absolute_gap']:.1f}s"

            cars.append(
                CarState(
                    driver_number=driver_number,
                    acronym=meta["acronym"],
                    team_colour=meta["team_colour"],
                    position=int(record["resolved_position"]),
                    lap_number=lap_number,
                    lap_progress=float(record["track_progress"]) % 1.0,
                    absolute_gap=float(record["absolute_gap"]),
                    displayed_gap=displayed_gap,
                    interval=as_finite_float(record.get("interval")),
                    compound=compound,
                    tyre_age=tyre_age,
                    recently_pitted=pd.notna(record.get("pit_at")),
                )
            )

        # A stable order is required for browser-side interpolation:
        # derselbe Array-Index muss in jedem Frame denselben Fahrer bezeichnen.
        cars.sort(key=lambda car: car.driver_number)
        status = str(active["status"].dropna().iloc[0]) if active["status"].notna().any() else "GREEN"
        pit_loss = (
            neutralized_pit_loss if status in {"SC", "VSC"} else green_pit_loss
        )
        projection = project_pit_exit(
            cars, focus_driver, pit_loss, reference_lap_time
        )
        lap_number = max((car.lap_number for car in cars), default=1)
        frames.append(
            ReplayFrame(
                date=cast(pd.Timestamp, date),
                lap_number=lap_number,
                status=status,
                cars=tuple(cars),
                projection=projection,
            )
        )

    if not frames:
        raise CircleOfDoomError("Es konnten keine Replay-Frames rekonstruiert werden.")
    return ReplayResult(tuple(frames), reference_lap_time, race_start, race_end)


def resolve_driver_number(drivers: pd.DataFrame, selector: str) -> int:
    """Resolve a driver number or acronym."""
    normalized = selector.strip().upper()
    if normalized.isdigit():
        number = int(normalized)
        if number in set(drivers["driver_number"].astype(int)):
            return number
    matches = drivers[
        drivers["name_acronym"].fillna("").astype(str).str.upper().eq(normalized)
    ]
    if len(matches) == 1:
        return int(matches.iloc[0]["driver_number"])
    available = ", ".join(sorted(drivers["name_acronym"].dropna().astype(str).unique()))
    raise CircleOfDoomError(
        f"Driver '{selector}' was not found uniquely. Available: {available}"
    )


def _angle_for_progress(progress: float) -> float:
    """Ordnet 0 % oben, 25 % rechts, 50 % unten und 75 % links an."""
    return math.pi / 2 - 2 * math.pi * (progress % 1.0)


def _xy_for_progress(progress: float, radius: float = 1.0) -> tuple[float, float]:
    angle = _angle_for_progress(progress)
    return radius * math.cos(angle), radius * math.sin(angle)


def _projection_description(projection: PitProjection | None) -> str:
    if projection is None:
        return "No current projection"
    neighbours: list[str] = []
    if projection.ahead:
        neighbours.append(f"{projection.gap_ahead:.1f}s behind {projection.ahead}")
    if projection.behind:
        neighbours.append(f"{projection.gap_behind:.1f}s ahead of {projection.behind}")
    return " · ".join(neighbours) if neighbours else "No direct neighbours"


def _frame_traces(
    frame: ReplayFrame,
    reference_lap_time: float,
    focus_driver: int,
    driver_order: tuple[int, ...] | None = None,
    geometry: TrackGeometry | None = None,
) -> tuple[go.Scatter, go.Scatter, go.Scatter]:
    track_geometry = geometry or synthetic_track_geometry()
    x: list[float | None] = []
    y: list[float | None] = []
    labels: list[str] = []
    colours: list[str] = []
    sizes: list[int] = []
    symbols: list[str] = []
    line_colours: list[str] = []
    line_widths: list[int] = []
    hovers: list[str] = []

    cars_by_driver = {car.driver_number: car for car in frame.cars}
    ordered_drivers = driver_order or tuple(sorted(cars_by_driver))
    for driver_number in ordered_drivers:
        car = cars_by_driver.get(driver_number)
        if car is None:
            x.append(None)
            y.append(None)
            labels.append("")
            colours.append(FALLBACK_TEAM_COLOUR)
            sizes.append(17)
            symbols.append("circle")
            line_colours.append("#FFFFFF")
            line_widths.append(1)
            hovers.append("")
            continue
        car_x, car_y = point_at_progress(track_geometry.points, car.lap_progress)
        x.append(car_x)
        y.append(car_y)
        labels.append(car.acronym)
        colours.append(car.team_colour)
        sizes.append(22 if car.driver_number == focus_driver else 17)
        symbols.append("diamond" if car.recently_pitted else "circle")
        line_colours.append("#FFD54F" if car.driver_number == focus_driver else "#FFFFFF")
        line_widths.append(4 if car.driver_number == focus_driver else 1)
        tyre = car.compound or "unknown"
        tyre_age = f"{car.tyre_age} laps" if car.tyre_age is not None else "unknown"
        hovers.append(
            f"<b>P{car.position} {car.acronym}</b><br>"
            f"Gap: {car.displayed_gap}<br>Lap: {car.lap_number}<br>"
            f"Lap progress: {car.lap_progress * 100:.1f}%<br>"
            f"Tyre: {tyre}, age: {tyre_age}"
        )

    cars_trace = go.Scattergl(
        x=x,
        y=y,
        mode="markers+text",
        text=labels,
        textposition="middle center",
        textfont={"color": "white", "size": 9, "family": "Arial Black"},
        marker={
            "color": colours,
            "size": sizes,
            "symbol": symbols,
            "line": {"color": line_colours, "width": line_widths},
        },
        hovertext=hovers,
        hoverinfo="text",
        customdata=list(ordered_drivers),
        name="Cars",
    )

    projection = frame.projection
    if projection is None:
        projection_trace = go.Scattergl(
            x=[], y=[], mode="markers", name="Pit projection"
        )
        arc_trace = go.Scatter(x=[], y=[], mode="lines", name="Pit loss")
    else:
        projection_x, projection_y = point_at_progress(
            track_geometry.points, projection.projected_progress, offset=0.08
        )
        projection_trace = go.Scattergl(
            x=[projection_x],
            y=[projection_y],
            mode="markers+text",
            text=[f"PIT→P{projection.projected_position}"],
            textposition="top center",
            textfont={"color": "#FFD54F", "size": 12},
            marker={
                "color": "#111111",
                "size": 19,
                "symbol": "x",
                "line": {"color": "#FFD54F", "width": 4},
            },
            hovertext=[
                f"<b>Projected pit exit P{projection.projected_position}</b><br>"
                f"Pit Loss: {projection.pit_loss:.1f}s<br>"
                f"Gap to leader: +{projection.projected_gap:.1f}s<br>"
                f"{_projection_description(projection)}"
            ],
            hoverinfo="text",
            name="Hypothetical pit-exit projection",
        )
        focus = next(
            car for car in frame.cars if car.driver_number == projection.driver_number
        )
        progress_loss = projection.pit_loss / reference_lap_time
        arc_progress = np.linspace(
            focus.lap_progress, focus.lap_progress - progress_loss, 30
        )
        arc_points = [
            point_at_progress(track_geometry.points, progress, offset=0.08)
            for progress in arc_progress
        ]
        arc_trace = go.Scatter(
            x=[point[0] for point in arc_points],
            y=[point[1] for point in arc_points],
            mode="lines",
            line={"color": "#FFD54F", "width": 3, "dash": "dot"},
            hoverinfo="skip",
            name="Assumed pit loss (not a forecast)",
        )
    return cars_trace, projection_trace, arc_trace


def _frame_title(
    frame: ReplayFrame,
    focus_acronym: str,
    geometry_label: str = "synthetic circle fallback",
) -> str:
    projection = frame.projection
    clock = frame.date.strftime("%H:%M:%S UTC")
    if projection:
        projection_text = (
            f"{focus_acronym}: immediate stop → P{projection.projected_position} "
            f"(+{projection.projected_gap:.1f}s; Pit Loss {projection.pit_loss:.1f}s)"
        )
    else:
        projection_text = f"{focus_acronym}: no fresh gap measurement"
    return (
        f"Race replay · Lap {frame.lap_number} · {clock} · {frame.status}"
        f"<br><sup>{SOURCE_TITLE}<br>Track geometry: {geometry_label}<br>{projection_text} · "
        f"{_projection_description(projection)} · Hypothetical pit-exit projection — not a forecast</sup>"
    )


def _replay_driver_order(replay: ReplayResult) -> tuple[int, ...]:
    """Return the canonical driver order for all animation frames."""
    return tuple(
        sorted({car.driver_number for frame in replay.frames for car in frame.cars})
    )


def build_animation_post_script(
    replay: ReplayResult,
    *,
    frame_seconds: int,
    geometry: TrackGeometry | None = None,
) -> str:
    """Create WebGL interpolation and playback controls for the HTML output."""
    track_geometry = geometry or synthetic_track_geometry()
    driver_order = _replay_driver_order(replay)
    frames_payload: list[dict[str, Any]] = []
    for frame in replay.frames:
        cars_by_driver = {car.driver_number: car for car in frame.cars}
        frames_payload.append(
            {
                "name": f"frame-{len(frames_payload)}",
                "date": frame.date.isoformat(),
                "progress": [
                    (
                        round(cars_by_driver[number].lap_progress, 8)
                        if number in cars_by_driver
                        else None
                    )
                    for number in driver_order
                ],
            }
        )

    payload = json.dumps(
        {
            "fallbackFrameMilliseconds": frame_seconds * 1000,
            "frames": frames_payload,
            "geometry": [
                [round(float(x), 6), round(float(y), 6)]
                for x, y in track_geometry.points
            ],
            "speeds": list(PLAYBACK_SPEEDS),
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return f"""
(() => {{
    "use strict";
    const plot = document.getElementById("{{plot_id}}");
    const replay = {payload};
    const carsTraceIndex = 3;
    const lastFrameIndex = replay.frames.length - 1;
    let currentFrameIndex = 0;
    let segmentFraction = 0;
    let segmentStartedAt = null;
    let speed = 1;
    let playing = false;
    let animationRequest = null;
    let internalSliderUpdates = 0;

    const finiteProgress = (value) => Number.isFinite(value);
    const trackGeometry = replay.geometry;

    function pointAtProgress(progress, offset = 0) {{
        if (!Array.isArray(trackGeometry) || trackGeometry.length < 3) {{
            const angle = Math.PI / 2 - 2 * Math.PI * progress;
            return [Math.cos(angle), Math.sin(angle)];
        }}
        const wrapped = ((progress % 1) + 1) % 1;
        const segmentCount = trackGeometry.length - 1;
        const position = wrapped * segmentCount;
        const index = Math.min(Math.floor(position), segmentCount - 1);
        const fraction = position - index;
        const start = trackGeometry[index];
        const end = trackGeometry[index + 1];
        let x = start[0] + (end[0] - start[0]) * fraction;
        let y = start[1] + (end[1] - start[1]) * fraction;
        const dx = end[0] - start[0];
        const dy = end[1] - start[1];
        const length = Math.hypot(dx, dy);
        if (offset && length > 0) {{
            x += (-dy / length) * offset;
            y += (dx / length) * offset;
        }}
        return [x, y];
    }}

    function frameDurationMilliseconds(fromIndex, toIndex) {{
        const from = Date.parse(replay.frames[fromIndex].date);
        const to = Date.parse(replay.frames[toIndex].date);
        const measured = to - from;
        const raceMilliseconds = Number.isFinite(measured) && measured > 0
            ? measured
            : replay.fallbackFrameMilliseconds;
        return Math.max(1, raceMilliseconds / speed);
    }}

    function interpolateProgress(from, to, fraction) {{
        let delta = to - from;
        if (delta < -0.5) delta += 1;
        if (delta > 0.5) delta -= 1;
        // Small backward GPS corrections must not move a car against the
        // driving direction or almost one full lap forward.
        delta = Math.max(0, delta);
        return (from + delta * fraction) % 1;
    }}

    function drawInterpolatedCars(fromIndex, toIndex, fraction) {{
        const from = replay.frames[fromIndex].progress;
        const to = replay.frames[toIndex].progress;
        const x = [];
        const y = [];
        for (let index = 0; index < from.length; index += 1) {{
            const start = from[index];
            const end = to[index];
            let progress = null;
            if (finiteProgress(start) && finiteProgress(end)) {{
                progress = interpolateProgress(start, end, fraction);
            }} else if (finiteProgress(start) && fraction < 1) {{
                progress = start;
            }} else if (finiteProgress(end) && fraction >= 1) {{
                progress = end;
            }}
            if (progress === null) {{
                x.push(null);
                y.push(null);
            }} else {{
                const point = pointAtProgress(progress);
                x.push(point[0]);
                y.push(point[1]);
            }}
        }}
        Plotly.restyle(plot, {{x: [x], y: [y]}}, [carsTraceIndex]);
    }}

    function applyKeyframe(index) {{
        currentFrameIndex = Math.max(0, Math.min(lastFrameIndex, index));
        const frame = replay.frames[currentFrameIndex];
        internalSliderUpdates += 1;
        const frameUpdate = Plotly.animate(plot, [frame.name], {{
            mode: "immediate",
            frame: {{duration: 0, redraw: true}},
            transition: {{duration: 0}}
        }});
        const sliderUpdate = Plotly.relayout(
            plot, {{"sliders[0].active": currentFrameIndex}}
        );
        Promise.allSettled([frameUpdate, sliderUpdate]).finally(() => {{
            internalSliderUpdates = Math.max(0, internalSliderUpdates - 1);
        }});
    }}

    function stopPlayback() {{
        playing = false;
        segmentStartedAt = null;
        if (animationRequest !== null) {{
            cancelAnimationFrame(animationRequest);
            animationRequest = null;
        }}
    }}

    function tick(now) {{
        if (!playing || currentFrameIndex >= lastFrameIndex) {{
            stopPlayback();
            return;
        }}
        let duration = frameDurationMilliseconds(
            currentFrameIndex, currentFrameIndex + 1
        );
        if (segmentStartedAt === null) {{
            segmentStartedAt = now - segmentFraction * duration;
        }}
        let elapsed = now - segmentStartedAt;

        while (elapsed >= duration && currentFrameIndex < lastFrameIndex) {{
            currentFrameIndex += 1;
            applyKeyframe(currentFrameIndex);
            segmentStartedAt += duration;
            segmentFraction = 0;
            if (currentFrameIndex >= lastFrameIndex) {{
                drawInterpolatedCars(lastFrameIndex, lastFrameIndex, 1);
                stopPlayback();
                return;
            }}
            elapsed = now - segmentStartedAt;
            duration = frameDurationMilliseconds(
                currentFrameIndex, currentFrameIndex + 1
            );
        }}

        segmentFraction = Math.max(0, Math.min(1, elapsed / duration));
        drawInterpolatedCars(
            currentFrameIndex, currentFrameIndex + 1, segmentFraction
        );
        animationRequest = requestAnimationFrame(tick);
    }}

    function playAtSpeed(requestedSpeed) {{
        const parsedSpeed = Number(requestedSpeed);
        if (!replay.speeds.includes(parsedSpeed)) return;
        if (currentFrameIndex >= lastFrameIndex) {{
            applyKeyframe(0);
            segmentFraction = 0;
        }}
        speed = parsedSpeed;
        const duration = frameDurationMilliseconds(
            currentFrameIndex, currentFrameIndex + 1
        );
        segmentStartedAt = performance.now() - segmentFraction * duration;
        playing = true;
        if (animationRequest === null) {{
            animationRequest = requestAnimationFrame(tick);
        }}
    }}

    function jumpToFrame(index) {{
        stopPlayback();
        segmentFraction = 0;
        applyKeyframe(Number(index));
        drawInterpolatedCars(currentFrameIndex, currentFrameIndex, 1);
    }}

    plot.on("plotly_buttonclicked", (event) => {{
        const command = event?.button?.args?.[0];
        if (command?.action === "play") playAtSpeed(command.speed);
        if (command?.action === "pause") stopPlayback();
    }});
    plot.on("plotly_sliderchange", (event) => {{
        if (internalSliderUpdates > 0) return;
        const command = event?.step?.args?.[0];
        if (Number.isInteger(command?.frameIndex)) {{
            jumpToFrame(command.frameIndex);
        }}
    }});

    // Deliberate public test/debug interface in the generated HTML.
    plot.circleOfDoomPlayback = {{
        play: playAtSpeed,
        pause: stopPlayback,
        jumpTo: jumpToFrame,
        state: () => ({{currentFrameIndex, segmentFraction, speed, playing}})
    }};
}})();
"""


def create_figure(
    replay: ReplayResult,
    *,
    focus_driver: int,
    focus_acronym: str,
    frame_seconds: int,
    geometry: TrackGeometry | None = None,
) -> go.Figure:
    """Create the interactive Plotly animation."""
    track_geometry = geometry or synthetic_track_geometry()
    track_x = [point[0] for point in track_geometry.points]
    track_y = [point[1] for point in track_geometry.points]
    track_trace = go.Scatter(
        x=track_x,
        y=track_y,
        mode="lines",
        line={"color": "#566573", "width": 16},
        hoverinfo="skip",
        name="Track lap",
    )
    start_inner = point_at_progress(track_geometry.points, 0.0, offset=-0.08)
    start_outer = point_at_progress(track_geometry.points, 0.0, offset=0.08)
    halfway_inner = point_at_progress(track_geometry.points, 0.5, offset=-0.08)
    halfway_outer = point_at_progress(track_geometry.points, 0.5, offset=0.08)
    start_line = go.Scatter(
        x=[start_inner[0], start_outer[0]],
        y=[start_inner[1], start_outer[1]],
        mode="lines+text",
        text=[None, "START / FINISH · 0/100 %"],
        textposition="top center",
        textfont={"color": "white"},
        line={"color": "white", "width": 3},
        hoverinfo="skip",
        name="Start/finish",
    )
    halfway_line = go.Scatter(
        x=[halfway_inner[0], halfway_outer[0]],
        y=[halfway_inner[1], halfway_outer[1]],
        mode="lines+text",
        text=[None, "HALF LAP · 50 %"],
        textposition="bottom center",
        textfont={"color": "#BDC3C7"},
        line={"color": "#BDC3C7", "width": 2, "dash": "dot"},
        hoverinfo="skip",
        name="Half lap",
    )

    driver_order = _replay_driver_order(replay)
    first_traces = _frame_traces(
        replay.frames[0],
        replay.reference_lap_time,
        focus_driver,
        driver_order,
        track_geometry,
    )
    figure = go.Figure(data=[track_trace, start_line, halfway_line, *first_traces])
    plotly_frames: list[go.Frame] = []
    slider_steps: list[dict[str, Any]] = []

    for index, frame in enumerate(replay.frames):
        name = f"frame-{index}"
        traces = _frame_traces(
            frame,
            replay.reference_lap_time,
            focus_driver,
            driver_order,
            track_geometry,
        )
        plotly_frames.append(
            go.Frame(
                data=list(traces),
                traces=[3, 4, 5],
                name=name,
                layout=go.Layout(
                    title={"text": _frame_title(frame, focus_acronym, track_geometry.label)}
                ),
            )
        )
        slider_steps.append(
            {
                "args": [
                    {"frameIndex": index},
                ],
                "label": f"L{frame.lap_number} {frame.date.strftime('%H:%M')}",
                "method": "skip",
                "value": name,
            }
        )

    figure.frames = plotly_frames
    figure.update_layout(
        template="plotly_dark",
        title={
            "text": _frame_title(
                replay.frames[0], focus_acronym, track_geometry.label
            ),
            "x": 0.5,
        },
        width=1000,
        height=900,
        showlegend=True,
        legend={"orientation": "h", "y": 1.02, "x": 0.5, "xanchor": "center"},
        margin={"l": 40, "r": 40, "t": 130, "b": 130},
        xaxis={"visible": False, "range": [-1.35, 1.35], "scaleanchor": "y"},
        yaxis={"visible": False, "range": [-1.35, 1.35]},
        annotations=[
            {
                "text": (
                    f"Track = {track_geometry.label}<br>"
                    "Position = geometric lap progress from OpenF1 x/y/z<br>"
                    "Gap = OpenF1 gap_to_leader<br>"
                    f"Pit projection using reference lap {replay.reference_lap_time:.1f}s<br>"
                    "Diamond = recently stopped<br>"
                    f"Keyframes every {frame_seconds}s · smoothly interpolated<br><br>{SOURCE_ANNOTATION}"
                ),
                "x": 0.01,
                "y": 0.01,
                "xref": "paper",
                "yref": "paper",
                "xanchor": "left",
                "yanchor": "bottom",
                "showarrow": False,
                "font": {"color": "#BDC3C7", "size": 12},
                "align": "left",
                "bgcolor": "rgba(17, 17, 17, 0.82)",
            }
        ],
        updatemenus=[
            {
                "type": "buttons",
                "direction": "left",
                "x": 0.5,
                "xanchor": "center",
                "y": -0.08,
                "buttons": [
                    *[
                        {
                            "label": f"▶ {speed}×",
                            "method": "skip",
                            "args": [{"action": "play", "speed": speed}],
                        }
                        for speed in PLAYBACK_SPEEDS
                    ],
                    {
                        "label": "Ⅱ Pause",
                        "method": "skip",
                        "args": [{"action": "pause"}],
                    },
                ],
            }
        ],
        sliders=[
            {
                "active": 0,
                "currentvalue": {"prefix": "Replay: "},
                "pad": {"t": 60},
                "steps": slider_steps,
            }
        ],
    )
    return figure


def default_output_path(session_key: int, focus_acronym: str) -> Path:
    return ARTIFACTS_DIR / (
        f"circle_of_doom_session_{session_key}_{focus_acronym.lower()}.html"
    )


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Create an interactive race replay from historical OpenF1 data."
        )
    )
    parser.add_argument("--session-key", type=int, default=DEFAULT_SESSION_KEY)
    parser.add_argument(
        "--driver",
        default=DEFAULT_FOCUS_DRIVER,
        help="Driver number or acronym for pit-exit projection (default: ANT).",
    )
    parser.add_argument(
        "--pit-loss",
        type=float,
        default=DEFAULT_GREEN_PIT_LOSS_SECONDS,
        help="Assumed pit loss under green conditions in seconds.",
    )
    parser.add_argument(
        "--neutralized-pit-loss",
        type=float,
        default=DEFAULT_NEUTRALIZED_PIT_LOSS_SECONDS,
        help="Assumed pit loss under SC/VSC conditions in seconds.",
    )
    parser.add_argument(
        "--frame-seconds",
        type=int,
        default=DEFAULT_FRAME_SECONDS,
        help="Time between animation frames.",
    )
    parser.add_argument(
        "--max-staleness",
        type=int,
        default=DEFAULT_MAX_STALENESS_SECONDS,
        help="Maximum age of carried-forward location measurements in seconds.",
    )
    parser.add_argument("--refresh", action="store_true", help="Refresh the OpenF1 cache.")
    parser.add_argument(
        "--build-geometry",
        action="store_true",
        help="Build and persist local track geometry for the preview or stored mode.",
    )
    parser.add_argument(
        "--geometry-mode",
        choices=("circle", "stored"),
        default="circle",
        help="Render the synthetic circle (default) or the stored local geometry.",
    )
    parser.add_argument(
        "--self-contained",
        action="store_true",
        help="Embed Plotly JavaScript in the HTML instead of using a CDN.",
    )
    parser.add_argument("--open", action="store_true", help="Open the result in a browser.")
    parser.add_argument("--output", type=Path, help="Custom HTML output path.")
    return parser


def main(argv: list[str] | None = None) -> int:
    """CLI entry point for data download, replay, and HTML generation."""
    args = build_argument_parser().parse_args(argv)
    try:
        datasets = load_session_datasets(args.session_key, refresh=args.refresh)
        session = datasets["sessions"].iloc[0]
        meeting_key = int(session["meeting_key"]) if pd.notna(session.get("meeting_key")) else None
        circuit_id = (
            f"openf1:circuit:{int(session['circuit_key'])}"
            if pd.notna(session.get("circuit_key"))
            else None
        )
        if args.build_geometry:
            from f1_pipeline.geometry import build_session_geometry

            build_session_geometry(args.session_key)
        geometry = synthetic_track_geometry()
        if args.geometry_mode == "stored":
            try:
                geometry = load_track_geometry(
                    args.session_key,
                    meeting_key=meeting_key,
                    circuit_id=circuit_id,
                ) or synthetic_track_geometry()
            except TrackGeometryError as exc:
                print(f"Track geometry unavailable; using fallback: {exc}")
        focus_driver = resolve_driver_number(datasets["drivers"], args.driver)
        focus_row = datasets["drivers"].loc[
            datasets["drivers"]["driver_number"].astype(int).eq(focus_driver)
        ].iloc[0]
        focus_acronym = str(focus_row["name_acronym"])

        replay = build_replay(
            datasets,
            focus_driver=focus_driver,
            green_pit_loss=args.pit_loss,
            neutralized_pit_loss=args.neutralized_pit_loss,
            frame_seconds=args.frame_seconds,
            max_staleness_seconds=args.max_staleness,
        )
        figure = create_figure(
            replay,
            focus_driver=focus_driver,
            focus_acronym=focus_acronym,
            frame_seconds=args.frame_seconds,
            geometry=geometry,
        )
        output = args.output or default_output_path(args.session_key, focus_acronym)
        output = output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        figure.write_html(
            output,
            include_plotlyjs=True if args.self_contained else "cdn",
            full_html=True,
            auto_play=False,
            auto_open=False,
            post_script=build_animation_post_script(
                replay, frame_seconds=args.frame_seconds, geometry=geometry
            ),
        )

        print(
            f"\nCircle of Doom created: {output}\n"
            f"Session: {session.get('circuit_short_name', args.session_key)} "
            f"{session.get('year', '')}\n"
            f"Focus: {focus_acronym} ({focus_driver})\n"
            f"Geometry: {geometry.label}\n"
            f"Frames: {len(replay.frames):,}\n"
            f"Reference lap time: {replay.reference_lap_time:.3f}s"
        )
        if args.open:
            webbrowser.open(output.as_uri())
        return 0
    except (
        CircleOfDoomError,
        TrackGeometryError,
        KeyError,
        TypeError,
        ValueError,
        OSError,
    ) as exc:
        print(f"Circle of Doom error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())



