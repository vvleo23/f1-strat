"""Build and load reusable local circuit geometry from OpenF1 locations."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import numpy as np
import pandas as pd

from f1_pipeline.data_validation import DataValidationError, validate_frame
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.settings import CURATED_DATA_DIR, PROJECT_ROOT, RAW_DATA_DIR

GEOMETRY_COLUMNS = [
    "geometry_id",
    "circuit_id",
    "geometry_type",
    "geometry_data",
    "crs",
    "source_system",
    "source_record_key",
    "source_version",
    "valid_from_utc",
    "valid_to_utc",
    "ingested_at",
    "schema_version",
]
GEOMETRY_PATH = CURATED_DATA_DIR / "dimensions" / "circuit_geometry.parquet"
GEOMETRY_TYPE = "observed_session_centerline"
GEOMETRY_SOURCE_VERSION = "openf1-session-geometry-v2"
GEOMETRY_SCHEMA_VERSION = 1
DEFAULT_SAMPLE_LAPS = 5
DEFAULT_POINT_COUNT = 201
MIN_LAP_SAMPLES = 20
DEFAULT_ORIENTATION_LAP = 1
MIN_ORIENTATION_CANDIDATES = 3
MIN_CANDIDATE_LENGTH_RATIO = 0.75
MAX_CANDIDATE_LENGTH_RATIO = 1.25
MAX_CANDIDATE_CLOSURE_RATIO = 0.08
SMOOTHING_WINDOW = 7
SMOOTHING_PASSES = 2


def geometry_table_path(season: int, curated_dir: Path = CURATED_DATA_DIR) -> Path:
    if season < 1950:
        raise ValueError("Season must be 1950 or later.")
    return curated_dir / "dimensions" / f"season={season}" / "circuit_geometry.parquet"


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _stored_path(value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


class TrackGeometryError(RuntimeError):
    """Describe an invalid or unavailable local track geometry."""


@dataclass(frozen=True)
class TrackGeometry:
    """Display-ready progress-parametrized track geometry."""

    points: tuple[tuple[float, float], ...]
    source: str
    geometry_type: str
    circuit_id: str | None = None
    source_session_key: int | None = None
    source_meeting_key: int | None = None
    quality_status: str = "available"

    @property
    def label(self) -> str:
        if self.source == "synthetic":
            return "synthetic circle fallback"
        return "OpenF1 observed session centerline"


def _parse_datetimes(values: pd.Series) -> pd.Series:
    return pd.to_datetime(values, utc=True, errors="coerce", format="mixed")


def _finite_points(points: np.ndarray) -> np.ndarray:
    values = np.asarray(points, dtype=float)
    if values.ndim != 2 or values.shape[1] < 2:
        raise TrackGeometryError("Track geometry requires at least x and y coordinates.")
    values = values[:, :3] if values.shape[1] >= 3 else values[:, :2]
    values = values[np.isfinite(values).all(axis=1)]
    if len(values) < 3:
        raise TrackGeometryError("Track geometry requires at least three finite points.")
    return values


def normalize_display_points(points: np.ndarray) -> tuple[tuple[float, float], ...]:
    values = _finite_points(points)
    x_min, x_max = float(values[:, 0].min()), float(values[:, 0].max())
    y_min, y_max = float(values[:, 1].min()), float(values[:, 1].max())
    scale = max(x_max - x_min, y_max - y_min)
    if not math.isfinite(scale) or scale <= 0:
        raise TrackGeometryError("Track geometry has no measurable x/y extent.")
    x_center = (x_min + x_max) / 2
    y_center = (y_min + y_max) / 2
    normalized = np.column_stack(
        ((values[:, 0] - x_center) * 2 / scale, (values[:, 1] - y_center) * 2 / scale)
    )
    return tuple((float(x), float(y)) for x, y in normalized)


def point_at_progress(
        points: tuple[tuple[float, float], ...],
        progress: float,
        offset: float = 0.0,
) -> tuple[float, float]:
    """Interpolate a point and optional outward normal offset on a closed line."""
    if len(points) < 3:
        raise TrackGeometryError("A closed track geometry requires at least three points.")
    values = np.asarray(points, dtype=float)
    if not np.isfinite(values).all():
        raise TrackGeometryError("Track geometry contains non-finite display coordinates.")
    segment_count = len(values) - 1
    wrapped = float(progress) % 1.0
    position = wrapped * segment_count
    index = min(int(math.floor(position)), segment_count - 1)
    fraction = position - index
    start = values[index]
    end = values[index + 1]
    point = start + (end - start) * fraction
    if offset:
        tangent = end - start
        length = float(np.linalg.norm(tangent))
        if length > 0:
            normal = np.array((-tangent[1], tangent[0])) / length
            point = point + normal * offset
    return float(point[0]), float(point[1])


def synthetic_track_geometry(point_count: int = 361) -> TrackGeometry:
    if point_count < 3:
        raise ValueError("point_count must be at least three.")
    progress = np.linspace(0, 1, point_count)
    points = tuple(
        (
            math.cos(math.pi / 2 - 2 * math.pi * float(value)),
            math.sin(math.pi / 2 - 2 * math.pi * float(value)),
        )
        for value in progress
    )
    return TrackGeometry(
        points=points,
        source="synthetic",
        geometry_type="synthetic_circle",
        quality_status="unavailable",
    )


def _path_for_lap(
        driver_locations: pd.DataFrame,
        lap_start: pd.Timestamp,
        next_lap_start: pd.Timestamp,
) -> tuple[np.ndarray, int] | None:
    samples = driver_locations[
        driver_locations["location_at"].between(
            lap_start, next_lap_start, inclusive="left"
        )
    ].copy()
    if len(samples) < MIN_LAP_SAMPLES:
        return None
    samples = samples.sort_values("location_at").drop_duplicates("location_at")
    coordinates = samples[["x", "y", "z"]].to_numpy(dtype=float)
    segments = np.linalg.norm(np.diff(coordinates, axis=0), axis=1)
    valid = np.isfinite(segments) & (segments >= 0)
    if valid.sum() < MIN_LAP_SAMPLES - 1:
        return None
    coordinates = coordinates[np.r_[True, valid]]
    segments = np.linalg.norm(np.diff(coordinates, axis=0), axis=1)
    cumulative = np.concatenate(([0.0], np.cumsum(segments)))
    total_distance = float(cumulative[-1])
    if not math.isfinite(total_distance) or total_distance <= 0:
        return None
    unique_distance, unique_indices = np.unique(cumulative, return_index=True)
    if len(unique_distance) < MIN_LAP_SAMPLES:
        return None
    return coordinates[unique_indices], len(samples)


def _resample_path(path: np.ndarray, point_count: int) -> np.ndarray:
    values = _finite_points(path)
    segments = np.linalg.norm(np.diff(values[:, :2], axis=0), axis=1)
    cumulative = np.concatenate(([0.0], np.cumsum(segments)))
    total_distance = float(cumulative[-1])
    if total_distance <= 0:
        raise TrackGeometryError("Cannot resample a path with zero length.")
    normalized = cumulative / total_distance
    target = np.linspace(0, 1, point_count)
    return np.column_stack(
        [np.interp(target, normalized, values[:, axis]) for axis in range(values.shape[1])]
    )


def _smooth_closed_path(path: np.ndarray) -> np.ndarray:
    values = _finite_points(path)
    if len(values) < SMOOTHING_WINDOW + 1:
        return values
    closed = values[:-1]
    half_window = SMOOTHING_WINDOW // 2
    for _ in range(SMOOTHING_PASSES):
        values = np.mean(
            np.stack(
                [np.roll(closed, shift, axis=0) for shift in range(-half_window, half_window + 1)]
            ),
            axis=0,
        )
        closed = values
    return np.vstack((closed, closed[0]))


def _candidate_paths(location: pd.DataFrame, laps: pd.DataFrame) -> list[dict[str, Any]]:
    required_lap_columns = {"driver_number", "lap_number", "date_start"}
    if not required_lap_columns.issubset(laps.columns):
        missing = sorted(required_lap_columns.difference(laps.columns))
        raise TrackGeometryError("Lap data is missing: " + ", ".join(missing))
    required_location_columns = {"driver_number", "date", "x", "y", "z"}
    if not required_location_columns.issubset(location.columns):
        missing = sorted(required_location_columns.difference(location.columns))
        raise TrackGeometryError("Location data is missing: " + ", ".join(missing))

    locations = location.copy()
    locations["location_at"] = _parse_datetimes(locations["date"])
    locations["driver_number"] = pd.to_numeric(
        locations["driver_number"], errors="coerce"
    )
    for column in ("x", "y", "z"):
        locations[column] = pd.to_numeric(locations[column], errors="coerce")
    locations = locations.dropna(
        subset=["location_at", "driver_number", "x", "y", "z"]
    )
    location_by_driver = {
        int(cast(Any, driver)): frame.sort_values("location_at")
        for driver, frame in locations.groupby("driver_number")
    }

    lap_data = laps.copy()
    lap_data["lap_started_at"] = _parse_datetimes(lap_data["date_start"])
    lap_data["driver_number"] = pd.to_numeric(
        lap_data["driver_number"], errors="coerce"
    )
    lap_data["lap_number"] = pd.to_numeric(lap_data["lap_number"], errors="coerce")
    for pit_column in ("is_pit_out_lap", "is_pit_in_lap"):
        if pit_column in lap_data.columns:
            lap_data = lap_data[~lap_data[pit_column].fillna(False).astype(bool)]
    lap_data = lap_data.dropna(
        subset=["lap_started_at", "driver_number", "lap_number"]
    )
    lap_data = lap_data.sort_values(["driver_number", "lap_started_at"])
    lap_data = lap_data.drop_duplicates(["driver_number", "lap_number"], keep="last")
    lap_data["next_lap_started_at"] = lap_data.groupby("driver_number")[
        "lap_started_at"
    ].shift(-1)

    candidates: list[dict[str, Any]] = []
    for row in lap_data.itertuples(index=False):
        next_lap_start = getattr(row, "next_lap_started_at")
        if pd.isna(next_lap_start):
            continue
        driver = int(cast(Any, row.driver_number))
        driver_locations = location_by_driver.get(driver)
        if driver_locations is None:
            continue
        path_result = _path_for_lap(
            driver_locations,
            cast_timestamp(row.lap_started_at),
            cast_timestamp(next_lap_start),
        )
        if path_result is None:
            continue
        path, sample_count = path_result
        distances = np.linalg.norm(np.diff(path[:, :2], axis=0), axis=1)
        path_length = float(distances.sum())
        closure_distance = float(np.linalg.norm(path[-1, :2] - path[0, :2]))
        if not math.isfinite(path_length) or path_length <= 0:
            continue
        candidates.append(
            {
                "driver_number": driver,
                "lap_number": int(cast(Any, row.lap_number)),
                "path": path,
                "sample_count": sample_count,
                "path_length": path_length,
                "closure_distance": closure_distance,
                "closure_ratio": closure_distance / path_length,
                "duration_seconds": (
                        cast_timestamp(next_lap_start) - cast_timestamp(row.lap_started_at)
                ).total_seconds(),
            }
        )
    return candidates


def cast_timestamp(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise TrackGeometryError("Geometry input contains an invalid timestamp.")
    return timestamp


def build_centerline(
        location: pd.DataFrame,
        laps: pd.DataFrame,
        *,
        sample_laps: int = DEFAULT_SAMPLE_LAPS,
        point_count: int = DEFAULT_POINT_COUNT,
        orientation_lap: int = DEFAULT_ORIENTATION_LAP,
) -> tuple[np.ndarray, list[dict[str, Any]], dict[str, Any]]:
    if sample_laps < 3:
        raise ValueError("sample_laps must be at least three.")
    if point_count < 20:
        raise ValueError("point_count must be at least twenty.")
    candidates = _candidate_paths(location, laps)
    if not candidates:
        raise TrackGeometryError("No complete OpenF1 lap with sufficient location data was found.")

    preferred = [
        candidate for candidate in candidates if candidate["lap_number"] == orientation_lap
    ]
    selection_basis = "first_race_lap" if len(preferred) >= MIN_ORIENTATION_CANDIDATES else "clean_laps"
    candidate_pool = preferred if preferred else candidates
    median_path_length = float(
        np.median([candidate["path_length"] for candidate in candidate_pool])
    )
    for candidate in candidate_pool:
        candidate["length_ratio"] = candidate["path_length"] / median_path_length
        candidate["quality_score"] = (
                abs(1.0 - candidate["length_ratio"]) + candidate["closure_ratio"]
        )
    usable = [
        candidate
        for candidate in candidate_pool
        if MIN_CANDIDATE_LENGTH_RATIO
           <= candidate["length_ratio"]
           <= MAX_CANDIDATE_LENGTH_RATIO
           and candidate["closure_ratio"] <= MAX_CANDIDATE_CLOSURE_RATIO
    ]
    if len(usable) < MIN_ORIENTATION_CANDIDATES and candidate_pool is preferred:
        selection_basis = "clean_laps"
        candidate_pool = candidates
        median_path_length = float(
            np.median([candidate["path_length"] for candidate in candidate_pool])
        )
        for candidate in candidate_pool:
            candidate["length_ratio"] = candidate["path_length"] / median_path_length
            candidate["quality_score"] = (
                    abs(1.0 - candidate["length_ratio"]) + candidate["closure_ratio"]
            )
        usable = [
            candidate
            for candidate in candidate_pool
            if MIN_CANDIDATE_LENGTH_RATIO
               <= candidate["length_ratio"]
               <= MAX_CANDIDATE_LENGTH_RATIO
               and candidate["closure_ratio"] <= MAX_CANDIDATE_CLOSURE_RATIO
        ]
    usable.sort(
        key=lambda item: (
            item["quality_score"],
            item["driver_number"],
            item["lap_number"],
        )
    )
    selected: list[dict[str, Any]] = []
    selected_drivers: set[int] = set()
    for candidate in usable:
        if candidate["driver_number"] in selected_drivers:
            continue
        selected.append(candidate)
        selected_drivers.add(candidate["driver_number"])
        if len(selected) >= sample_laps:
            break
    if len(selected) < sample_laps:
        for candidate in usable:
            if candidate in selected:
                continue
            selected.append(candidate)
            if len(selected) >= sample_laps:
                break
    if len(selected) < 3:
        raise TrackGeometryError("At least three clean laps are required for track geometry.")

    resampled = np.stack(
        [_resample_path(candidate["path"], point_count) for candidate in selected]
    )
    centerline = np.median(resampled, axis=0)
    closure_distance = float(np.linalg.norm(centerline[-1] - centerline[0]))
    centerline = _smooth_closed_path(centerline)
    samples: list[dict[str, Any]] = [
        {
            "driver_number": int(candidate["driver_number"]),
            "lap_number": int(candidate["lap_number"]),
            "sample_count": int(candidate["sample_count"]),
            "path_length": round(float(candidate["path_length"]), 3),
            "closure_distance": round(float(candidate["closure_distance"]), 3),
            "closure_ratio": round(float(candidate["closure_ratio"]), 6),
            "duration_seconds": round(float(candidate["duration_seconds"]), 3),
        }
        for candidate in selected
    ]
    quality = {
        "status": "available" if len(selected) >= sample_laps else "partial",
        "selected_laps": len(selected),
        "requested_laps": sample_laps,
        "point_count": point_count,
        "orientation_lap": orientation_lap,
        "selection_basis": selection_basis,
        "candidate_count": len(candidates),
        "usable_candidate_count": len(usable),
        "median_candidate_path_length": round(median_path_length, 3),
        "minimum_location_samples": min(item["sample_count"] for item in selected),
        "maximum_location_samples": max(item["sample_count"] for item in selected),
        "closure_distance_before_forcing": closure_distance,
    }
    return centerline, samples, quality


def build_geometry_record(
        location: pd.DataFrame,
        laps: pd.DataFrame,
        *,
        session_key: int,
        meeting_key: int,
        circuit_id: str,
        sample_laps: int = DEFAULT_SAMPLE_LAPS,
        point_count: int = DEFAULT_POINT_COUNT,
        orientation_lap: int = DEFAULT_ORIENTATION_LAP,
        ingested_at: pd.Timestamp | None = None,
) -> dict[str, Any]:
    centerline, samples, quality = build_centerline(
        location,
        laps,
        sample_laps=sample_laps,
        point_count=point_count,
        orientation_lap=orientation_lap,
    )
    points = [
        {
            "progress": round(float(index / (len(centerline) - 1)), 8),
            "x": round(float(point[0]), 6),
            "y": round(float(point[1]), 6),
            "z": round(float(point[2]), 6),
        }
        for index, point in enumerate(centerline)
    ]
    geometry_data = {
        "coordinate_axes": ["x", "y", "z"],
        "geometry_semantics": "local observed OpenF1 centerline; not geographic coordinates",
        "source_session_key": session_key,
        "source_meeting_key": meeting_key,
        "source_lap_samples": samples,
        "quality": quality,
        "points": points,
    }
    timestamp = ingested_at or pd.Timestamp(datetime.now(timezone.utc))
    return {
        "geometry_id": f"openf1:geometry:meeting:{meeting_key}:session:{session_key}:v2",
        "circuit_id": circuit_id,
        "geometry_type": GEOMETRY_TYPE,
        "geometry_data": json.dumps(geometry_data, ensure_ascii=False, separators=(",", ":")),
        "crs": "openf1_session_local",
        "source_system": "openf1",
        "source_record_key": f"meeting:{meeting_key}:session:{session_key}:location",
        "source_version": GEOMETRY_SOURCE_VERSION,
        "valid_from_utc": timestamp,
        "valid_to_utc": None,
        "ingested_at": timestamp,
        "schema_version": GEOMETRY_SCHEMA_VERSION,
    }


def _validate_geometry_frame(frame: pd.DataFrame) -> None:
    try:
        validate_frame(
            frame,
            name="circuit_geometry",
            required_columns=GEOMETRY_COLUMNS,
            key_columns=("geometry_id",),
            datetime_columns=("valid_from_utc", "valid_to_utc", "ingested_at"),
            numeric_columns=("schema_version",),
            required_non_null=(
                "geometry_id",
                "circuit_id",
                "geometry_type",
                "geometry_data",
                "crs",
                "source_system",
                "source_record_key",
                "ingested_at",
            ),
            allow_empty=True,
        )
    except DataValidationError as exc:
        raise TrackGeometryError(str(exc)) from exc


def write_geometry_record(
        record: dict[str, Any], path: Path = GEOMETRY_PATH
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(f"{path.suffix}.lock")
    deadline = time.monotonic() + 30
    descriptor: int | None = None
    while descriptor is None:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if time.monotonic() >= deadline:
                raise TrackGeometryError("Geometry dimension is locked by another pipeline run.")
            time.sleep(0.05)
    os.close(descriptor)
    try:
        existing = pd.DataFrame(columns=GEOMETRY_COLUMNS)
        if path.exists():
            try:
                existing = pd.read_parquet(path)
            except (OSError, ValueError) as exc:
                raise TrackGeometryError(f"Could not read geometry dimension: {exc}") from exc
        records = existing.to_dict(orient="records")
        records = [
            item
            for item in records
            if item.get("geometry_id") != record.get("geometry_id")
               and item.get("source_record_key") != record.get("source_record_key")
        ]
        records.append(record)
        frame = pd.DataFrame(records)
        for column in GEOMETRY_COLUMNS:
            if column not in frame.columns:
                frame[column] = None
        frame = frame[GEOMETRY_COLUMNS]
        _validate_geometry_frame(frame)
        atomic_parquet(frame, path)
    finally:
        lock_path.unlink(missing_ok=True)
    return path


def _record_data(record: dict[str, Any]) -> dict[str, Any]:
    raw = record.get("geometry_data")
    if not isinstance(raw, str):
        raise TrackGeometryError("Geometry data is not stored as JSON text.")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TrackGeometryError("Geometry data contains invalid JSON.") from exc
    if not isinstance(data, dict) or not isinstance(data.get("points"), list):
        raise TrackGeometryError("Geometry data does not contain a point list.")
    try:
        data["source_session_key"] = int(data["source_session_key"])
        if data.get("source_meeting_key") is not None:
            data["source_meeting_key"] = int(data["source_meeting_key"])
    except (KeyError, TypeError, ValueError) as exc:
        raise TrackGeometryError("Geometry data contains invalid source keys.") from exc
    return data


def _track_geometry_from_record(record: dict[str, Any]) -> TrackGeometry:
    data = _record_data(record)
    raw_points = []
    for point in data["points"]:
        if not isinstance(point, dict):
            continue
        raw_points.append([point.get("x"), point.get("y")])
    display_points = normalize_display_points(np.asarray(raw_points, dtype=float))
    if display_points[0] != display_points[-1]:
        display_points = (*display_points, display_points[0])
    return TrackGeometry(
        points=display_points,
        source="openf1",
        geometry_type=str(record.get("geometry_type") or GEOMETRY_TYPE),
        circuit_id=str(record.get("circuit_id")) if record.get("circuit_id") else None,
        source_session_key=int(data["source_session_key"]),
        source_meeting_key=int(data["source_meeting_key"])
        if data.get("source_meeting_key") is not None
        else None,
        quality_status=str(data.get("quality", {}).get("status", "available")),
    )


def _selected_geometry_record(
        frame: pd.DataFrame,
        session_key: int,
        meeting_key: int | None,
        circuit_id: str | None,
) -> dict[str, Any] | None:
    if frame.empty:
        return None
    _validate_geometry_frame(frame)
    exact: list[dict[str, Any]] = []
    weekend: list[dict[str, Any]] = []
    circuit: list[dict[str, Any]] = []
    records = cast(list[dict[str, Any]], frame.to_dict(orient="records"))
    for record in records:
        if circuit_id is not None and record.get("circuit_id") != circuit_id:
            continue
        data = _record_data(record)
        if int(data.get("source_session_key", -1)) == session_key:
            exact.append(record)
        elif meeting_key is not None and int(data.get("source_meeting_key", -1)) == meeting_key:
            weekend.append(record)
        elif circuit_id is not None:
            circuit.append(record)
    selected = exact or weekend or circuit
    if not selected:
        return None

    def order(record: dict[str, Any]) -> tuple[pd.Timestamp, str]:
        raw_timestamp = record.get("valid_from_utc")
        if pd.isna(raw_timestamp):
            raw_timestamp = record.get("ingested_at")
        timestamp = cast(
            pd.Timestamp,
            pd.to_datetime(
                str(raw_timestamp),
                utc=True,
                errors="coerce",
            ),
        )
        if pd.isna(timestamp):
            timestamp = pd.Timestamp.min.tz_localize("UTC")
        return timestamp, str(record.get("geometry_id", ""))

    return max(selected, key=order)


def load_track_geometry(
        session_key: int,
        *,
        meeting_key: int | None = None,
        circuit_id: str | None = None,
        season: int | None = None,
        path: Path | None = None,
        curated_dir: Path = CURATED_DATA_DIR,
) -> TrackGeometry | None:
    paths = [path] if path is not None else [
        geometry_table_path(season, curated_dir) if season is not None else GEOMETRY_PATH
    ]
    if path is None and season == 2026 and paths[0] != GEOMETRY_PATH:
        paths.append(GEOMETRY_PATH)
    for candidate_path in paths:
        if not candidate_path.exists():
            continue
        try:
            frame = pd.read_parquet(candidate_path)
        except (OSError, ValueError) as exc:
            raise TrackGeometryError(f"Could not read geometry dimension: {exc}") from exc
        selected = _selected_geometry_record(
            frame,
            session_key,
            meeting_key,
            circuit_id,
        )
        if selected is not None:
            return _track_geometry_from_record(selected)
    return None


def _session_context(session_key: int) -> tuple[int, int, str]:
    path = RAW_DATA_DIR / f"openf1_{session_key}_sessions.parquet"
    if not path.exists():
        raise TrackGeometryError(f"Session snapshot is missing: {path}")
    sessions = pd.read_parquet(path)
    if sessions.empty:
        raise TrackGeometryError(f"Session snapshot is empty: {path}")
    row = sessions.iloc[0]
    meeting_key = int(row["meeting_key"])
    circuit_key = int(row["circuit_key"])
    return meeting_key, circuit_key, f"openf1:circuit:{circuit_key}"


def _refresh_master_geometry_lineage(
        season: int,
        output_path: Path,
        curated_dir: Path,
) -> None:
    manifest_path = curated_dir / "manifests" / f"master_data_{season}.json"
    if not manifest_path.exists():
        return
    try:
        with manifest_path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise TrackGeometryError(f"Could not update master geometry lineage: {exc}") from exc
    tables = manifest.get("tables")
    if not isinstance(tables, dict):
        raise TrackGeometryError("Master manifest has no valid table lineage.")
    tables["circuit_geometry"] = {
        "path": _relative_path(output_path),
        "row_count": len(pd.read_parquet(output_path)),
        "sha256": sha256(output_path),
    }
    atomic_json(manifest, manifest_path)


def build_manifest_geometry(
        session_manifest: dict[str, Any],
        *,
        season: int,
        meeting_key: int,
        circuit_id: str,
        curated_dir: Path = CURATED_DATA_DIR,
) -> tuple[dict[str, Any], Path]:
    try:
        session_key = int(session_manifest["session_key"])
        endpoints = session_manifest["endpoints"]
    except (KeyError, TypeError, ValueError) as exc:
        raise TrackGeometryError("Session manifest has no valid geometry context.") from exc
    if not isinstance(endpoints, dict):
        raise TrackGeometryError("Session manifest endpoints are invalid.")
    inputs: dict[str, dict[str, Any]] = {}
    for name in ("sessions", "laps", "location"):
        endpoint = endpoints.get(name)
        if not isinstance(endpoint, dict) or endpoint.get("status") not in {
            "available",
            "stale",
        }:
            raise TrackGeometryError(f"Session manifest has no usable {name} endpoint.")
        raw_path_value = endpoint.get("raw_path")
        raw_hash = endpoint.get("raw_sha256")
        if not isinstance(raw_path_value, str) or not isinstance(raw_hash, str):
            raise TrackGeometryError(f"Session manifest {name} lineage is incomplete.")
        raw_path = _stored_path(raw_path_value)
        if not raw_path.is_file() or sha256(raw_path) != raw_hash:
            raise TrackGeometryError(f"Session manifest {name} snapshot failed hash validation.")
        inputs[name] = {
            "raw_path": _relative_path(raw_path),
            "raw_sha256": raw_hash,
            "retrieved_at": endpoint.get("retrieved_at"),
        }
    laps = pd.read_parquet(_stored_path(inputs["laps"]["raw_path"]))
    location = pd.read_parquet(_stored_path(inputs["location"]["raw_path"]))
    retrieved_times = pd.to_datetime(
        [value["retrieved_at"] for value in inputs.values()],
        utc=True,
        errors="coerce",
        format="mixed",
    )
    valid_times = retrieved_times[~pd.isna(retrieved_times)]
    ingested_at = max(valid_times) if len(valid_times) else pd.Timestamp(datetime.now(timezone.utc))
    record = build_geometry_record(
        location,
        laps,
        session_key=session_key,
        meeting_key=meeting_key,
        circuit_id=circuit_id,
        ingested_at=ingested_at,
    )
    output_path = geometry_table_path(season, curated_dir)
    write_geometry_record(record, output_path)
    _refresh_master_geometry_lineage(season, output_path, curated_dir)
    quality = _record_data(record)["quality"]
    result = {
        "status": quality["status"],
        "season": season,
        "session_key": session_key,
        "meeting_key": meeting_key,
        "circuit_id": circuit_id,
        "quality": quality,
        "inputs": inputs,
        "curated_path": _relative_path(output_path),
        "curated_sha256": sha256(output_path),
        "session_manifest_path": session_manifest.get("manifest_path"),
        "session_manifest_sha256": session_manifest.get("manifest_sha256"),
    }
    identity = json.dumps(
        {
            "schema_version": 1,
            "season": season,
            "session_key": session_key,
            "meeting_key": meeting_key,
            "circuit_id": circuit_id,
            "inputs": inputs,
            "curated_sha256": result["curated_sha256"],
        },
        sort_keys=True,
    )
    manifest_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    manifest_path = (
            curated_dir
            / "manifests"
            / "geometries"
            / f"geometry_{session_key}_{manifest_id}.json"
    )
    if not manifest_path.exists():
        atomic_json({"schema_version": 1, **result}, manifest_path)
    result["manifest_path"] = _relative_path(manifest_path)
    result["manifest_sha256"] = sha256(manifest_path)
    return result, manifest_path


def build_session_geometry(
        session_key: int,
        *,
        season: int = 2026,
        sample_laps: int = DEFAULT_SAMPLE_LAPS,
        point_count: int = DEFAULT_POINT_COUNT,
        orientation_lap: int = DEFAULT_ORIENTATION_LAP,
        path: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    laps_path = RAW_DATA_DIR / f"openf1_{session_key}_laps.parquet"
    location_path = RAW_DATA_DIR / f"openf1_{session_key}_location.parquet"
    if not laps_path.exists() or not location_path.exists():
        raise TrackGeometryError("Required OpenF1 laps or location snapshot is missing.")
    laps = pd.read_parquet(laps_path)
    location = pd.read_parquet(location_path)
    meeting_key, _, circuit_id = _session_context(session_key)
    record = build_geometry_record(
        location,
        laps,
        session_key=session_key,
        meeting_key=meeting_key,
        circuit_id=circuit_id,
        sample_laps=sample_laps,
        point_count=point_count,
        orientation_lap=orientation_lap,
    )
    output = write_geometry_record(record, path or geometry_table_path(season))
    data = _record_data(record)
    return output, data["quality"]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build reusable local circuit geometry from OpenF1 location data."
    )
    parser.add_argument("--session-key", type=int, required=True)
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--sample-laps", type=int, default=DEFAULT_SAMPLE_LAPS)
    parser.add_argument("--point-count", type=int, default=DEFAULT_POINT_COUNT)
    parser.add_argument("--orientation-lap", type=int, default=DEFAULT_ORIENTATION_LAP)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        output, quality = build_session_geometry(
            args.session_key,
            season=args.season,
            sample_laps=args.sample_laps,
            point_count=args.point_count,
            orientation_lap=args.orientation_lap,
            path=args.output,
        )
    except (OSError, TrackGeometryError, ValueError, TypeError) as exc:
        print(f"Track geometry could not be built: {exc}")
        return 1
    print(f"Track geometry: {output}")
    print(f"Quality: {quality['status']}")
    print(f"Selected laps: {quality['selected_laps']}/{quality['requested_laps']}")
    print(f"Points: {quality['point_count']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
