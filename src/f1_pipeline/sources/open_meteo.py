from __future__ import annotations

import json
import math
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from f1_pipeline.data_validation import DataValidationError, validate_frame
from f1_pipeline.persistence import atomic_json, sha256
from f1_pipeline.settings import PROJECT_ROOT, RAW_DATA_DIR
from f1_pipeline.sources.wikidata import CircuitReference

API_URL = "https://single-runs-api.open-meteo.com/v1/forecast"
REQUEST_TIMEOUT_SECONDS = 60
MODEL = "ecmwf_ifs"
HOURLY_VARIABLES = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "rain",
    "cloud_cover",
    "weather_code",
    "wind_speed_10m",
    "wind_direction_10m",
    "surface_pressure",
)


class OpenMeteoError(RuntimeError):
    pass


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


class OpenMeteoClient:
    def __init__(self) -> None:
        try:
            import truststore
        except ImportError as exc:
            raise OpenMeteoError(
                "The 'truststore' package is missing. Run 'pip install -r requirements.txt'."
            ) from exc
        truststore.inject_into_ssl()
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
        self.session = requests.Session()
        self.session.mount("https://", HTTPAdapter(max_retries=retry))
        self.session.headers.update({"User-Agent": "f1-strat/1.0"})

    def get_forecast(self, params: dict[str, Any]) -> dict[str, Any]:
        try:
            response = self.session.get(API_URL, params=params, timeout=REQUEST_TIMEOUT_SECONDS)
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.Timeout as exc:
            raise OpenMeteoError("Open-Meteo request timed out.") from exc
        except requests.exceptions.JSONDecodeError as exc:
            raise OpenMeteoError("Open-Meteo did not return valid JSON.") from exc
        except requests.exceptions.RequestException as exc:
            raise OpenMeteoError(f"Open-Meteo request failed: {exc}") from exc
        if not isinstance(payload, dict):
            raise OpenMeteoError("Open-Meteo returned an unexpected response format.")
        return payload


def utc_timestamp(value: str | datetime | pd.Timestamp, name: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if not isinstance(parsed, pd.Timestamp) or pd.isna(parsed):
        raise OpenMeteoError(f"{name} must be a valid UTC timestamp.")
    return parsed


def select_historical_single_run(
        decision_time: str | datetime | pd.Timestamp,
        *,
        publication_lag: timedelta = timedelta(hours=6),
        cycle_hours: tuple[int, ...] = (0, 6, 12, 18),
) -> tuple[pd.Timestamp, pd.Timestamp]:
    cut_time = utc_timestamp(decision_time, "decision_time")
    if publication_lag < timedelta(0):
        raise OpenMeteoError("publication_lag must not be negative.")
    cycles = tuple(sorted(set(cycle_hours)))
    if not cycles or any(isinstance(hour, bool) or not 0 <= hour <= 23 for hour in cycles):
        raise OpenMeteoError("cycle_hours must contain UTC hours from 0 to 23.")
    latest_initialization = cut_time - publication_lag
    eligible = [hour for hour in cycles if hour <= latest_initialization.hour]
    run_day = latest_initialization.normalize()
    if eligible:
        run_hour = eligible[-1]
    else:
        run_day -= pd.Timedelta(days=1)
        run_hour = cycles[-1]
    run_time = run_day + pd.Timedelta(int(run_hour), unit="h")
    available_at = run_time + pd.Timedelta(publication_lag)
    if available_at > cut_time:
        raise OpenMeteoError("No historical model run is available at decision_time.")
    return run_time, available_at


def validate_forecast_horizon(
        forecast: pd.DataFrame,
        target_time: str | datetime | pd.Timestamp,
) -> None:
    if forecast.empty or "valid_time" not in forecast.columns:
        raise OpenMeteoError("The forecast has no validity horizon.")
    valid_times = pd.to_datetime(forecast["valid_time"], utc=True, errors="coerce")
    if valid_times.isna().all():
        raise OpenMeteoError("The forecast has no valid timestamps.")
    target = utc_timestamp(target_time, "target_time")
    if target < valid_times.min() or target > valid_times.max():
        raise OpenMeteoError("The forecast horizon does not cover the target session.")


def normalize_forecast(
        payload: dict[str, Any],
        *,
        snapshot_id: str,
        session_id: str,
        circuit_id: str,
        reference: CircuitReference,
        run_initialized_at: pd.Timestamp,
        available_at: pd.Timestamp,
        retrieved_at: pd.Timestamp,
        decision_time: pd.Timestamp,
        raw_path: Path,
) -> pd.DataFrame:
    if available_at > decision_time:
        raise OpenMeteoError("The forecast was not available at decision_time.")
    if available_at < run_initialized_at:
        raise OpenMeteoError("available_at must not precede run_initialized_at.")
    hourly = payload.get("hourly")
    units = payload.get("hourly_units")
    if not isinstance(hourly, dict) or not isinstance(units, dict):
        raise OpenMeteoError("Open-Meteo response has no hourly data and units.")
    times = hourly.get("time")
    if not isinstance(times, list) or not times:
        raise OpenMeteoError("Open-Meteo response has no forecast times.")
    row_count = len(times)
    for variable in HOURLY_VARIABLES:
        values = hourly.get(variable)
        if not isinstance(values, list) or len(values) != row_count:
            raise OpenMeteoError(f"Open-Meteo hourly variable '{variable}' has an invalid length.")
        if variable not in units:
            raise OpenMeteoError(f"Open-Meteo unit for '{variable}' is missing.")
    valid_times = pd.to_datetime(pd.Series(times), utc=True, errors="coerce")
    if valid_times.isna().any():
        raise OpenMeteoError("Open-Meteo forecast contains invalid times.")
    try:
        grid_latitude = float(payload["latitude"])
        grid_longitude = float(payload["longitude"])
        elevation = float(payload["elevation"])
    except (KeyError, TypeError, ValueError) as exc:
        raise OpenMeteoError("Open-Meteo response has invalid grid coordinates.") from exc
    if not all(math.isfinite(value) for value in (grid_latitude, grid_longitude, elevation)):
        raise OpenMeteoError("Open-Meteo response has non-finite grid metadata.")
    frame = pd.DataFrame({"valid_time": valid_times})
    for variable in HOURLY_VARIABLES:
        frame[variable] = hourly[variable]
    frame.insert(0, "snapshot_id", snapshot_id)
    frame.insert(1, "session_id", session_id)
    frame.insert(2, "circuit_id", circuit_id)
    frame["source_system"] = "open_meteo"
    frame["model"] = MODEL
    frame["latitude"] = reference.latitude
    frame["longitude"] = reference.longitude
    frame["grid_latitude"] = grid_latitude
    frame["grid_longitude"] = grid_longitude
    frame["elevation"] = elevation
    frame["run_initialized_at"] = run_initialized_at
    frame["available_at"] = available_at
    frame["retrieved_at"] = retrieved_at
    frame["decision_time"] = decision_time
    frame["lead_time_minutes"] = (
                                         frame["valid_time"] - run_initialized_at
                                 ).dt.total_seconds() / 60
    frame["availability_basis"] = "conservative_documented_latency"
    frame["units_json"] = json.dumps(
        {variable: units[variable] for variable in HOURLY_VARIABLES},
        sort_keys=True,
    )
    frame["raw_path"] = _relative_path(raw_path)
    frame["raw_sha256"] = sha256(raw_path)
    frame["status"] = "available"
    frame["schema_version"] = 1
    try:
        validate_frame(
            frame,
            name="Open-Meteo forecast",
            required_columns={
                "snapshot_id",
                "session_id",
                "circuit_id",
                "valid_time",
                "run_initialized_at",
                "available_at",
                "retrieved_at",
                *HOURLY_VARIABLES,
            },
            key_columns=("snapshot_id", "valid_time"),
            datetime_columns=(
                "valid_time",
                "run_initialized_at",
                "available_at",
                "retrieved_at",
            ),
            required_non_null=(
                "snapshot_id",
                "session_id",
                "circuit_id",
                "valid_time",
                "run_initialized_at",
                "available_at",
                "retrieved_at",
            ),
        )
    except DataValidationError as exc:
        raise OpenMeteoError(str(exc)) from exc
    return frame


def load_forecast(
        reference: CircuitReference,
        *,
        session_id: str,
        circuit_id: str,
        run_initialized_at: str | datetime | pd.Timestamp,
        available_at: str | datetime | pd.Timestamp,
        decision_time: str | datetime | pd.Timestamp,
        refresh: bool = False,
        client: OpenMeteoClient | None = None,
        retrieved_at: datetime | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    run_time = utc_timestamp(run_initialized_at, "run_initialized_at")
    availability_time = utc_timestamp(available_at, "available_at")
    cut_time = utc_timestamp(decision_time, "decision_time")
    checked_at = utc_timestamp(retrieved_at or datetime.now(timezone.utc), "retrieved_at")
    params = {
        "latitude": reference.latitude,
        "longitude": reference.longitude,
        "run": run_time.strftime("%Y-%m-%dT%H:%M"),
        "hourly": ",".join(HOURLY_VARIABLES),
        "models": MODEL,
        "timezone": "GMT",
    }
    snapshot_dir = RAW_DATA_DIR / "snapshots" / "open_meteo"
    run_key = run_time.strftime("%Y%m%dT%H%M%SZ")
    existing: list[Path] = sorted(
        snapshot_dir.glob(f"{reference.wikidata_entity_id}_{MODEL}_{run_key}_*.json")
    )
    fetched = refresh or not existing
    payload: dict[str, Any] = {}
    raw_path: Path
    if fetched:
        payload = (client or OpenMeteoClient()).get_forecast(params)
        retrieved_key = checked_at.strftime("%Y%m%dT%H%M%S%fZ")
        raw_path = snapshot_dir / (
            f"{reference.wikidata_entity_id}_{MODEL}_{run_key}_{retrieved_key}.json"
        )
        atomic_json(payload, raw_path)
        snapshot_retrieved_at = checked_at
    else:
        raw_path = existing[-1]
        with raw_path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
        snapshot_retrieved_at = pd.Timestamp(
            datetime.fromtimestamp(raw_path.stat().st_mtime, timezone.utc)
        )
    snapshot_id = f"open_meteo:{raw_path.stem}"
    frame = normalize_forecast(
        payload,
        snapshot_id=snapshot_id,
        session_id=session_id,
        circuit_id=circuit_id,
        reference=reference,
        run_initialized_at=run_time,
        available_at=availability_time,
        retrieved_at=snapshot_retrieved_at,
        decision_time=cut_time,
        raw_path=raw_path,
    )
    return frame, {
        "status": "available" if fetched else "stale",
        "fetched": fetched,
        "request": {"url": API_URL, "parameters": params},
        "raw_path": _relative_path(raw_path),
        "raw_sha256": sha256(raw_path),
        "row_count": len(frame),
    }
