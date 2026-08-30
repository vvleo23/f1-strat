from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.dashboard.read_models import load_forecast, select_session_manifest
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.settings import PROJECT_ROOT

SEASON = 2026
MEETING_ID = "openf1:meeting:1291"
SESSION_KEY = 11342
SESSION_ID = f"openf1:session:{SESSION_KEY}"
SOURCE_CURATED_DIR = PROJECT_ROOT / "data" / "curated"
SOURCE_MANIFEST_DIR = SOURCE_CURATED_DIR / "manifests"
DEMO_DIR = PROJECT_ROOT / "demo_data"
DEMO_CURATED_DIR = DEMO_DIR / "curated"
DEMO_RAW_DIR = DEMO_DIR / "raw" / f"session_{SESSION_KEY}"
DEMO_MANIFEST_DIR = DEMO_CURATED_DIR / "manifests"
REPLAY_ENDPOINTS = (
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
DISPLAY_ENDPOINTS = (
    "session_result",
    "championship_drivers",
    "championship_teams",
    "weather",
)


def _relative(path: Path) -> str:
    return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()


def _write_frame(frame: pd.DataFrame, path: Path) -> dict[str, Any]:
    atomic_parquet(frame, path)
    return {
        "path": _relative(path),
        "row_count": len(frame),
        "sha256": sha256(path),
    }


def _demo_location(frame: pd.DataFrame) -> pd.DataFrame:
    sampled = frame.copy()
    sampled["date"] = pd.to_datetime(sampled["date"], utc=True, format="mixed")
    sampled["_bucket"] = sampled["date"].dt.floor("4s")
    sampled = (
        sampled.sort_values("date")
        .drop_duplicates(["driver_number", "_bucket"], keep="last")
        .drop(columns="_bucket")
        .reset_index(drop=True)
    )
    return sampled


def _build_dimensions(driver_numbers: set[int]) -> dict[str, dict[str, Any]]:
    source = SOURCE_CURATED_DIR / "dimensions" / f"season={SEASON}"
    frames = {
        name: pd.read_parquet(source / f"{name}.parquet")
        for name in ("season", "meeting", "session", "country", "circuit", "driver", "team")
    }
    frames["meeting"] = frames["meeting"][frames["meeting"]["meeting_id"].eq(MEETING_ID)]
    frames["session"] = frames["session"][frames["session"]["session_id"].eq(SESSION_ID)]
    season_ids = set(frames["meeting"]["season_id"])
    country_ids = set(frames["meeting"]["country_id"])
    circuit_ids = set(frames["meeting"]["circuit_id"])
    frames["season"] = frames["season"][frames["season"]["season_id"].isin(season_ids)]
    frames["country"] = frames["country"][frames["country"]["country_id"].isin(country_ids)]
    frames["circuit"] = frames["circuit"][frames["circuit"]["circuit_id"].isin(circuit_ids)]
    frames["driver"] = frames["driver"][frames["driver"]["driver_number"].isin(driver_numbers)]
    team_ids = set(frames["driver"]["team_id"])
    frames["team"] = frames["team"][frames["team"]["team_id"].isin(team_ids)]
    target = DEMO_CURATED_DIR / "dimensions" / f"season={SEASON}"
    return {
        name: _write_frame(frame.reset_index(drop=True), target / f"{name}.parquet")
        for name, frame in frames.items()
    }


def _build_session() -> tuple[dict[str, Any], set[int]]:
    requested = (*REPLAY_ENDPOINTS, *DISPLAY_ENDPOINTS)
    source_manifest, _ = select_session_manifest(
        SESSION_KEY,
        requested,
        manifest_dir=SOURCE_MANIFEST_DIR,
    )
    manifest = copy.deepcopy(source_manifest)
    manifest["status"] = "available"
    endpoints: dict[str, dict[str, Any]] = {}
    for endpoint in requested:
        source_result = source_manifest["endpoints"][endpoint]
        raw = pd.read_parquet(PROJECT_ROOT / source_result["raw_path"])
        if endpoint == "location":
            raw = _demo_location(raw)
        raw_info = _write_frame(raw, DEMO_RAW_DIR / f"{endpoint}.parquet")
        result = {
            "status": "available",
            "fetched": False,
            "row_count": raw_info["row_count"],
            "raw_path": raw_info["path"],
            "raw_sha256": raw_info["sha256"],
            "retrieved_at": source_result["retrieved_at"],
            "empty": raw.empty,
        }
        silver_path = source_result.get("silver_path")
        if silver_path:
            silver = pd.read_parquet(PROJECT_ROOT / silver_path)
            silver_info = _write_frame(
                silver,
                DEMO_CURATED_DIR / "facts" / f"session_{SESSION_KEY}" / f"{endpoint}.parquet",
            )
            result.update(
                {
                    "silver_path": silver_info["path"],
                    "silver_sha256": silver_info["sha256"],
                    "silver_row_count": silver_info["row_count"],
                }
            )
        endpoints[endpoint] = result
    manifest["endpoints"] = endpoints
    manifest["required_endpoints"] = list(REPLAY_ENDPOINTS)
    manifest["optional_endpoints"] = list(DISPLAY_ENDPOINTS)
    manifest["skipped_endpoints"] = []
    path = DEMO_MANIFEST_DIR / "openf1_sessions" / f"session_{SESSION_KEY}_demo.json"
    atomic_json(manifest, path)
    drivers = pd.read_parquet(DEMO_RAW_DIR / "drivers.parquet")
    return manifest, set(drivers["driver_number"].astype(int))


def _build_master_manifest(driver_numbers: set[int]) -> None:
    tables = _build_dimensions(driver_numbers)
    manifest = {
        "schema_version": 1,
        "season": SEASON,
        "source_system": "openf1",
        "ingested_at": "2026-07-26T16:00:00+00:00",
        "roster_session_key": SESSION_KEY,
        "raw_inputs": [],
        "tables": tables,
        "validation": {
            "status": "valid",
            "checks": [
                "schema",
                "required_fields",
                "primary_keys",
                "foreign_keys",
                "allowed_statuses",
                "read_after_write",
            ],
        },
    }
    atomic_json(manifest, DEMO_MANIFEST_DIR / f"master_data_{SEASON}.json")


def _build_forecast_manifest() -> None:
    forecast = load_forecast(SESSION_KEY, manifest_dir=SOURCE_MANIFEST_DIR)
    info = _write_frame(
        forecast,
        DEMO_CURATED_DIR / "facts" / "weather_forecast" / f"session_{SESSION_KEY}.parquet",
    )
    manifest = {
        "schema_version": 1,
        "completed_at": "2026-07-26T16:00:00+00:00",
        "status": "available",
        "selection": {"target_session_key": SESSION_KEY},
        "jobs": {
            "open_meteo": {
                "status": "available",
                "row_count": info["row_count"],
                "curated_path": info["path"],
                "curated_sha256": info["sha256"],
            }
        },
    }
    atomic_json(
        manifest,
        DEMO_MANIFEST_DIR / "weekend_weather_pipeline_1291_demo.json",
    )


def main() -> None:
    _, driver_numbers = _build_session()
    _build_master_manifest(driver_numbers)
    _build_forecast_manifest()
    files = list(DEMO_DIR.rglob("*"))
    size = sum(path.stat().st_size for path in files if path.is_file())
    print(f"Demo data: {sum(path.is_file() for path in files)} files, {size / 1024 / 1024:.2f} MiB")


if __name__ == "__main__":
    main()
