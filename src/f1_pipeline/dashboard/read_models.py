from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
CURATED_DIR = PROJECT_ROOT / "data" / "curated"
MANIFEST_DIR = CURATED_DIR / "manifests"
MASTER_TABLES = frozenset(
    {"season", "meeting", "session", "country", "circuit", "driver", "team"}
)
READABLE_STATUSES = frozenset({"available", "stale", "partial"})


class DashboardDataError(RuntimeError):
    pass


@dataclass(frozen=True)
class SeasonCatalog:
    season: int
    meetings: pd.DataFrame
    sessions: pd.DataFrame
    countries: pd.DataFrame
    circuits: pd.DataFrame
    drivers: pd.DataFrame
    teams: pd.DataFrame
    manifest_path: Path


@dataclass(frozen=True)
class SessionBundle:
    session_key: int
    status: str
    frames: dict[str, pd.DataFrame]
    manifest: dict[str, Any]
    manifest_path: Path
    missing: tuple[str, ...]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _project_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise DashboardDataError(f"Could not read manifest {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DashboardDataError(f"Manifest {path} does not contain an object.")
    return payload


def _read_verified_parquet(path_value: str, expected_hash: str | None = None) -> pd.DataFrame:
    path = _project_path(path_value)
    if not path.is_file():
        raise DashboardDataError(f"Referenced data file is missing: {path}")
    if expected_hash and _sha256(path) != expected_hash:
        raise DashboardDataError(f"Referenced data file failed its hash check: {path}")
    try:
        return pd.read_parquet(path)
    except (OSError, ValueError) as exc:
        raise DashboardDataError(f"Could not read data file {path}: {exc}") from exc


def available_seasons(manifest_dir: Path = MANIFEST_DIR) -> tuple[int, ...]:
    seasons: set[int] = set()
    for path in manifest_dir.glob("master_data_*.json"):
        try:
            payload = _load_json(path)
            season = int(payload["season"])
            if payload.get("validation", {}).get("status") == "valid":
                seasons.add(season)
        except (DashboardDataError, KeyError, TypeError, ValueError):
            continue
    return tuple(sorted(seasons, reverse=True))


def load_master_table(
        season: int,
        table: str,
        *,
        manifest_dir: Path = MANIFEST_DIR,
) -> pd.DataFrame:
    if table not in MASTER_TABLES:
        raise DashboardDataError(f"Unsupported master table: {table}")
    manifest_path = manifest_dir / f"master_data_{season}.json"
    manifest = _load_json(manifest_path)
    if manifest.get("validation", {}).get("status") != "valid":
        raise DashboardDataError(f"Master data for {season} is not valid.")
    table_info = manifest.get("tables", {}).get(table)
    if not isinstance(table_info, dict) or not table_info.get("path"):
        raise DashboardDataError(f"Master data for {season} has no {table} table.")
    frame = _read_verified_parquet(str(table_info["path"]))
    expected_rows = table_info.get("row_count")
    if expected_rows is not None and len(frame) != int(str(expected_rows)):
        raise DashboardDataError(
            f"Master table {table} has {len(frame)} rows; expected {expected_rows}."
        )
    return frame


def load_season_catalog(
        season: int,
        *,
        manifest_dir: Path = MANIFEST_DIR,
) -> SeasonCatalog:
    manifest_path = manifest_dir / f"master_data_{season}.json"
    return SeasonCatalog(
        season=season,
        meetings=load_master_table(season, "meeting", manifest_dir=manifest_dir),
        sessions=load_master_table(season, "session", manifest_dir=manifest_dir),
        countries=load_master_table(season, "country", manifest_dir=manifest_dir),
        circuits=load_master_table(season, "circuit", manifest_dir=manifest_dir),
        drivers=load_master_table(season, "driver", manifest_dir=manifest_dir),
        teams=load_master_table(season, "team", manifest_dir=manifest_dir),
        manifest_path=manifest_path,
    )


def source_session_key(session_id: Any) -> int:
    try:
        return int(str(session_id).rsplit(":", 1)[-1])
    except (TypeError, ValueError) as exc:
        raise DashboardDataError(f"Invalid OpenF1 session ID: {session_id!r}") from exc


def source_meeting_key(meeting_id: Any) -> int:
    try:
        return int(str(meeting_id).rsplit(":", 1)[-1])
    except (TypeError, ValueError) as exc:
        raise DashboardDataError(f"Invalid OpenF1 meeting ID: {meeting_id!r}") from exc


def _manifest_timestamp(manifest: dict[str, Any], path: Path) -> pd.Timestamp:
    retrieved: list[pd.Timestamp] = []
    for result in manifest.get("endpoints", {}).values():
        if not isinstance(result, dict) or not result.get("retrieved_at"):
            continue
        parsed = pd.to_datetime(str(result["retrieved_at"]), utc=True, errors="coerce")
        if isinstance(parsed, pd.Timestamp) and pd.notna(parsed):
            retrieved.append(parsed)
    if retrieved:
        return max(retrieved)
    return pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")


def select_session_manifest(
        session_key: int,
        required_endpoints: Iterable[str],
        *,
        manifest_dir: Path = MANIFEST_DIR,
) -> tuple[dict[str, Any], Path]:
    required = tuple(dict.fromkeys(required_endpoints))
    candidates: list[tuple[pd.Timestamp, dict[str, Any], Path]] = []
    session_dir = manifest_dir / "openf1_sessions"
    for path in session_dir.glob(f"session_{session_key}_*.json"):
        try:
            manifest = _load_json(path)
        except DashboardDataError:
            continue
        if manifest.get("status") not in READABLE_STATUSES:
            continue
        endpoints = manifest.get("endpoints", {})
        if not all(
                isinstance(endpoints.get(name), dict)
                and endpoints[name].get("status") in READABLE_STATUSES
                for name in required
        ):
            continue
        candidates.append((_manifest_timestamp(manifest, path), manifest, path))
    if not candidates:
        requested = ", ".join(required) or "session metadata"
        raise DashboardDataError(
            f"No readable manifest for session {session_key} contains: {requested}."
        )
    _, manifest, path = max(candidates, key=lambda item: (item[0], item[2].name))
    return manifest, path


def load_session_bundle(
        session_key: int,
        endpoints: Iterable[str],
        *,
        layer: str = "silver",
        optional_endpoints: Iterable[str] = (),
        manifest_dir: Path = MANIFEST_DIR,
) -> SessionBundle:
    required = tuple(dict.fromkeys(endpoints))
    optional = tuple(name for name in dict.fromkeys(optional_endpoints) if name not in required)
    manifest, manifest_path = select_session_manifest(
        session_key,
        required,
        manifest_dir=manifest_dir,
    )
    path_key = "silver_path" if layer == "silver" else "raw_path"
    hash_key = "silver_sha256" if layer == "silver" else "raw_sha256"
    frames: dict[str, pd.DataFrame] = {}
    missing: list[str] = []
    endpoint_results = manifest.get("endpoints", {})
    for endpoint in (*required, *optional):
        result = endpoint_results.get(endpoint)
        if not isinstance(result, dict) or result.get("status") not in READABLE_STATUSES:
            if endpoint in required:
                raise DashboardDataError(
                    f"Session {session_key} has no readable {endpoint} data."
                )
            missing.append(endpoint)
            continue
        path_value = result.get(path_key)
        if not path_value:
            if endpoint in required:
                raise DashboardDataError(
                    f"Session {session_key} has no {layer} path for {endpoint}."
                )
            missing.append(endpoint)
            continue
        try:
            frames[endpoint] = _read_verified_parquet(
                str(path_value),
                str(result[hash_key]) if result.get(hash_key) else None,
            )
        except DashboardDataError:
            if endpoint in required:
                raise
            missing.append(endpoint)
    status = "partial" if missing or manifest.get("status") == "partial" else str(manifest["status"])
    return SessionBundle(
        session_key=session_key,
        status=status,
        frames=frames,
        manifest=manifest,
        manifest_path=manifest_path,
        missing=tuple(missing),
    )


def load_latest_standings(
        catalog: SeasonCatalog,
        endpoint: str,
        *,
        manifest_dir: Path = MANIFEST_DIR,
) -> pd.DataFrame:
    if endpoint not in {"championship_drivers", "championship_teams"}:
        raise DashboardDataError(f"Unsupported standing endpoint: {endpoint}")
    completed = catalog.sessions[catalog.sessions["status"].eq("completed")].copy()
    completed["scheduled_end_utc"] = pd.to_datetime(
        completed["scheduled_end_utc"], utc=True, errors="coerce"
    )
    completed = completed.sort_values("scheduled_end_utc", ascending=False)
    for session_id in completed["session_id"]:
        session_key = source_session_key(session_id)
        try:
            return load_session_bundle(
                session_key,
                (endpoint,),
                manifest_dir=manifest_dir,
            ).frames[endpoint]
        except DashboardDataError:
            continue
    return pd.DataFrame()


def load_season_results(
        catalog: SeasonCatalog,
        *,
        manifest_dir: Path = MANIFEST_DIR,
) -> pd.DataFrame:
    race_sessions = catalog.sessions[
        catalog.sessions["status"].eq("completed")
        & catalog.sessions["session_type"].astype(str).str.casefold().eq("race")
        ]
    frames: list[pd.DataFrame] = []
    for session_id in race_sessions["session_id"]:
        session_key = source_session_key(session_id)
        try:
            frame = load_session_bundle(
                session_key,
                ("session_result",),
                manifest_dir=manifest_dir,
            ).frames["session_result"]
        except DashboardDataError:
            continue
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).drop_duplicates(
        ["session_id", "driver_number"], keep="last"
    )


def load_forecast(
        session_key: int,
        *,
        manifest_dir: Path = MANIFEST_DIR,
) -> pd.DataFrame:
    candidates: list[tuple[pd.Timestamp, Path, str | None]] = []
    for path in manifest_dir.glob("weekend_weather_pipeline_*.json"):
        try:
            manifest = _load_json(path)
        except DashboardDataError:
            continue
        selection = manifest.get("selection", {})
        job = manifest.get("jobs", {}).get("open_meteo", {})
        if (
                selection.get("target_session_key") != session_key
                or not isinstance(job, dict)
                or job.get("status") not in READABLE_STATUSES
                or not job.get("curated_path")
        ):
            continue
        completed_at = pd.to_datetime(
            str(manifest.get("completed_at", "")), utc=True, errors="coerce"
        )
        if not isinstance(completed_at, pd.Timestamp) or pd.isna(completed_at):
            completed_at = pd.Timestamp(path.stat().st_mtime, unit="s", tz="UTC")
        candidates.append(
            (completed_at, _project_path(str(job["curated_path"])), job.get("curated_sha256"))
        )
    if not candidates:
        return pd.DataFrame()
    _, data_path, expected_hash = max(candidates, key=lambda item: item[0])
    return _read_verified_parquet(
        str(data_path), str(expected_hash) if expected_hash else None
    )
