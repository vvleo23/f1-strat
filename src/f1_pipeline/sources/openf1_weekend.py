from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.data_validation import DataValidationError, validate_frame
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.session_facts import FACT_NAMES, SessionFactError, normalize_session_fact
from f1_pipeline.settings import CURATED_DATA_DIR, PROJECT_ROOT, RAW_DATA_DIR
from f1_pipeline.sources.openf1 import (
    JsonClient,
    OpenF1Client,
    OpenF1Error,
    location_driver_cache_path,
    make_parquet_safe,
    session_cache_path,
    write_latest_parquet,
)

ENDPOINTS = (
    "sessions",
    "drivers",
    "laps",
    "intervals",
    "position",
    "pit",
    "stints",
    "race_control",
    "weather",
    "location",
    "session_result",
    "championship_drivers",
    "championship_teams",
)
REQUIRED_BY_SESSION_TYPE = {
    "practice": frozenset({"drivers", "laps", "stints", "weather"}),
    "qualifying": frozenset({"drivers", "laps", "position", "stints", "weather"}),
    "sprint_qualifying": frozenset({"drivers", "laps", "position", "stints", "weather"}),
    "sprint": frozenset(
        {"drivers", "laps", "intervals", "position", "stints", "race_control", "weather"}
    ),
    "race": frozenset(
        {
            "drivers",
            "laps",
            "intervals",
            "position",
            "pit",
            "stints",
            "race_control",
            "weather",
        }
    ),
}
OPTIONAL_BY_SESSION_TYPE = {
    "practice": frozenset({"position", "pit", "race_control"}),
    "qualifying": frozenset({"pit", "race_control", "session_result"}),
    "sprint_qualifying": frozenset({"pit", "race_control", "session_result"}),
    "sprint": frozenset(
        {"pit", "session_result", "championship_drivers", "championship_teams"}
    ),
    "race": frozenset(
        {"session_result", "championship_drivers", "championship_teams"}
    ),
}
ALLOW_EMPTY = frozenset({"intervals", "pit", "race_control"})
REQUIRED_COLUMNS = {
    "sessions": {"session_key", "meeting_key", "session_name", "date_start"},
    "drivers": {"session_key", "driver_number", "name_acronym"},
    "laps": {"session_key", "driver_number", "lap_number", "date_start"},
    "intervals": {"session_key", "driver_number", "date", "gap_to_leader", "interval"},
    "position": {"session_key", "driver_number", "date", "position"},
    "pit": {"session_key", "driver_number", "date", "lap_number"},
    "stints": {"session_key", "driver_number", "stint_number", "lap_start", "compound"},
    "race_control": {"session_key", "date", "message"},
    "weather": {"session_key", "date"},
    "location": {"session_key", "driver_number", "date", "x", "y", "z"},
    "session_result": {"session_key", "meeting_key", "driver_number", "position"},
    "championship_drivers": {
        "session_key",
        "meeting_key",
        "driver_number",
        "position_current",
        "points_current",
    },
    "championship_teams": {
        "session_key",
        "meeting_key",
        "team_name",
        "position_current",
        "points_current",
    },
}
KEY_COLUMNS = {
    "sessions": ("session_key",),
    "drivers": ("session_key", "driver_number"),
    "laps": ("session_key", "driver_number", "lap_number"),
    "intervals": ("session_key", "driver_number", "date"),
    "position": ("session_key", "driver_number", "date"),
    "pit": ("session_key", "driver_number", "date"),
    "stints": ("session_key", "driver_number", "stint_number"),
    "race_control": ("session_key", "date", "message"),
    "weather": ("session_key", "date"),
    "location": ("session_key", "driver_number", "date"),
    "session_result": ("session_key", "driver_number"),
    "championship_drivers": ("session_key", "driver_number"),
    "championship_teams": ("session_key", "team_name"),
}
DATETIME_COLUMNS = {
    "sessions": ("date_start",),
    "drivers": (),
    "laps": ("date_start",),
    "intervals": ("date",),
    "position": ("date",),
    "pit": ("date",),
    "stints": (),
    "race_control": ("date",),
    "weather": ("date",),
    "location": ("date",),
    "session_result": (),
    "championship_drivers": (),
    "championship_teams": (),
}
NUMERIC_COLUMNS = {
    "sessions": ("session_key", "meeting_key"),
    "drivers": ("session_key", "driver_number"),
    "laps": ("session_key", "driver_number", "lap_number", "lap_duration"),
    "intervals": ("session_key", "driver_number"),
    "position": ("session_key", "driver_number", "position"),
    "pit": ("session_key", "driver_number", "lap_number", "pit_duration"),
    "stints": (
        "session_key",
        "driver_number",
        "stint_number",
        "lap_start",
        "lap_end",
        "tyre_age_at_start",
    ),
    "race_control": ("session_key", "driver_number", "lap_number", "sector"),
    "weather": (
        "session_key",
        "air_temperature",
        "track_temperature",
        "humidity",
        "pressure",
        "rainfall",
        "wind_speed",
        "wind_direction",
    ),
    "location": ("session_key", "driver_number", "x", "y", "z"),
    "session_result": (
        "session_key",
        "meeting_key",
        "driver_number",
        "position",
        "number_of_laps",
        "points",
    ),
    "championship_drivers": (
        "session_key",
        "meeting_key",
        "driver_number",
        "position_start",
        "position_current",
        "points_start",
        "points_current",
    ),
    "championship_teams": (
        "session_key",
        "meeting_key",
        "position_start",
        "position_current",
        "points_start",
        "points_current",
    ),
}
REQUIRED_NON_NULL = {
    "sessions": ("session_key", "meeting_key", "session_name", "date_start"),
    "drivers": ("session_key", "driver_number", "name_acronym"),
    "laps": ("session_key", "driver_number", "lap_number"),
    "intervals": ("session_key", "driver_number", "date"),
    "position": ("session_key", "driver_number", "date", "position"),
    "pit": ("session_key", "driver_number", "date", "lap_number"),
    "stints": ("session_key", "driver_number", "stint_number", "lap_start"),
    "race_control": ("session_key", "date", "message"),
    "weather": ("session_key", "date"),
    "location": ("session_key", "driver_number", "date", "x", "y", "z"),
    "session_result": ("session_key", "meeting_key", "driver_number"),
    "championship_drivers": (
        "session_key",
        "meeting_key",
        "driver_number",
        "position_current",
        "points_current",
    ),
    "championship_teams": (
        "session_key",
        "meeting_key",
        "team_name",
        "position_current",
        "points_current",
    ),
}


class OpenF1WeekendError(RuntimeError):
    pass


class OpenF1WeekendClient(OpenF1Client):
    def __init__(self) -> None:
        super().__init__(error_type=OpenF1WeekendError, user_agent="f1-strat/1.0")


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def normalize_session_type(session_type: str, session_name: str = "") -> str:
    value = f"{session_type} {session_name}".casefold()
    if "sprint" in value and ("qualifying" in value or "shootout" in value):
        return "sprint_qualifying"
    if "sprint" in value:
        return "sprint"
    if "practice" in value:
        return "practice"
    if "qualifying" in value:
        return "qualifying"
    if "race" in value:
        return "race"
    raise OpenF1WeekendError(
        f"Unsupported session type '{session_type}' and name '{session_name}'."
    )


def plan_weekend_sessions(
        sessions: list[dict[str, Any]],
        *,
        purpose: str = "weekend",
) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    for session in sessions:
        if str(session.get("status", "")).casefold() == "cancelled":
            continue
        session_key = session.get("source_session_key")
        if session_key is None:
            session_id = str(session.get("session_id", ""))
            try:
                session_key = int(session_id.rsplit(":", 1)[-1])
            except ValueError as exc:
                raise OpenF1WeekendError(f"Invalid session identity: {session_id}") from exc
        normalized_type = normalize_session_type(
            str(session.get("session_type", "")), str(session.get("session_name", ""))
        )
        required = REQUIRED_BY_SESSION_TYPE[normalized_type]
        optional = OPTIONAL_BY_SESSION_TYPE[normalized_type]
        if purpose == "replay":
            required = required | {"sessions", "location"}
        elif purpose == "weekend_complete_v1":
            required = required | {"sessions"}
            if normalized_type in {"sprint", "race"}:
                required = required | {"location"}
        skipped = frozenset(ENDPOINTS).difference(required | optional)
        plans.append(
            {
                **session,
                "source_session_key": int(session_key),
                "normalized_session_type": normalized_type,
                "required_endpoints": sorted(required),
                "optional_endpoints": sorted(optional),
                "skipped_endpoints": sorted(skipped),
            }
        )
    return plans


def _validate_source_frame(endpoint: str, frame: pd.DataFrame, session_key: int) -> None:
    if frame.empty:
        if endpoint not in ALLOW_EMPTY:
            raise OpenF1WeekendError(
                f"OpenF1 endpoint '{endpoint}' returned no rows for session {session_key}."
            )
        return
    numeric_columns = tuple(
        column for column in NUMERIC_COLUMNS[endpoint] if column in frame.columns
    )
    try:
        validate_frame(
            frame,
            name=f"OpenF1 {endpoint}",
            required_columns=REQUIRED_COLUMNS[endpoint],
            key_columns=KEY_COLUMNS[endpoint],
            datetime_columns=DATETIME_COLUMNS[endpoint],
            numeric_columns=numeric_columns,
            required_non_null=REQUIRED_NON_NULL[endpoint],
            expected_session_key=session_key,
            allow_empty=endpoint in ALLOW_EMPTY,
        )
    except DataValidationError as exc:
        raise OpenF1WeekendError(str(exc)) from exc


def _snapshot_path(
        raw_dir: Path, session_key: int, endpoint: str, frame: pd.DataFrame
) -> Path:
    snapshot_dir = raw_dir / "snapshots" / "openf1" / str(session_key)
    temporary_path = snapshot_dir / f".{endpoint}_{time.time_ns()}.parquet"
    atomic_parquet(frame, temporary_path)
    digest = sha256(temporary_path)
    target = snapshot_dir / f"{endpoint}_{digest[:20]}.parquet"
    if target.exists():
        temporary_path.unlink(missing_ok=True)
    else:
        temporary_path.replace(target)
    return target


def _cached_snapshot(
        raw_dir: Path, session_key: int, endpoint: str
) -> tuple[pd.DataFrame, Path] | None:
    latest = session_cache_path(session_key, endpoint)
    if raw_dir != RAW_DATA_DIR:
        latest = raw_dir / latest.name
    if not latest.exists():
        return None
    frame = pd.read_parquet(latest)
    if frame.empty and endpoint in ALLOW_EMPTY:
        for column in REQUIRED_COLUMNS[endpoint]:
            if column not in frame.columns:
                frame[column] = pd.Series(dtype="object")
    _validate_source_frame(endpoint, frame, session_key)
    snapshot = _snapshot_path(raw_dir, session_key, endpoint, frame)
    return frame, snapshot


def _load_endpoint(
        client: JsonClient,
        endpoint: str,
        session_key: int,
        *,
        refresh: bool,
        raw_dir: Path,
        driver_numbers: list[int] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if not refresh:
        try:
            cached = _cached_snapshot(raw_dir, session_key, endpoint)
        except (OpenF1WeekendError, OSError, ValueError):
            cached = None
        if cached is not None:
            frame, snapshot = cached
            retrieved_at = datetime.fromtimestamp(snapshot.stat().st_mtime, timezone.utc)
            return frame, {
                "status": "stale",
                "fetched": False,
                "row_count": len(frame),
                "raw_path": _relative(snapshot),
                "raw_sha256": sha256(snapshot),
                "retrieved_at": retrieved_at.isoformat(),
                "empty": frame.empty,
            }
    retrieved_at = datetime.now(timezone.utc)
    if endpoint == "location":
        if not driver_numbers:
            raise OpenF1WeekendError(
                f"OpenF1 location requires drivers for session {session_key}."
            )
        location_frames: list[pd.DataFrame] = []
        for driver_number in driver_numbers:
            driver_path = location_driver_cache_path(session_key, driver_number)
            if raw_dir != RAW_DATA_DIR:
                driver_path = raw_dir / driver_path.name
            if driver_path.exists() and not refresh:
                driver_frame = pd.read_parquet(driver_path)
            else:
                payload = client.get_json(
                    endpoint,
                    {"session_key": session_key, "driver_number": driver_number},
                )
                driver_frame = make_parquet_safe(endpoint, pd.DataFrame(payload))
                write_latest_parquet(driver_frame, driver_path)
            location_frames.append(driver_frame)
        frame = pd.concat(location_frames, ignore_index=True)
    else:
        payload = client.get_json(endpoint, {"session_key": session_key})
        frame = make_parquet_safe(
            endpoint,
            pd.DataFrame(payload, columns=sorted(REQUIRED_COLUMNS[endpoint]))
            if not payload
            else pd.DataFrame(payload),
        )
    _validate_source_frame(endpoint, frame, session_key)
    snapshot = _snapshot_path(raw_dir, session_key, endpoint, frame)
    latest = session_cache_path(session_key, endpoint)
    if raw_dir != RAW_DATA_DIR:
        latest = raw_dir / latest.name
    write_latest_parquet(frame, latest)
    return frame, {
        "status": "available",
        "fetched": True,
        "row_count": len(frame),
        "raw_path": _relative(snapshot),
        "raw_sha256": sha256(snapshot),
        "retrieved_at": retrieved_at.isoformat(),
        "empty": frame.empty,
    }


def _session_status(
        required: set[str], optional: set[str], endpoints: dict[str, dict[str, Any]]
) -> str:
    required_failed = any(
        endpoints[name]["status"] == "unavailable"
        or (endpoints[name].get("empty") and name not in ALLOW_EMPTY)
        for name in required
    )
    if required_failed:
        return "unavailable"
    optional_failed = any(
        endpoints[name]["status"] == "unavailable" or endpoints[name].get("empty")
        for name in optional
    )
    if optional_failed:
        return "partial"
    if all(result["status"] == "stale" for result in endpoints.values()):
        return "stale"
    return "available"


def ingest_session(
        plan: dict[str, Any],
        *,
        client: JsonClient,
        refresh: bool,
        raw_dir: Path,
        curated_dir: Path,
) -> dict[str, Any]:
    session_key = int(plan["source_session_key"])
    required = set(plan["required_endpoints"])
    optional = set(plan["optional_endpoints"])
    endpoint_results: dict[str, dict[str, Any]] = {}
    frames: dict[str, pd.DataFrame] = {}
    for endpoint in sorted(required | optional):
        try:
            driver_numbers = None
            if endpoint == "location" and "drivers" in frames:
                driver_numbers = sorted(
                    frames["drivers"]["driver_number"].astype(int).unique().tolist()
                )
            frame, result = _load_endpoint(
                client,
                endpoint,
                session_key,
                refresh=refresh,
                raw_dir=raw_dir,
                driver_numbers=driver_numbers,
            )
            frames[endpoint] = frame
            endpoint_results[endpoint] = result
        except (OpenF1Error, OpenF1WeekendError, OSError, ValueError, TypeError) as exc:
            endpoint_results[endpoint] = {
                "status": "unavailable",
                "row_count": 0,
                "error": str(exc),
            }

    ingested_at = pd.Timestamp(datetime.now(timezone.utc))
    for endpoint, frame in frames.items():
        if endpoint not in FACT_NAMES:
            continue
        result = endpoint_results[endpoint]
        retrieved_at = pd.Timestamp(result["retrieved_at"])
        try:
            fact_name, facts = normalize_session_fact(
                endpoint,
                frame,
                session_key=session_key,
                retrieved_at=retrieved_at,
                ingested_at=ingested_at,
                raw_path=Path(result["raw_path"]),
                raw_sha256=str(result["raw_sha256"]),
                laps=frames.get("laps"),
            )
            fact_path = (
                    curated_dir
                    / "facts"
                    / fact_name
                    / f"openf1_session_{session_key}_{str(result['raw_sha256'])[:16]}.parquet"
            )
            if not fact_path.exists():
                atomic_parquet(facts, fact_path)
            result["silver_path"] = _relative(fact_path)
            result["silver_sha256"] = sha256(fact_path)
            result["silver_row_count"] = len(facts)
        except (SessionFactError, OSError, ValueError, TypeError) as exc:
            result["status"] = "unavailable"
            result["error"] = f"Silver transformation failed: {exc}"

    status = _session_status(required, optional, endpoint_results)
    identity = json.dumps(
        {
            "session_key": session_key,
            "endpoints": {
                name: {
                    "raw_sha256": result.get("raw_sha256"),
                    "silver_sha256": result.get("silver_sha256"),
                    "retrieved_at": result.get("retrieved_at"),
                }
                for name, result in endpoint_results.items()
            },
        },
        sort_keys=True,
    )
    manifest_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    manifest_path = (
            curated_dir
            / "manifests"
            / "openf1_sessions"
            / f"session_{session_key}_{manifest_id}.json"
    )
    manifest = {
        "schema_version": 1,
        "session_key": session_key,
        "session_id": plan["session_id"],
        "session_type": plan["normalized_session_type"],
        "status": status,
        "required_endpoints": sorted(required),
        "optional_endpoints": sorted(optional),
        "skipped_endpoints": plan["skipped_endpoints"],
        "endpoints": endpoint_results,
    }
    if manifest_path.exists():
        with manifest_path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
    else:
        atomic_json(manifest, manifest_path)
    manifest["manifest_path"] = _relative(manifest_path)
    manifest["manifest_sha256"] = sha256(manifest_path)
    return manifest


def ingest_weekend(
        sessions: list[dict[str, Any]],
        *,
        meeting_key: int,
        purpose: str = "weekend",
        refresh: bool = False,
        client: JsonClient | None = None,
        raw_dir: Path = RAW_DATA_DIR,
        curated_dir: Path = CURATED_DATA_DIR,
) -> tuple[dict[str, Any], Path]:
    plans = plan_weekend_sessions(sessions, purpose=purpose)
    if not plans:
        raise OpenF1WeekendError(f"Meeting {meeting_key} has no ingestible sessions.")
    api = client or OpenF1WeekendClient()
    session_results = [
        ingest_session(
            plan,
            client=api,
            refresh=refresh,
            raw_dir=raw_dir,
            curated_dir=curated_dir,
        )
        for plan in plans
    ]
    statuses = {result["status"] for result in session_results}
    if statuses == {"stale"}:
        status = "stale"
    elif statuses.issubset({"available", "stale"}):
        status = "available"
    elif statuses.intersection({"available", "stale", "partial"}):
        status = "partial"
    else:
        status = "unavailable"
    identity = json.dumps(
        {
            "meeting_key": meeting_key,
            "sessions": {
                result["session_id"]: result["manifest_sha256"]
                for result in session_results
            },
        },
        sort_keys=True,
    )
    run_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    manifest_path = (
            curated_dir
            / "manifests"
            / f"openf1_weekend_{meeting_key}_{run_id}.json"
    )
    manifest = {
        "schema_version": 1,
        "run_id": run_id,
        "meeting_key": meeting_key,
        "status": status,
        "sessions": session_results,
    }
    if manifest_path.exists():
        with manifest_path.open(encoding="utf-8") as stream:
            manifest = json.load(stream)
    else:
        atomic_json(manifest, manifest_path)
    return manifest, manifest_path
