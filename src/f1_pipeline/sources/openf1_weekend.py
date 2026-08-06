from __future__ import annotations

import hashlib
import json
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from f1_pipeline.data_validation import DataValidationError, validate_frame
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.session_facts import FACT_NAMES, SessionFactError, normalize_session_fact
from f1_pipeline.settings import CURATED_DATA_DIR, PROJECT_ROOT, RAW_DATA_DIR

BASE_URL = "https://api.openf1.org/v1"
REQUEST_TIMEOUT_SECONDS = 90
MIN_REQUEST_INTERVAL_SECONDS = 2.1
ENDPOINTS = (
    "drivers",
    "laps",
    "intervals",
    "position",
    "pit",
    "stints",
    "race_control",
    "weather",
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
    "qualifying": frozenset({"pit", "race_control"}),
    "sprint_qualifying": frozenset({"pit", "race_control"}),
    "sprint": frozenset({"pit"}),
    "race": frozenset(),
}
ALLOW_EMPTY = frozenset({"intervals", "pit", "race_control"})
REQUIRED_COLUMNS = {
    "drivers": {"session_key", "driver_number", "name_acronym"},
    "laps": {"session_key", "driver_number", "lap_number", "date_start"},
    "intervals": {"session_key", "driver_number", "date", "gap_to_leader", "interval"},
    "position": {"session_key", "driver_number", "date", "position"},
    "pit": {"session_key", "driver_number", "date", "lap_number"},
    "stints": {"session_key", "driver_number", "stint_number", "lap_start", "compound"},
    "race_control": {"session_key", "date", "message"},
    "weather": {"session_key", "date"},
}
KEY_COLUMNS = {
    "drivers": ("session_key", "driver_number"),
    "laps": ("session_key", "driver_number", "lap_number"),
    "intervals": ("session_key", "driver_number", "date"),
    "position": ("session_key", "driver_number", "date"),
    "pit": ("session_key", "driver_number", "date"),
    "stints": ("session_key", "driver_number", "stint_number"),
    "race_control": ("session_key", "date", "message"),
    "weather": ("session_key", "date"),
}
DATETIME_COLUMNS = {
    "drivers": (),
    "laps": ("date_start",),
    "intervals": ("date",),
    "position": ("date",),
    "pit": ("date",),
    "stints": (),
    "race_control": ("date",),
    "weather": ("date",),
}
NUMERIC_COLUMNS = {
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
}
REQUIRED_NON_NULL = {
    "drivers": ("session_key", "driver_number", "name_acronym"),
    "laps": ("session_key", "driver_number", "lap_number"),
    "intervals": ("session_key", "driver_number", "date"),
    "position": ("session_key", "driver_number", "date", "position"),
    "pit": ("session_key", "driver_number", "date", "lap_number"),
    "stints": ("session_key", "driver_number", "stint_number", "lap_start"),
    "race_control": ("session_key", "date", "message"),
    "weather": ("session_key", "date"),
}


class OpenF1WeekendError(RuntimeError):
    pass


class JsonClient(Protocol):
    def get_json(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        ...


class OpenF1WeekendClient:
    def __init__(self) -> None:
        try:
            import truststore
        except ImportError as exc:
            raise OpenF1WeekendError(
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
        self._last_request_at = 0.0

    def get_json(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        elapsed = time.monotonic() - self._last_request_at
        if elapsed < MIN_REQUEST_INTERVAL_SECONDS:
            time.sleep(MIN_REQUEST_INTERVAL_SECONDS - elapsed)
        try:
            response = self.session.get(
                f"{BASE_URL}/{endpoint}", params=params, timeout=REQUEST_TIMEOUT_SECONDS
            )
            self._last_request_at = time.monotonic()
            response.raise_for_status()
            payload = response.json()
        except requests.exceptions.Timeout as exc:
            raise OpenF1WeekendError(f"OpenF1 endpoint '{endpoint}' timed out.") from exc
        except requests.exceptions.JSONDecodeError as exc:
            raise OpenF1WeekendError(f"OpenF1 endpoint '{endpoint}' returned invalid JSON.") from exc
        except requests.exceptions.RequestException as exc:
            raise OpenF1WeekendError(f"OpenF1 endpoint '{endpoint}' failed: {exc}") from exc
        if not isinstance(payload, list) or not all(isinstance(row, dict) for row in payload):
            raise OpenF1WeekendError(f"OpenF1 endpoint '{endpoint}' returned an invalid payload.")
        return payload


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


def plan_weekend_sessions(sessions: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        skipped = frozenset(ENDPOINTS).difference(required | optional)
        plans.append(
            {
                **session,
                "source_session_key": int(session_key),
                "normalized_session_type": normalized_type,
                "required_endpoints": sorted(required),
                "optional_endpoints": sorted(optional),
                "skipped_endpoints": sorted({"location", *skipped}),
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


def _parquet_safe(endpoint: str, frame: pd.DataFrame) -> pd.DataFrame:
    safe = frame.copy()
    if endpoint == "intervals":
        for column in ("gap_to_leader", "interval"):
            if column in safe.columns:
                safe[column] = safe[column].map(
                    lambda value: None if pd.isna(value) else str(value)
                )
    return safe


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
    latest = raw_dir / f"openf1_{session_key}_{endpoint}.parquet"
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
    payload = client.get_json(endpoint, {"session_key": session_key})
    frame = _parquet_safe(
        endpoint,
        pd.DataFrame(payload, columns=sorted(REQUIRED_COLUMNS[endpoint]))
        if not payload
        else pd.DataFrame(payload),
    )
    _validate_source_frame(endpoint, frame, session_key)
    snapshot = _snapshot_path(raw_dir, session_key, endpoint, frame)
    atomic_parquet(frame, raw_dir / f"openf1_{session_key}_{endpoint}.parquet")
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
            frame, result = _load_endpoint(
                client,
                endpoint,
                session_key,
                refresh=refresh,
                raw_dir=raw_dir,
            )
            frames[endpoint] = frame
            endpoint_results[endpoint] = result
        except (OpenF1WeekendError, OSError, ValueError, TypeError) as exc:
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
        raw_path = Path(result["raw_path"])
        if not raw_path.is_absolute():
            raw_path = PROJECT_ROOT / raw_path
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
    refresh: bool = False,
    client: JsonClient | None = None,
    raw_dir: Path = RAW_DATA_DIR,
    curated_dir: Path = CURATED_DATA_DIR,
) -> tuple[dict[str, Any], Path]:
    plans = plan_weekend_sessions(sessions)
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


