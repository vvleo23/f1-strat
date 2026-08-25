"""Load a Formula 1 season's master data from OpenF1."""

from __future__ import annotations

import argparse
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import pandas as pd

from f1_pipeline.data_validation import DataValidationError, validate_frame
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.settings import CURATED_DATA_DIR, RAW_DATA_DIR
from f1_pipeline.sources.openf1 import (
    OpenF1Client as SharedOpenF1Client,
    season_cache_path,
)

SCHEMA_VERSION = 1
LEGACY_DIMENSION_SEASON = 2026

TABLE_COLUMNS: dict[str, list[str]] = {
    "country": [
        "country_id",
        "source_country_key",
        "country_code",
        "country_name",
        "country_flag_url",
        "source_system",
        "source_record_key",
        "ingested_at",
        "schema_version",
    ],
    "circuit": [
        "circuit_id",
        "source_circuit_key",
        "circuit_name",
        "location",
        "circuit_type",
        "country_id",
        "circuit_info_url",
        "circuit_image_url",
        "wikidata_entity_id",
        "reference_latitude",
        "reference_longitude",
        "reference_crs",
        "coordinate_revision",
        "coordinate_retrieved_at",
        "coordinate_verification_status",
        "coordinate_raw_path",
        "coordinate_sha256",
        "source_system",
        "source_record_key",
        "ingested_at",
        "schema_version",
    ],
    "circuit_geometry": [
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
    ],
    "driver": [
        "driver_id",
        "season_id",
        "driver_number",
        "name_acronym",
        "full_name",
        "first_name",
        "last_name",
        "broadcast_name",
        "country_code",
        "team_id",
        "team_name",
        "team_colour",
        "headshot_url",
        "roster_session_key",
        "source_system",
        "source_record_key",
        "ingested_at",
        "schema_version",
    ],
    "team": [
        "team_id",
        "team_name",
        "team_colour",
        "season_id",
        "source_system",
        "source_record_key",
        "ingested_at",
        "schema_version",
    ],
    "season": [
        "season_id",
        "year",
        "source_system",
        "source_record_key",
        "ingested_at",
        "schema_version",
    ],
    "meeting": [
        "meeting_id",
        "season_id",
        "meeting_name",
        "meeting_official_name",
        "location",
        "country_id",
        "circuit_id",
        "planned_start_utc",
        "planned_end_utc",
        "status",
        "is_cancelled",
        "superseded_by_meeting_id",
        "source_system",
        "source_record_key",
        "ingested_at",
        "schema_version",
    ],
    "session": [
        "session_id",
        "meeting_id",
        "season_id",
        "session_type",
        "session_name",
        "scheduled_start_utc",
        "scheduled_end_utc",
        "status",
        "is_cancelled",
        "superseded_by_session_id",
        "source_system",
        "source_record_key",
        "ingested_at",
        "schema_version",
    ],
}

DATETIME_COLUMNS = {
    "ingested_at",
    "planned_start_utc",
    "planned_end_utc",
    "scheduled_start_utc",
    "scheduled_end_utc",
    "valid_from_utc",
    "valid_to_utc",
    "coordinate_retrieved_at",
}

MASTER_KEY_COLUMNS = {
    "country": ("country_id",),
    "circuit": ("circuit_id",),
    "circuit_geometry": ("geometry_id",),
    "driver": ("driver_id",),
    "team": ("team_id",),
    "season": ("season_id",),
    "meeting": ("meeting_id",),
    "session": ("session_id",),
}

MASTER_REQUIRED_NON_NULL = {
    "country": ("country_id", "country_name", "source_system", "source_record_key", "ingested_at"),
    "circuit": ("circuit_id", "circuit_name", "country_id", "source_system", "source_record_key", "ingested_at"),
    "circuit_geometry": ("geometry_id", "circuit_id", "geometry_type", "source_system", "source_record_key",
                         "ingested_at"),
    "driver": ("driver_id", "season_id", "driver_number", "name_acronym", "full_name", "team_id", "source_system",
               "source_record_key", "ingested_at"),
    "team": ("team_id", "team_name", "season_id", "source_system", "source_record_key", "ingested_at"),
    "season": ("season_id", "year", "source_system", "source_record_key", "ingested_at"),
    "meeting": ("meeting_id", "season_id", "meeting_name", "country_id", "circuit_id", "status", "source_system",
                "source_record_key", "ingested_at"),
    "session": ("session_id", "meeting_id", "season_id", "session_type", "session_name", "status", "source_system",
                "source_record_key", "ingested_at"),
}

MASTER_ALLOWED_STATUSES = {"scheduled", "completed", "cancelled", "postponed"}


class MasterDataError(RuntimeError):
    """Describe an error while loading or validating master data."""


class OpenF1Client(SharedOpenF1Client):
    def __init__(self) -> None:
        super().__init__(error_type=MasterDataError, user_agent="f1-strat/1.0")


def _slug(value: Any) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "-", str(value).strip().lower()).strip("-")
    if not normalized:
        raise MasterDataError(f"Cannot create a stable identifier from {value!r}.")
    return normalized


def _source_id(kind: str, value: Any) -> str:
    if value is None or pd.isna(value):
        raise MasterDataError(f"Missing OpenF1 key for {kind}.")
    return f"openf1:{kind}:{value}"


def _timestamp(value: Any):
    if value is None or pd.isna(value) or value == "":
        return pd.NaT
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if parsed is pd.NaT:
        raise MasterDataError(f"Invalid UTC timestamp: {value!r}")
    if not isinstance(parsed, pd.Timestamp):
        raise MasterDataError(f"Invalid UTC timestamp: {value!r}")
    if pd.isna(parsed):
        raise MasterDataError(f"Invalid UTC timestamp: {value!r}")
    return parsed


def _timestamp_sort_key(value: Any) -> int:
    parsed = _timestamp(value)
    return -1 if pd.isna(parsed) else int(parsed.value)


def _bool(value: Any) -> bool:
    return bool(value) if value is not None and not pd.isna(value) else False


def _int_or_none(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _meeting_id(row: dict[str, Any]) -> str:
    return _source_id("meeting", row.get("meeting_key"))


def _session_id(row: dict[str, Any]) -> str:
    return _source_id("session", row.get("session_key"))


def _country_id(row: dict[str, Any]) -> str:
    return _source_id("country", row.get("country_key") or row.get("country_code"))


def _circuit_id(row: dict[str, Any]) -> str:
    return _source_id("circuit", row.get("circuit_key") or row.get("circuit_short_name"))


def _meeting_replacements(meetings: list[dict[str, Any]]) -> dict[int, int]:
    active = [row for row in meetings if not _bool(row.get("is_cancelled"))]
    replacements: dict[int, int] = {}
    for cancelled in meetings:
        if not _bool(cancelled.get("is_cancelled")):
            continue
        candidates = [
            row
            for row in active
            if row.get("meeting_name") == cancelled.get("meeting_name")
               and _timestamp_sort_key(row.get("date_start"))
               > _timestamp_sort_key(cancelled.get("date_start"))
        ]
        candidates.sort(key=lambda row: _timestamp_sort_key(row.get("date_start")))
        if candidates:
            replacements[int(cancelled["meeting_key"])] = int(candidates[0]["meeting_key"])
    return replacements


def _status(row: dict[str, Any], as_of: pd.Timestamp, postponed: bool = False) -> str:
    if _bool(row.get("is_cancelled")):
        return "postponed" if postponed else "cancelled"
    end = _timestamp(row.get("date_end"))
    return "completed" if not pd.isna(end) and end <= as_of else "scheduled"


def choose_roster_session(
        sessions: list[dict[str, Any]], as_of: pd.Timestamp
) -> dict[str, Any]:
    races = [
        row
        for row in sessions
        if str(row.get("session_type", "")).lower() == "race"
           and not _bool(row.get("is_cancelled"))
    ]
    completed = [
        row for row in races if
        not pd.isna(_timestamp(row.get("date_end"))) and _timestamp(row.get("date_end")) <= as_of
    ]
    candidates = completed or races
    if not candidates:
        candidates = [row for row in sessions if not _bool(row.get("is_cancelled"))]
    if not candidates:
        raise MasterDataError("No non-cancelled session is available for the driver roster.")
    if completed:
        return max(candidates, key=lambda row: _timestamp_sort_key(row.get("date_end")))
    return min(candidates, key=lambda row: _timestamp_sort_key(row.get("date_start")))


def _raw_snapshot_path(path: Path, ingested_at: pd.Timestamp) -> Path:
    snapshot_dir = RAW_DATA_DIR / "snapshots"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    timestamp = ingested_at.strftime("%Y%m%dT%H%M%S%fZ")
    return snapshot_dir / f"{path.stem}_{timestamp}{path.suffix}"


def _load_or_fetch(
        client: OpenF1Client,
        endpoint: str,
        params: dict[str, Any],
        path: Path,
        refresh: bool,
        ingested_at: pd.Timestamp,
) -> tuple[list[dict[str, Any]], bool, Path]:
    if path.exists() and not refresh:
        records = pd.read_parquet(path).to_dict(orient="records")
        return cast(list[dict[str, Any]], records), False, path
    payload = client.get_json(endpoint, params)
    snapshot_path = _raw_snapshot_path(path, ingested_at)
    atomic_parquet(pd.DataFrame(payload), snapshot_path)
    return payload, True, snapshot_path


def _commit_raw_snapshot(snapshot_path: Path, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, suffix=path.suffix, delete=False) as temporary:
        temporary_path = Path(temporary.name)
    try:
        shutil.copy2(snapshot_path, temporary_path)
        pd.read_parquet(temporary_path)
        temporary_path.replace(path)
    finally:
        temporary_path.unlink(missing_ok=True)


def _table_frame(name: str, records: list[dict[str, Any]]) -> pd.DataFrame:
    columns = TABLE_COLUMNS[name]
    frame = pd.DataFrame(records)
    for column in columns:
        if column not in frame.columns:
            frame[column] = None
    frame = frame[columns]
    for column in DATETIME_COLUMNS.intersection(columns):
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    return frame


def master_table_path(name: str, season: int) -> Path:
    return CURATED_DATA_DIR / "dimensions" / f"season={season}" / f"{name}.parquet"


def _write_table(name: str, records: list[dict[str, Any]], season: int) -> Path:
    target = master_table_path(name, season)
    frame = _table_frame(name, records)
    atomic_parquet(frame, target)
    if season == LEGACY_DIMENSION_SEASON:
        atomic_parquet(frame, CURATED_DATA_DIR / "dimensions" / f"{name}.parquet")
    return target


def _validate_master_frames(frames: dict[str, pd.DataFrame]) -> None:
    missing_tables = sorted(set(TABLE_COLUMNS).difference(frames))
    if missing_tables:
        raise MasterDataError("Missing master-data tables: " + ", ".join(missing_tables))

    for name, frame in frames.items():
        try:
            validate_frame(
                frame,
                name=name,
                required_columns=TABLE_COLUMNS[name],
                key_columns=MASTER_KEY_COLUMNS[name],
                datetime_columns=DATETIME_COLUMNS.intersection(TABLE_COLUMNS[name]),
                numeric_columns=("year", "schema_version") if name == "season" else ("schema_version",),
                required_non_null=MASTER_REQUIRED_NON_NULL[name],
                allow_empty=name == "circuit_geometry",
            )
        except DataValidationError as exc:
            raise MasterDataError(str(exc)) from exc

    statuses = set()
    for name in ("meeting", "session"):
        statuses.update(frames[name]["status"].dropna().astype(str))
    invalid_statuses = sorted(statuses.difference(MASTER_ALLOWED_STATUSES))
    if invalid_statuses:
        raise MasterDataError("Invalid master-data status values: " + ", ".join(invalid_statuses))

    meeting_ids = set(frames["meeting"]["meeting_id"])
    season_ids = set(frames["season"]["season_id"])
    circuit_ids = set(frames["circuit"]["circuit_id"])
    country_ids = set(frames["country"]["country_id"])
    if not frames["meeting"]["season_id"].isin(season_ids).all():
        raise MasterDataError("A meeting references an unknown season.")
    if not frames["session"]["meeting_id"].isin(meeting_ids).all():
        raise MasterDataError("A session references an unknown meeting.")
    if not frames["meeting"]["circuit_id"].isin(circuit_ids).all():
        raise MasterDataError("A meeting references an unknown circuit.")
    if not frames["meeting"]["country_id"].isin(country_ids).all():
        raise MasterDataError("A meeting references an unknown country.")
    if not frames["driver"]["season_id"].isin(season_ids).all():
        raise MasterDataError("A driver references an unknown season.")
    if not frames["team"]["season_id"].isin(season_ids).all():
        raise MasterDataError("A driver or team references an unknown season.")
    if not frames["driver"]["team_id"].isin(set(frames["team"]["team_id"])).all():
        raise MasterDataError("A driver references an unknown team.")
    if not frames["circuit_geometry"]["circuit_id"].isin(circuit_ids).all():
        raise MasterDataError("Circuit geometry references an unknown circuit.")


def validate_persisted_tables(table_paths: dict[str, Path]) -> dict[str, int]:
    """Validate curated Parquet schemas and return verified row counts."""
    frames: dict[str, pd.DataFrame] = {}
    for name, path in table_paths.items():
        if name not in TABLE_COLUMNS:
            raise MasterDataError(f"Unknown master-data table: {name}.")
        try:
            frames[name] = pd.read_parquet(path)
        except (OSError, ValueError) as exc:
            raise MasterDataError(f"Could not read curated table '{name}': {exc}") from exc
    _validate_master_frames(frames)
    return {name: len(frame) for name, frame in frames.items()}


def _load_existing_geometry(season: int) -> list[dict[str, Any]]:
    path = master_table_path("circuit_geometry", season)
    if not path.exists() and season == LEGACY_DIMENSION_SEASON:
        path = CURATED_DATA_DIR / "dimensions" / "circuit_geometry.parquet"
    if not path.exists():
        return []
    try:
        frame = pd.read_parquet(path)
    except (OSError, ValueError) as exc:
        raise MasterDataError(f"Could not read existing circuit geometry: {exc}") from exc
    if frame.empty:
        return []
    return cast(list[dict[str, Any]], frame.to_dict(orient="records"))


def build_tables(
        meetings: list[dict[str, Any]],
        sessions: list[dict[str, Any]],
        drivers: list[dict[str, Any]],
        season: int,
        ingested_at: pd.Timestamp,
        roster_session_key: int,
        circuit_geometry: list[dict[str, Any]] | None = None,
) -> dict[str, list[dict[str, Any]]]:
    season_id = f"f1:season:{season}"
    replacements = _meeting_replacements(meetings)
    replacement_ids = {
        old: _source_id("meeting", new) for old, new in replacements.items()
    }
    countries: dict[str, dict[str, Any]] = {}
    circuits: dict[str, dict[str, Any]] = {}
    for row in meetings:
        country_id = _country_id(row)
        circuit_id = _circuit_id(row)
        countries.setdefault(
            country_id,
            {
                "country_id": country_id,
                "source_country_key": _int_or_none(row.get("country_key")),
                "country_code": row.get("country_code"),
                "country_name": row.get("country_name"),
                "country_flag_url": row.get("country_flag"),
                "source_system": "openf1",
                "source_record_key": f"country:{row.get('country_key')}",
                "ingested_at": ingested_at,
                "schema_version": SCHEMA_VERSION,
            },
        )
        circuits.setdefault(
            circuit_id,
            {
                "circuit_id": circuit_id,
                "source_circuit_key": _int_or_none(row.get("circuit_key")),
                "circuit_name": row.get("circuit_short_name"),
                "location": row.get("location"),
                "circuit_type": row.get("circuit_type"),
                "country_id": country_id,
                "circuit_info_url": row.get("circuit_info_url"),
                "circuit_image_url": row.get("circuit_image"),
                "source_system": "openf1",
                "source_record_key": f"meeting:{row.get('meeting_key')}",
                "ingested_at": ingested_at,
                "schema_version": SCHEMA_VERSION,
            },
        )

    meeting_records: list[dict[str, Any]] = []
    for row in meetings:
        source_key = int(row["meeting_key"])
        replacement_id = replacement_ids.get(source_key)
        meeting_records.append(
            {
                "meeting_id": _meeting_id(row),
                "season_id": season_id,
                "meeting_name": row.get("meeting_name"),
                "meeting_official_name": row.get("meeting_official_name"),
                "location": row.get("location"),
                "country_id": _country_id(row),
                "circuit_id": _circuit_id(row),
                "planned_start_utc": _timestamp(row.get("date_start")),
                "planned_end_utc": _timestamp(row.get("date_end")),
                "status": _status(row, ingested_at, postponed=replacement_id is not None),
                "is_cancelled": _bool(row.get("is_cancelled")),
                "superseded_by_meeting_id": replacement_id,
                "source_system": "openf1",
                "source_record_key": f"meeting:{source_key}",
                "ingested_at": ingested_at,
                "schema_version": SCHEMA_VERSION,
            }
        )

    meeting_status = {
        int(row["meeting_key"]): record["status"]
        for row, record in zip(meetings, meeting_records)
    }
    session_replacements: dict[int, int] = {}
    for row in sessions:
        old_meeting = int(row["meeting_key"])
        replacement_meeting = replacements.get(old_meeting)
        if replacement_meeting is None:
            continue
        candidates = [
            candidate
            for candidate in sessions
            if candidate.get("meeting_key") is not None
               and int(candidate["meeting_key"]) == replacement_meeting
               and candidate.get("session_type") == row.get("session_type")
               and candidate.get("session_name") == row.get("session_name")
        ]
        if candidates:
            session_replacements[int(row["session_key"])] = int(candidates[0]["session_key"])

    session_records: list[dict[str, Any]] = []
    for row in sessions:
        meeting_key = int(row["meeting_key"])
        status = meeting_status[meeting_key]
        if status not in {"postponed", "cancelled"}:
            status = _status(row, ingested_at)
        session_records.append(
            {
                "session_id": _session_id(row),
                "meeting_id": _meeting_id(row),
                "season_id": season_id,
                "session_type": row.get("session_type"),
                "session_name": row.get("session_name"),
                "scheduled_start_utc": _timestamp(row.get("date_start")),
                "scheduled_end_utc": _timestamp(row.get("date_end")),
                "status": status,
                "is_cancelled": _bool(row.get("is_cancelled")),
                "superseded_by_session_id": (
                    _source_id("session", session_replacements[int(row["session_key"])])
                    if int(row["session_key"]) in session_replacements
                    else None
                ),
                "source_system": "openf1",
                "source_record_key": f"session:{row.get('session_key')}",
                "ingested_at": ingested_at,
                "schema_version": SCHEMA_VERSION,
            }
        )

    driver_records: dict[str, dict[str, Any]] = {}
    team_records: dict[str, dict[str, Any]] = {}
    for row in drivers:
        team_name = row.get("team_name")
        team_id = f"openf1:team:{_slug(team_name)}" if team_name else None
        if team_id:
            team_records.setdefault(
                team_id,
                {
                    "team_id": team_id,
                    "team_name": team_name,
                    "team_colour": row.get("team_colour"),
                    "season_id": season_id,
                    "source_system": "openf1",
                    "source_record_key": f"session:{roster_session_key}:team:{_slug(team_name)}",
                    "ingested_at": ingested_at,
                    "schema_version": SCHEMA_VERSION,
                },
            )
        acronym = row.get("name_acronym") or _slug(row.get("full_name"))
        driver_id = f"openf1:driver:{_slug(acronym)}"
        driver_records[driver_id] = {
            "driver_id": driver_id,
            "season_id": season_id,
            "driver_number": _int_or_none(row.get("driver_number")),
            "name_acronym": row.get("name_acronym"),
            "full_name": row.get("full_name"),
            "first_name": row.get("first_name"),
            "last_name": row.get("last_name"),
            "broadcast_name": row.get("broadcast_name"),
            "country_code": row.get("country_code"),
            "team_id": team_id,
            "team_name": team_name,
            "team_colour": row.get("team_colour"),
            "headshot_url": row.get("headshot_url"),
            "roster_session_key": roster_session_key,
            "source_system": "openf1",
            "source_record_key": f"session:{roster_session_key}:driver:{row.get('driver_number')}",
            "ingested_at": ingested_at,
            "schema_version": SCHEMA_VERSION,
        }

    tables = {
        "country": sorted(countries.values(), key=lambda row: row["country_id"]),
        "circuit": sorted(circuits.values(), key=lambda row: row["circuit_id"]),
        "circuit_geometry": circuit_geometry or [],
        "driver": sorted(driver_records.values(), key=lambda row: row["driver_id"]),
        "team": sorted(team_records.values(), key=lambda row: row["team_id"]),
        "season": [
            {
                "season_id": season_id,
                "year": season,
                "source_system": "openf1",
                "source_record_key": f"year:{season}",
                "ingested_at": ingested_at,
                "schema_version": SCHEMA_VERSION,
            }
        ],
        "meeting": sorted(meeting_records, key=lambda row: row["meeting_id"]),
        "session": sorted(session_records, key=lambda row: row["session_id"]),
    }
    _validate_master_frames(
        {name: _table_frame(name, records) for name, records in tables.items()}
    )
    return tables


def load_master_data(
        season: int = 2026,
        refresh: bool = False,
        client: OpenF1Client | None = None,
) -> dict[str, Path]:
    """Load and persist the requested season's master data."""
    ingested_at = pd.Timestamp(datetime.now(timezone.utc))
    api = client or OpenF1Client()
    meeting_path = season_cache_path(season, "meetings")
    session_path = season_cache_path(season, "sessions")
    meetings, meeting_fetched, meeting_input_path = _load_or_fetch(
        api, "meetings", {"year": season}, meeting_path, refresh, ingested_at
    )
    sessions, session_fetched, session_input_path = _load_or_fetch(
        api, "sessions", {"year": season}, session_path, refresh, ingested_at
    )
    if not meetings or not sessions:
        raise MasterDataError(f"No OpenF1 calendar data found for season {season}.")
    roster_session = choose_roster_session(sessions, ingested_at)
    roster_key = int(roster_session["session_key"])
    driver_path = season_cache_path(season, "drivers", str(roster_key))
    drivers, drivers_fetched, driver_input_path = _load_or_fetch(
        api,
        "drivers",
        {"session_key": roster_key},
        driver_path,
        refresh,
        ingested_at,
    )
    tables = build_tables(
        meetings,
        sessions,
        drivers,
        season,
        ingested_at,
        roster_key,
        circuit_geometry=_load_existing_geometry(season),
    )
    for fetched, snapshot_path, latest_path in (
            (meeting_fetched, meeting_input_path, meeting_path),
            (session_fetched, session_input_path, session_path),
            (drivers_fetched, driver_input_path, driver_path),
    ):
        if fetched:
            _commit_raw_snapshot(snapshot_path, latest_path)
    output_paths = {
        name: _write_table(name, records, season) for name, records in tables.items()
    }
    verified_row_counts = validate_persisted_tables(output_paths)

    raw_inputs = [
        ("meetings", {"year": season}, meeting_input_path, meeting_fetched),
        ("sessions", {"year": season}, session_input_path, session_fetched),
        ("drivers", {"session_key": roster_key}, driver_input_path, drivers_fetched),
    ]
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "season": season,
        "source_system": "openf1",
        "ingested_at": ingested_at.isoformat(),
        "roster_session_key": roster_key,
        "raw_inputs": [
            {
                "endpoint": endpoint,
                "parameters": params,
                "path": str(path.relative_to(RAW_DATA_DIR.parent.parent)),
                "fetched": fetched,
                "row_count": len(pd.read_parquet(path)),
                "sha256": sha256(path),
            }
            for endpoint, params, path, fetched in raw_inputs
        ],
        "tables": {
            name: {
                "path": str(path.relative_to(CURATED_DATA_DIR.parent.parent)),
                "row_count": verified_row_counts[name],
                "sha256": sha256(path),
            }
            for name, path in output_paths.items()
        },
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
    manifest_dir = CURATED_DATA_DIR / "manifests"
    manifest_path = manifest_dir / f"master_data_{season}.json"
    atomic_json(manifest, manifest_path)
    output_paths["manifest"] = manifest_path
    return output_paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Load Formula 1 master data from OpenF1.")
    parser.add_argument("--season", type=int, default=2026)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    try:
        outputs = load_master_data(args.season, refresh=args.refresh)
    except (MasterDataError, OSError, ValueError) as exc:
        print(f"Master data could not be loaded: {exc}")
        return 1
    for name, path in outputs.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
