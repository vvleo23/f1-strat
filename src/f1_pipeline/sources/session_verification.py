"""Verify the historical Hungarian Grand Prix 2026 source inputs."""

from __future__ import annotations

import argparse
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol

import pandas as pd
import fastf1

from f1_pipeline.data_validation import DataValidationError, validate_frame
from f1_pipeline.replay.circle_of_doom import (
    CircleOfDoomError,
    OpenF1Client,
    cache_path,
    location_driver_cache_path,
    make_parquet_safe,
)
from f1_pipeline.settings import ARTIFACTS_DIR, CACHE_DIR, PROJECT_ROOT, RAW_DATA_DIR

TARGET_SEASON = 2026
TARGET_COUNTRY = "Hungary"
TARGET_LOCATION = "Hungaroring"
TARGET_START = pd.Timestamp("2026-07-24T00:00:00Z")
TARGET_END = pd.Timestamp("2026-07-26T23:59:59Z")
TARGET_SESSION_NAME = "Race"
MEETINGS_PATH = RAW_DATA_DIR / "openf1_2026_meetings.parquet"
SESSIONS_PATH = RAW_DATA_DIR / "openf1_2026_sessions.parquet"


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)

SESSION_ENDPOINTS = (
    "sessions",
    "drivers",
    "laps",
    "intervals",
    "position",
    "pit",
    "stints",
    "race_control",
    "weather",
)
REQUIRED_ENDPOINTS = frozenset(
    {"sessions", "drivers", "laps", "intervals", "position", "location"}
)
ENDPOINT_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "sessions": frozenset({"session_key", "meeting_key", "session_name", "date_start"}),
    "drivers": frozenset({"session_key", "driver_number", "name_acronym"}),
    "laps": frozenset({"session_key", "driver_number", "lap_number", "date_start", "lap_duration"}),
    "intervals": frozenset({"session_key", "driver_number", "date", "gap_to_leader", "interval"}),
    "position": frozenset({"session_key", "driver_number", "date", "position"}),
    "location": frozenset({"session_key", "driver_number", "date", "x", "y", "z"}),
    "pit": frozenset({"session_key", "driver_number", "date", "lap_number"}),
    "stints": frozenset({"session_key", "driver_number", "stint_number", "lap_start", "lap_end", "compound"}),
    "race_control": frozenset({"session_key", "date", "message"}),
    "weather": frozenset({"session_key", "date"}),
}
ENDPOINT_KEY_COLUMNS: dict[str, tuple[str, ...]] = {
    "sessions": ("session_key",),
    "drivers": ("session_key", "driver_number"),
    "laps": ("session_key", "driver_number", "lap_number"),
    "intervals": ("session_key", "driver_number", "date"),
    "position": ("session_key", "driver_number", "date"),
    "location": ("session_key", "driver_number", "date"),
    "pit": ("session_key", "driver_number", "lap_number"),
    "stints": ("session_key", "driver_number", "stint_number"),
    "race_control": ("session_key", "date", "message"),
    "weather": ("session_key", "date"),
}
ENDPOINT_DATETIME_COLUMNS: dict[str, tuple[str, ...]] = {
    "sessions": ("date_start",),
    "drivers": (),
    "laps": ("date_start",),
    "intervals": ("date",),
    "position": ("date",),
    "location": ("date",),
    "pit": ("date",),
    "stints": (),
    "race_control": ("date",),
    "weather": ("date",),
}
ENDPOINT_REQUIRED_NON_NULL: dict[str, tuple[str, ...]] = {
    "sessions": ("session_key", "meeting_key", "session_name", "date_start"),
    "drivers": ("session_key", "driver_number", "name_acronym"),
    "laps": ("session_key", "driver_number", "lap_number", "date_start"),
    "intervals": ("session_key", "driver_number", "date"),
    "position": ("session_key", "driver_number", "date", "position"),
    "location": ("session_key", "driver_number", "date", "x", "y", "z"),
    "pit": ("session_key", "driver_number", "date", "lap_number"),
    "stints": ("session_key", "driver_number", "stint_number", "lap_start", "lap_end"),
    "race_control": ("session_key", "date", "message"),
    "weather": ("session_key", "date"),
}
ENDPOINT_NUMERIC_COLUMNS: dict[str, tuple[str, ...]] = {
    "sessions": ("session_key", "meeting_key"),
    "drivers": ("session_key", "driver_number"),
    "laps": ("session_key", "driver_number", "lap_number", "lap_duration"),
    "intervals": ("session_key", "driver_number"),
    "position": ("session_key", "driver_number", "position"),
    "location": ("session_key", "driver_number", "x", "y", "z"),
    "pit": ("session_key", "driver_number", "lap_number", "pit_duration"),
    "stints": ("session_key", "driver_number", "stint_number", "lap_start", "lap_end", "tyre_age_at_start"),
    "race_control": ("session_key", "driver_number", "lap_number"),
    "weather": ("session_key",),
}
CALENDAR_REQUIRED_COLUMNS: dict[str, frozenset[str]] = {
    "meetings": frozenset({"meeting_key", "meeting_name", "date_start", "date_end"}),
    "sessions": frozenset({"session_key", "meeting_key", "session_name", "date_start", "date_end"}),
}
UNIMPLEMENTED_SOURCES = (
    "open_meteo",
    "rainviewer",
    "wikidata",
)
SOURCE_METADATA: dict[str, dict[str, Any]] = {
    "openf1": {
        "display_name": "OpenF1",
        "role": "primary_replay_source",
        "used_for": [
            "session identity",
            "replay timeline",
            "positions and gaps",
            "pit stops and stints",
            "race control",
        ],
        "boundary": "Primary timestamped replay evidence; not an official steward-decision feed.",
        "implemented": True,
    },
    "fastf1": {
        "display_name": "FastF1",
        "role": "historical_cross_check",
        "used_for": ["laps", "tyres", "session weather", "telemetry"],
        "boundary": "Session-centric historical analysis and cross-check; not required for replay state.",
        "implemented": True,
    },
    "open_meteo": {
        "display_name": "Open-Meteo",
        "role": "weather_forecast",
        "used_for": ["point-in-time weather forecasts"],
        "boundary": "Forecast model data, not measured trackside observations.",
        "implemented": False,
    },
    "rainviewer": {
        "display_name": "RainViewer",
        "role": "rain_radar_nowcast",
        "used_for": ["radar precipitation", "short-term rain nowcast"],
        "boundary": "Radar and nowcast precipitation, not temperature or tyre data.",
        "implemented": False,
    },
    "wikidata": {
        "display_name": "Wikidata",
        "role": "circuit_coordinates",
        "used_for": ["validated circuit reference point", "weather request coordinates"],
        "boundary": "Geographic reference point, not track geometry or replay measurements.",
        "implemented": False,
    },
}


class SessionVerificationError(RuntimeError):
    """Describe a missing or ambiguous historical session."""


class JsonClient(Protocol):
    def get_json(self, endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Return a JSON list from an external source."""


def _validate_endpoint_frame(
    endpoint: str, frame: pd.DataFrame, session_key: int
) -> None:
    validate_frame(
        frame,
        name=f"OpenF1 {endpoint}",
        required_columns=ENDPOINT_REQUIRED_COLUMNS[endpoint],
        key_columns=ENDPOINT_KEY_COLUMNS[endpoint],
        datetime_columns=ENDPOINT_DATETIME_COLUMNS[endpoint],
        numeric_columns=ENDPOINT_NUMERIC_COLUMNS[endpoint],
        required_non_null=ENDPOINT_REQUIRED_NON_NULL[endpoint],
        expected_session_key=session_key,
    )


def _validate_calendar_frame(endpoint: str, frame: pd.DataFrame) -> None:
    key = "meeting_key" if endpoint == "meetings" else "session_key"
    validate_frame(
        frame,
        name=f"OpenF1 {endpoint} calendar",
        required_columns=CALENDAR_REQUIRED_COLUMNS[endpoint],
        key_columns=(key,),
        datetime_columns=("date_start", "date_end"),
        numeric_columns=(key, "meeting_key") if endpoint == "sessions" else (key,),
        required_non_null=(key, "date_start"),
    )


def _text(values: dict[str, Any], *keys: str) -> str:
    return " ".join(str(values.get(key, "")) for key in keys).casefold()


def _timestamp(value: Any) -> pd.Timestamp | None:
    if value is None or value == "":
        return None
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if isinstance(parsed, pd.Timestamp) and not pd.isna(parsed):
        return parsed
    return None


def select_hungary_meeting(meetings: list[dict[str, Any]]) -> dict[str, Any]:
    candidates = []
    for meeting in meetings:
        searchable = _text(
            meeting,
            "country_name",
            "country",
            "meeting_name",
            "meeting_official_name",
            "location",
            "circuit_short_name",
        )
        if TARGET_COUNTRY.casefold() in searchable or TARGET_LOCATION.casefold() in searchable:
            candidates.append(meeting)

    if len(candidates) != 1:
        raise SessionVerificationError(
            f"Expected one Hungarian Grand Prix meeting, found {len(candidates)}."
        )
    if candidates[0].get("meeting_key") is None:
        raise SessionVerificationError("The Hungarian meeting has no meeting_key.")
    return candidates[0]


def select_hungary_race(
    sessions: list[dict[str, Any]], meeting_key: int,
) -> dict[str, Any]:
    candidates = []
    for session in sessions:
        if session.get("meeting_key") != meeting_key:
            continue
        if str(session.get("session_name", "")).casefold() != TARGET_SESSION_NAME.casefold():
            continue
        if session.get("is_cancelled") is True:
            continue
        start = _timestamp(session.get("date_start"))
        end = _timestamp(session.get("date_end")) or start
        if start is None or end is None or end < TARGET_START or start > TARGET_END:
            continue
        candidates.append(session)

    if len(candidates) != 1:
        raise SessionVerificationError(
            f"Expected one Hungarian race session in the target window, found {len(candidates)}."
        )
    if candidates[0].get("session_key") is None:
        raise SessionVerificationError("The Hungarian race has no session_key.")
    return candidates[0]


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=f".{path.name}.", suffix=".parquet", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
        assert temporary_path is not None
        frame.to_parquet(temporary_path, index=False)
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _atomic_json(payload: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".json",
            delete=False,
        ) as temporary:
            temporary_path = Path(temporary.name)
            json.dump(payload, temporary, indent=2, ensure_ascii=False)
            temporary.write("\n")
        temporary_path.replace(path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def _source_result(source: str, result: dict[str, Any]) -> dict[str, Any]:
    return {**SOURCE_METADATA[source], **result}


def _unimplemented_source_results() -> dict[str, dict[str, Any]]:
    return {
        source: {
            **SOURCE_METADATA[source],
            "status": "not_implemented",
            "details": "No executable adapter is available for this source yet.",
        }
        for source in UNIMPLEMENTED_SOURCES
    }


def _overall_status(endpoint_results: dict[str, dict[str, Any]]) -> str:
    if not endpoint_results:
        return "unavailable"
    required_statuses = [
        result.get("status") for name, result in endpoint_results.items() if name in REQUIRED_ENDPOINTS
    ]
    usable_statuses = {"available", "stale"}
    if all(status in usable_statuses for status in required_statuses):
        if all(result.get("status") == "available" for result in endpoint_results.values()):
            return "available"
        if all(result.get("status") in usable_statuses for result in endpoint_results.values()):
            return "stale"
        return "partial"
    if any(status in usable_statuses for status in required_statuses):
        return "partial"
    return "unavailable"


def _fastf1_event_schedule() -> pd.DataFrame:
    schedule = fastf1.get_event_schedule(TARGET_SEASON, include_testing=False)
    if schedule.empty:
        raise SessionVerificationError("FastF1 returned no event schedule.")
    location = schedule.get("Location", pd.Series(dtype=str)).astype(str).str.casefold()
    event_name = schedule.get("EventName", pd.Series(dtype=str)).astype(str).str.casefold()
    candidates = schedule[
        location.str.contains(TARGET_LOCATION.casefold(), na=False)
        | event_name.str.contains("hungarian", na=False)
    ]
    if len(candidates) != 1:
        raise SessionVerificationError(
            f"Expected one FastF1 Hungaroring event, found {len(candidates)}."
        )
    return candidates.iloc[0]


def verify_fastf1() -> dict[str, Any]:
    result: dict[str, Any] = {"status": "unavailable", "session": "Race"}
    try:
        fastf1.Cache.enable_cache(str(CACHE_DIR))
        event = _fastf1_event_schedule()
        round_number = int(event["RoundNumber"])
        session = fastf1.get_session(TARGET_SEASON, round_number, "R")
        session.load(telemetry=False, weather=True)
        laps = session.laps
        required_columns = {"Driver", "LapNumber", "LapTime"}
        missing_columns = sorted(required_columns.difference(laps.columns))
        if missing_columns:
            raise SessionVerificationError(
                "FastF1 lap columns are missing: " + ", ".join(missing_columns)
            )
        if laps.empty:
            raise SessionVerificationError("FastF1 returned no lap data for the race.")

        lap_path = RAW_DATA_DIR / "fastf1_hungary_2026_race_laps.parquet"
        _atomic_parquet(laps, lap_path)
        weather = session.weather_data
        weather_path = RAW_DATA_DIR / "fastf1_hungary_2026_race_weather.parquet"
        weather_status = "available" if weather is not None and not weather.empty else "partial"
        if weather is not None and not weather.empty:
            _atomic_parquet(weather, weather_path)
        result.update(
            {
                "status": weather_status,
                "event": str(event.get("EventName", TARGET_LOCATION)),
                "round_number": round_number,
                "lap_rows": len(laps),
                "lap_path": _relative_path(lap_path),
                "weather_rows": 0 if weather is None else len(weather),
            }
        )
        if weather_status == "partial":
            result["details"] = "Lap data is available, but FastF1 weather data is empty."
    except Exception as exc:
        result["details"] = str(exc)
    return result


def _endpoint_result(
    status: str,
    *,
    row_count: int = 0,
    path: str | None = None,
    details: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"status": status, "row_count": row_count}
    if path is not None:
        result["path"] = _relative_path(Path(path))
    if details is not None:
        result["details"] = details
    return result


def _cached_frame(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        return None
    frame = pd.read_parquet(path)
    return frame if not frame.empty else None


def _calendar_records(path: Path) -> list[dict[str, Any]] | None:
    frame = _cached_frame(path)
    return None if frame is None else frame.to_dict(orient="records")


def _load_calendar(
    client: JsonClient,
    endpoint: str,
    path: Path,
    params: dict[str, Any],
    *,
    refresh: bool,
) -> tuple[list[dict[str, Any]], str]:
    if not refresh:
        try:
            records = _calendar_records(path)
            if records is not None:
                _validate_calendar_frame(endpoint, pd.DataFrame(records))
        except (OSError, ValueError):
            records = None
        if records is not None:
            return records, "stale"
    payload = client.get_json(endpoint, params)
    frame = pd.DataFrame(payload)
    _validate_calendar_frame(endpoint, frame)
    _atomic_parquet(frame, path)
    return payload, "available"


def _load_endpoint(
    client: JsonClient,
    endpoint: str,
    session_key: int,
    *,
    refresh: bool,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    path = cache_path(session_key, endpoint)
    if not refresh:
        try:
            frame = _cached_frame(path)
            if frame is not None:
                _validate_endpoint_frame(endpoint, frame, session_key)
        except (OSError, ValueError):
            frame = None
        if frame is not None:
            return frame, _endpoint_result(
                "stale", row_count=len(frame), path=_relative_path(path), details="Loaded from a previous snapshot."
            )
    try:
        payload = client.get_json(endpoint, {"session_key": session_key})
        frame = make_parquet_safe(endpoint, pd.DataFrame(payload))
        _validate_endpoint_frame(endpoint, frame, session_key)
        _atomic_parquet(frame, path)
        return frame, _endpoint_result(
            "available", row_count=len(frame), path=_relative_path(path)
        )
    except (CircleOfDoomError, OSError, ValueError, TypeError) as exc:
        return None, _endpoint_result("unavailable", details=str(exc))


def _load_locations(
    client: JsonClient,
    session_key: int,
    drivers: pd.DataFrame | None,
    *,
    refresh: bool,
) -> tuple[pd.DataFrame | None, dict[str, Any]]:
    combined_path = cache_path(session_key, "location")
    if not refresh:
        try:
            combined = _cached_frame(combined_path)
            if combined is not None:
                _validate_endpoint_frame("location", combined, session_key)
        except (OSError, ValueError):
            combined = None
        if combined is not None:
            return combined, _endpoint_result(
                "stale",
                row_count=len(combined),
                path=_relative_path(combined_path),
                details="Loaded from a previous snapshot.",
            )
    if drivers is None or "driver_number" not in drivers.columns:
        return None, _endpoint_result("unavailable", details="Driver data is unavailable.")

    frames: list[pd.DataFrame] = []
    failures: list[str] = []
    numeric_driver_numbers = pd.Series(
        pd.to_numeric(drivers["driver_number"], errors="coerce")
    ).dropna()
    driver_numbers = sorted(int(value) for value in numeric_driver_numbers.unique())
    for driver_number in driver_numbers:
        try:
            payload = client.get_json(
                "location",
                {"session_key": session_key, "driver_number": driver_number},
            )
            frame = make_parquet_safe("location", pd.DataFrame(payload))
            _validate_endpoint_frame("location", frame, session_key)
            if frame.empty:
                failures.append(str(driver_number))
                continue
            _atomic_parquet(frame, location_driver_cache_path(session_key, driver_number))
            frames.append(frame)
        except (CircleOfDoomError, OSError, ValueError, TypeError) as exc:
            failures.append(f"{driver_number}: {exc}")

    if not frames:
        details = "No location data was available."
        if failures:
            details += " " + "; ".join(failures)
        return None, _endpoint_result("unavailable", details=details)

    combined = pd.concat(frames, ignore_index=True)
    _validate_endpoint_frame("location", combined, session_key)
    _atomic_parquet(combined, cache_path(session_key, "location"))
    status = "partial" if failures else "available"
    details = "; ".join(failures) if failures else None
    return combined, _endpoint_result(
        status, row_count=len(combined), path=_relative_path(cache_path(session_key, "location")), details=details
    )


def verify_openf1(
    client: JsonClient | None = None,
    *,
    refresh: bool = False,
) -> dict[str, Any]:
    checked_at = datetime.now(timezone.utc).isoformat()
    result: dict[str, Any] = {
        "status": "unavailable",
        "checked_at": checked_at,
        "meeting": None,
        "session": None,
        "endpoints": {},
    }
    try:
        api = client or OpenF1Client()
        meetings, calendar_status = _load_calendar(
            api, "meetings", MEETINGS_PATH, {"year": TARGET_SEASON}, refresh=refresh
        )
        meeting = select_hungary_meeting(meetings)
        meeting_key = int(meeting["meeting_key"])
        sessions, session_calendar_status = _load_calendar(
            api,
            "sessions",
            SESSIONS_PATH,
            {"year": TARGET_SEASON},
            refresh=refresh,
        )
        session = select_hungary_race(sessions, meeting_key)
        session_key = int(session["session_key"])
        result["meeting"] = meeting
        result["session"] = session
        result["session_key"] = session_key
        result["calendar_status"] = (
            "stale" if "stale" in {calendar_status, session_calendar_status} else "available"
        )

        frames: dict[str, pd.DataFrame | None] = {}
        for endpoint in SESSION_ENDPOINTS:
            frames[endpoint], result["endpoints"][endpoint] = _load_endpoint(
                api, endpoint, session_key, refresh=refresh
            )
        frames["location"], result["endpoints"]["location"] = _load_locations(
            api, session_key, frames.get("drivers"), refresh=refresh
        )
        result["status"] = _overall_status(result["endpoints"])
        if result["calendar_status"] == "stale" and result["status"] == "available":
            result["status"] = "stale"
    except (CircleOfDoomError, SessionVerificationError, OSError, ValueError, TypeError) as exc:
        result["details"] = str(exc)
    return result


def build_report(openf1_result: dict[str, Any]) -> dict[str, Any]:
    sources = {
        "openf1": _source_result("openf1", openf1_result),
        "fastf1": _source_result("fastf1", verify_fastf1()),
    }
    sources.update(_unimplemented_source_results())
    return {
        "schema_version": 1,
        "example": {
            "name": "Hungarian Grand Prix 2026",
            "season": TARGET_SEASON,
            "location": TARGET_LOCATION,
            "start": TARGET_START.isoformat(),
            "end": TARGET_END.isoformat(),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
    }


def run(output: Path | None = None, *, strict: bool = False, refresh: bool = False) -> int:
    report = build_report(verify_openf1(refresh=refresh))
    report_path = output or ARTIFACTS_DIR / "source_verification" / "hungary_2026.json"
    _atomic_json(report, report_path)
    openf1_status = report["sources"]["openf1"]["status"]
    print(f"Verification report: {report_path}")
    print(f"OpenF1 status: {openf1_status}")
    return 0 if not strict or openf1_status == "available" else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Verify the Hungarian Grand Prix 2026 source inputs."
    )
    parser.add_argument("--output", type=Path, help="Verification report JSON path.")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return a non-zero exit code unless all required OpenF1 data is available.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore existing snapshots and fetch current source data.",
    )
    args = parser.parse_args(argv)
    return run(args.output, strict=args.strict, refresh=args.refresh)


if __name__ == "__main__":
    raise SystemExit(main())

