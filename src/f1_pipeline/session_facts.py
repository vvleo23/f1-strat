from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.data_validation import DataValidationError, validate_frame

SCHEMA_VERSION = 1
FACT_NAMES = {
    "drivers": "session_entry",
    "laps": "lap",
    "intervals": "interval",
    "position": "position",
    "pit": "pit_stop",
    "stints": "stint",
    "race_control": "race_control_event",
    "weather": "weather_observation",
    "starting_grid": "starting_grid",
    "session_result": "session_result",
    "championship_drivers": "driver_championship_standing",
    "championship_teams": "team_championship_standing",
}
LAPPED_PATTERN = re.compile(r"^\+(\d+)\s+LAPS?$", re.IGNORECASE)


class SessionFactError(RuntimeError):
    pass


def _column(frame: pd.DataFrame, name: str) -> pd.Series:
    if name in frame.columns:
        return frame[name]
    return pd.Series(pd.NA, index=frame.index, dtype="object")


def _numeric(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_numeric(_column(frame, name), errors="coerce")


def _timestamp(frame: pd.DataFrame, name: str) -> pd.Series:
    return pd.to_datetime(_column(frame, name), utc=True, errors="coerce", format="mixed")


def _text(frame: pd.DataFrame, name: str) -> pd.Series:
    values = _column(frame, name)
    return values.map(lambda value: None if pd.isna(value) else str(value))


def _raw_text(frame: pd.DataFrame, name: str) -> pd.Series:
    def serialize(value: Any) -> str | None:
        if value is None:
            return None
        if not pd.api.types.is_scalar(value):
            if hasattr(value, "tolist"):
                value = value.tolist()
            return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
        if pd.isna(value):
            return None
        return str(value)

    return _column(frame, name).map(serialize)


def _boolean(frame: pd.DataFrame, name: str) -> pd.Series:
    def parse(value: Any) -> bool | None:
        if value is None or pd.isna(value):
            return None
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().casefold()
        if normalized in {"true", "1"}:
            return True
        if normalized in {"false", "0"}:
            return False
        return None

    return _column(frame, name).map(parse).astype("boolean")


def _team_id(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    slug = re.sub(r"[^a-z0-9]+", "-", str(value).strip().casefold()).strip("-")
    return f"openf1:team:{slug}" if slug else None


def _identifier(kind: str, values: list[Any]) -> str:
    normalized = "|".join("" if pd.isna(value) else str(value) for value in values)
    digest = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:20]
    return f"openf1:{kind}:{digest}"


def _base(
    frame: pd.DataFrame,
    *,
    fact_name: str,
    session_key: int,
    retrieved_at: pd.Timestamp,
    ingested_at: pd.Timestamp,
    raw_path: Path,
    raw_sha256: str,
) -> pd.DataFrame:
    result = pd.DataFrame(index=frame.index)
    result["session_id"] = f"openf1:session:{session_key}"
    result["source_session_key"] = session_key
    drivers = _numeric(frame, "driver_number").astype("Int64")
    result["driver_number"] = drivers
    result["session_entry_id"] = drivers.map(
        lambda value: (
            None
            if pd.isna(value)
            else f"openf1:session_entry:{session_key}:{int(value)}"
        )
    )
    result["ingested_at"] = ingested_at
    result["source_system"] = "openf1"
    result["raw_path"] = str(raw_path)
    result["raw_sha256"] = raw_sha256
    result["schema_version"] = SCHEMA_VERSION
    result["fact_type"] = fact_name
    result["retrieved_at"] = retrieved_at
    return result


def _lap_times(laps: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    starts = _timestamp(laps, "date_start")
    durations = _numeric(laps, "lap_duration")
    ends = starts + pd.to_timedelta(durations, unit="s")
    ends = ends.where(starts.notna() & durations.notna() & durations.ge(0))
    return starts, ends


def _finalize(
    result: pd.DataFrame,
    *,
    fact_name: str,
    source_keys: list[str],
    event_time: pd.Series,
    availability_basis: str = "simulated_event_time",
) -> pd.DataFrame:
    result["event_time"] = event_time
    result["available_at"] = event_time
    if availability_basis == "simulated_event_time":
        result["availability_basis"] = event_time.map(
            lambda value: "unavailable" if pd.isna(value) else availability_basis
        )
    else:
        result["availability_basis"] = availability_basis
    result["source_record_key"] = [
        "|".join("" if pd.isna(row.get(key)) else str(row.get(key)) for key in source_keys)
        for row in result.to_dict(orient="records")
    ]
    result.insert(
        0,
        "fact_id",
        [
            _identifier(fact_name, [row.get(key) for key in source_keys])
            for row in result.to_dict(orient="records")
        ],
    )
    try:
        validate_frame(
            result,
            name=f"Silver {fact_name}",
            required_columns={
                "fact_id",
                "session_id",
                "source_session_key",
                "event_time",
                "available_at",
                "ingested_at",
                "source_system",
                "source_record_key",
                "raw_path",
                "raw_sha256",
                "schema_version",
            },
            key_columns=("fact_id",),
            datetime_columns=("event_time", "available_at", "ingested_at", "retrieved_at"),
            required_non_null=(
                "fact_id",
                "session_id",
                "source_session_key",
                "source_system",
                "source_record_key",
                "raw_path",
                "raw_sha256",
                "ingested_at",
            ),
            allow_empty=True,
        )
    except DataValidationError as exc:
        raise SessionFactError(str(exc)) from exc
    return result.reset_index(drop=True)


def normalize_session_fact(
    endpoint: str,
    frame: pd.DataFrame,
    *,
    session_key: int,
    retrieved_at: pd.Timestamp,
    ingested_at: pd.Timestamp,
    raw_path: Path,
    raw_sha256: str,
    laps: pd.DataFrame | None = None,
) -> tuple[str, pd.DataFrame]:
    fact_name = FACT_NAMES.get(endpoint)
    if fact_name is None:
        raise SessionFactError(f"Endpoint '{endpoint}' has no Silver fact mapping.")
    result = _base(
        frame,
        fact_name=fact_name,
        session_key=session_key,
        retrieved_at=retrieved_at,
        ingested_at=ingested_at,
        raw_path=raw_path,
        raw_sha256=raw_sha256,
    )

    if endpoint == "drivers":
        result["name_acronym"] = _text(frame, "name_acronym")
        result["full_name"] = _text(frame, "full_name")
        result["team_name"] = _text(frame, "team_name")
        result["team_colour"] = _text(frame, "team_colour")
        result["event_time"] = retrieved_at
        result["available_at"] = retrieved_at
        result["availability_basis"] = "observed_retrieval"
        result["source_record_key"] = result["session_entry_id"]
        result.insert(0, "fact_id", result["session_entry_id"])
        return fact_name, _finalize(
            result.drop(columns=["fact_id", "event_time", "available_at", "availability_basis", "source_record_key"]),
            fact_name=fact_name,
            source_keys=["session_entry_id"],
            event_time=pd.Series(retrieved_at, index=result.index),
            availability_basis="observed_retrieval",
        )

    if endpoint == "starting_grid":
        result["meeting_id"] = _numeric(frame, "meeting_key").astype("Int64").map(
            lambda value: None if pd.isna(value) else f"openf1:meeting:{int(value)}"
        )
        result["grid_position"] = _numeric(frame, "position").astype("Int64")
        result["qualifying_lap_seconds"] = _numeric(frame, "lap_duration")
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number"],
            event_time=pd.Series(retrieved_at, index=result.index),
            availability_basis="observed_retrieval",
        )

    if endpoint == "session_result":
        result["meeting_id"] = _numeric(frame, "meeting_key").astype("Int64").map(
            lambda value: None if pd.isna(value) else f"openf1:meeting:{int(value)}"
        )
        result["position"] = _numeric(frame, "position").astype("Int64")
        result["number_of_laps"] = _numeric(frame, "number_of_laps").astype("Int64")
        result["points"] = _numeric(frame, "points")
        result["dnf"] = _boolean(frame, "dnf")
        result["dns"] = _boolean(frame, "dns")
        result["dsq"] = _boolean(frame, "dsq")
        result["duration_raw"] = _raw_text(frame, "duration")
        result["duration_seconds"] = _numeric(frame, "duration")
        gap_raw = _text(frame, "gap_to_leader")
        result["gap_to_leader_raw"] = gap_raw
        result["gap_to_leader_seconds"] = pd.to_numeric(gap_raw, errors="coerce")
        result["laps_behind"] = gap_raw.map(
            lambda value: (
                int(match.group(1))
                if isinstance(value, str) and (match := LAPPED_PATTERN.fullmatch(value.strip()))
                else None
            )
        ).astype("Int64")
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number"],
            event_time=pd.Series(retrieved_at, index=result.index),
            availability_basis="observed_retrieval",
        )

    if endpoint == "championship_drivers":
        result["meeting_id"] = _numeric(frame, "meeting_key").astype("Int64").map(
            lambda value: None if pd.isna(value) else f"openf1:meeting:{int(value)}"
        )
        result["position_start"] = _numeric(frame, "position_start").astype("Int64")
        result["position_current"] = _numeric(frame, "position_current").astype("Int64")
        result["points_start"] = _numeric(frame, "points_start")
        result["points_current"] = _numeric(frame, "points_current")
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number"],
            event_time=pd.Series(retrieved_at, index=result.index),
            availability_basis="observed_retrieval",
        )

    if endpoint == "championship_teams":
        result["meeting_id"] = _numeric(frame, "meeting_key").astype("Int64").map(
            lambda value: None if pd.isna(value) else f"openf1:meeting:{int(value)}"
        )
        result["team_name"] = _text(frame, "team_name")
        result["team_id"] = result["team_name"].map(_team_id)
        result["position_start"] = _numeric(frame, "position_start").astype("Int64")
        result["position_current"] = _numeric(frame, "position_current").astype("Int64")
        result["points_start"] = _numeric(frame, "points_start")
        result["points_current"] = _numeric(frame, "points_current")
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "team_name"],
            event_time=pd.Series(retrieved_at, index=result.index),
            availability_basis="observed_retrieval",
        )

    if endpoint == "laps":
        starts, ends = _lap_times(frame)
        result["lap_number"] = _numeric(frame, "lap_number").astype("Int64")
        result["lap_started_at"] = starts
        result["lap_duration_seconds"] = _numeric(frame, "lap_duration")
        result["sector_1_seconds"] = _numeric(frame, "duration_sector_1")
        result["sector_2_seconds"] = _numeric(frame, "duration_sector_2")
        result["sector_3_seconds"] = _numeric(frame, "duration_sector_3")
        result["is_pit_out_lap"] = _column(frame, "is_pit_out_lap").astype("boolean")
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number", "lap_number"],
            event_time=ends,
        )

    if endpoint == "intervals":
        gap_raw = _text(frame, "gap_to_leader")
        interval_raw = _text(frame, "interval")
        result["gap_to_leader_raw"] = gap_raw
        result["interval_raw"] = interval_raw
        result["gap_to_leader_seconds"] = pd.to_numeric(gap_raw, errors="coerce")
        result["interval_seconds"] = pd.to_numeric(interval_raw, errors="coerce")
        result["laps_behind"] = gap_raw.map(
            lambda value: (
                int(match.group(1))
                if isinstance(value, str) and (match := LAPPED_PATTERN.fullmatch(value.strip()))
                else None
            )
        ).astype("Int64")
        event_time = _timestamp(frame, "date")
        result["source_event_time"] = event_time
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number", "source_event_time"],
            event_time=event_time,
        )

    if endpoint == "position":
        result["position"] = _numeric(frame, "position").astype("Int64")
        event_time = _timestamp(frame, "date")
        result["source_event_time"] = event_time
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number", "source_event_time"],
            event_time=event_time,
        )

    if endpoint == "pit":
        event_time = _timestamp(frame, "date")
        result["source_event_time"] = event_time
        result["lap_number"] = _numeric(frame, "lap_number").astype("Int64")
        result["pit_duration_seconds"] = _numeric(frame, "pit_duration")
        result["stop_duration_seconds"] = _numeric(frame, "stop_duration")
        result["lane_duration_seconds"] = _numeric(frame, "lane_duration")
        ordering = result.assign(_index=range(len(result))).sort_values(
            ["driver_number", "source_event_time", "_index"], na_position="last"
        )
        sequences = ordering.groupby("driver_number", dropna=False).cumcount() + 1
        result["stop_sequence"] = sequences.reindex(ordering.index).sort_index().astype("Int64")
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number", "source_event_time", "stop_sequence"],
            event_time=event_time,
        )

    if endpoint == "stints":
        result["stint_number"] = _numeric(frame, "stint_number").astype("Int64")
        result["lap_start"] = _numeric(frame, "lap_start").astype("Int64")
        result["lap_end"] = _numeric(frame, "lap_end").astype("Int64")
        result["compound"] = _text(frame, "compound")
        result["tyre_age_at_start"] = _numeric(frame, "tyre_age_at_start")
        event_time = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
        started_at = pd.Series(pd.NaT, index=frame.index, dtype="datetime64[ns, UTC]")
        if laps is not None and not laps.empty:
            lap_starts, lap_ends = _lap_times(laps)
            lookup = pd.DataFrame(
                {
                    "driver_number": _numeric(laps, "driver_number").astype("Int64"),
                    "lap_number": _numeric(laps, "lap_number").astype("Int64"),
                    "lap_started_at": lap_starts,
                    "lap_ended_at": lap_ends,
                }
            )
            start_lookup = lookup.set_index(["driver_number", "lap_number"])["lap_started_at"]
            end_lookup = lookup.set_index(["driver_number", "lap_number"])["lap_ended_at"]
            keys_start = list(zip(result["driver_number"], result["lap_start"]))
            keys_end = list(zip(result["driver_number"], result["lap_end"]))
            started_at = pd.Series([start_lookup.get(key, pd.NaT) for key in keys_start], index=frame.index)
            event_time = pd.Series([end_lookup.get(key, pd.NaT) for key in keys_end], index=frame.index)
        result["stint_started_at"] = pd.to_datetime(started_at, utc=True)
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=["session_id", "driver_number", "stint_number"],
            event_time=pd.to_datetime(event_time, utc=True),
        )

    if endpoint == "race_control":
        event_time = _timestamp(frame, "date")
        result["source_event_time"] = event_time
        for column in ("category", "flag", "scope", "message"):
            result[column] = _text(frame, column)
        result["sector"] = _numeric(frame, "sector").astype("Int64")
        result["lap_number"] = _numeric(frame, "lap_number").astype("Int64")
        return fact_name, _finalize(
            result,
            fact_name=fact_name,
            source_keys=[
                "session_id",
                "source_event_time",
                "category",
                "flag",
                "message",
                "driver_number",
            ],
            event_time=event_time,
        )

    event_time = _timestamp(frame, "date")
    result["source_event_time"] = event_time
    for column in (
        "air_temperature",
        "track_temperature",
        "humidity",
        "pressure",
        "rainfall",
        "wind_speed",
        "wind_direction",
    ):
        result[column] = _numeric(frame, column)
    return fact_name, _finalize(
        result,
        fact_name=fact_name,
        source_keys=["session_id", "source_event_time"],
        event_time=event_time,
    )
