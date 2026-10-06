from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.pit_loss import PitLoss, PitLossConfigError, load_pit_loss
from f1_pipeline.settings import ARTIFACTS_DIR, CURATED_DATA_DIR, PROJECT_ROOT
from f1_pipeline.temporal import TemporalCutError, cut_facts, decision_timestamp

FEATURE_VERSION = "race_strategy_features_v1"
CALCULATION_VERSION = "race_strategy_rules_v4"

EVALUATION_COLUMNS = (
    "actual_pit_lap",
    "actual_compound",
    "entry_time",
    "revealed_at",
    "recommendation_id",
    "recommended_pit_lap",
    "recommended_compound",
    "lap_error",
    "within_one_lap",
    "within_two_laps",
    "compound_match",
)
READABLE_STATUSES = frozenset({"available", "partial", "stale"})
DRY_COMPOUNDS = ("SOFT", "MEDIUM", "HARD")
WET_COMPOUNDS = ("INTERMEDIATE", "WET")
DEFAULT_MAXIMUM_AGE = {
    "SOFT": 25,
    "MEDIUM": 35,
    "HARD": 50,
    "INTERMEDIATE": 30,
    "WET": 30,
}
RACE_DISTANCE_PATH = PROJECT_ROOT / "config" / "race_distance_laps.json"


class RaceStrategyError(RuntimeError):
    pass


@dataclass(frozen=True)
class PaceProfile:
    driver_pace: dict[int, float]
    compound_delta: dict[str, float]
    degradation: dict[str, float]
    maximum_age: dict[str, int]
    compounds: tuple[str, ...]


@dataclass(frozen=True)
class StrategyResult:
    recommendations: pd.DataFrame
    evaluations: pd.DataFrame
    status: str
    diagnostics: dict[str, Any]


def _project_path(value: str | Path) -> Path:
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _number(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _timestamp(value: Any) -> pd.Timestamp | None:
    result = pd.to_datetime(value, utc=True, errors="coerce")
    return result if isinstance(result, pd.Timestamp) and pd.notna(result) else None


def _read_parquet(path_value: str, expected_hash: str | None = None) -> pd.DataFrame:
    path = _project_path(path_value)
    if not path.is_file():
        raise RaceStrategyError(f"Strategy input is missing: {path}")
    if expected_hash and sha256(path) != expected_hash:
        raise RaceStrategyError(f"Strategy input failed its hash check: {path}")
    try:
        return pd.read_parquet(path)
    except (OSError, ValueError) as exc:
        raise RaceStrategyError(f"Could not read strategy input: {exc}") from exc


def _frames_from_manifest(manifest: dict[str, Any]) -> dict[str, pd.DataFrame]:
    frames: dict[str, pd.DataFrame] = {}
    endpoints = manifest.get("endpoints", {})
    for endpoint in (
        "drivers",
        "laps",
        "intervals",
        "position",
        "pit",
        "stints",
        "race_control",
        "weather",
    ):
        result = endpoints.get(endpoint)
        if (
            not isinstance(result, dict)
            or result.get("status") not in READABLE_STATUSES
            or not result.get("silver_path")
        ):
            frames[endpoint] = pd.DataFrame()
            continue
        frames[endpoint] = _read_parquet(
            str(result["silver_path"]),
            str(result["silver_sha256"]) if result.get("silver_sha256") else None,
        )
    return frames


def _safe_cut(frame: pd.DataFrame, decision_time: pd.Timestamp) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    try:
        return cut_facts(frame, decision_time=decision_time)
    except TemporalCutError as exc:
        raise RaceStrategyError(str(exc)) from exc


def _released_stints(frame: pd.DataFrame, decision_time: pd.Timestamp) -> pd.DataFrame:
    if frame.empty or "stint_started_at" not in frame.columns:
        return frame.iloc[0:0].copy()
    result = frame.copy()
    started = pd.to_datetime(result["stint_started_at"], utc=True, errors="coerce")
    result = result[started.notna() & started.le(decision_time)].copy()
    result["stint_started_at"] = started.loc[result.index]
    if "lap_end" in result.columns:
        result["lap_end"] = pd.NA
    return result.reset_index(drop=True)


def _load_race_distance(season: int, circuit_key: str) -> tuple[int | None, str]:
    try:
        payload = json.loads(RACE_DISTANCE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RaceStrategyError(f"Could not read race-distance configuration: {exc}") from exc
    if payload.get("schema_version") != 1 or int(payload.get("season", 0)) != season:
        return None, "No matching static race-distance configuration."
    for row in payload.get("circuits", []):
        if isinstance(row, dict) and str(row.get("key")) == circuit_key:
            laps = row.get("laps")
            if isinstance(laps, int) and laps > 0:
                return laps, str(payload.get("basis") or "Static pre-race metadata.")
    return None, "No matching static race-distance configuration."


def _lap_compounds(laps: pd.DataFrame, stints: pd.DataFrame) -> pd.DataFrame:
    if laps.empty:
        return laps.copy()
    result = laps.copy()
    result["compound"] = pd.NA
    result["tyre_age"] = math.nan
    if stints.empty:
        return result
    for row in stints.to_dict(orient="records"):
        driver = _number(row.get("driver_number"))
        start = _number(row.get("lap_start"))
        end = _number(row.get("lap_end"))
        if driver is None or start is None:
            continue
        end = end if end is not None else math.inf
        mask = (
            pd.to_numeric(result.get("driver_number"), errors="coerce").eq(int(driver))
            & pd.to_numeric(result.get("lap_number"), errors="coerce").between(start, end)
        )
        result.loc[mask, "compound"] = str(row.get("compound") or "").upper() or pd.NA
        age = _number(row.get("tyre_age_at_start")) or 0.0
        result.loc[mask, "tyre_age"] = (
            pd.to_numeric(result.loc[mask, "lap_number"], errors="coerce") - start + age
        )
    return result


def build_pace_profile(session_frames: list[dict[str, pd.DataFrame]]) -> PaceProfile:
    samples: list[pd.DataFrame] = []
    stint_rows: list[pd.DataFrame] = []
    for frames in session_frames:
        laps = _lap_compounds(frames.get("laps", pd.DataFrame()), frames.get("stints", pd.DataFrame()))
        if laps.empty or "lap_duration_seconds" not in laps.columns:
            continue
        laps = laps.copy()
        laps["lap_duration_seconds"] = pd.to_numeric(laps["lap_duration_seconds"], errors="coerce")
        valid = laps["lap_duration_seconds"].between(45, 300)
        if "is_pit_out_lap" in laps.columns:
            valid &= ~laps["is_pit_out_lap"].fillna(False).astype(bool)
        laps = laps[valid & laps["compound"].notna()].copy()
        if not laps.empty:
            samples.append(laps)
        stints = frames.get("stints", pd.DataFrame())
        if not stints.empty:
            stint_rows.append(stints)
    if not samples:
        return PaceProfile({}, {}, {}, {}, DRY_COMPOUNDS)
    data = pd.concat(samples, ignore_index=True)
    data["driver_number"] = pd.to_numeric(data["driver_number"], errors="coerce")
    data["compound"] = data["compound"].astype(str).str.upper()
    driver_pace = {
        int(driver): float(group["lap_duration_seconds"].median())
        for driver, group in data.dropna(subset=["driver_number"]).groupby("driver_number")
    }
    normalized = data.copy()
    normalized["driver_median"] = normalized["driver_number"].map(driver_pace)
    normalized["relative"] = normalized["lap_duration_seconds"] - normalized["driver_median"]
    compound_raw = normalized.groupby("compound")["relative"].median().to_dict()
    fastest = min((float(value) for value in compound_raw.values()), default=0.0)
    compound_delta = {
        str(compound): max(0.0, min(5.0, float(value) - fastest))
        for compound, value in compound_raw.items()
    }
    degradation: dict[str, float] = {}
    for compound, group in normalized.dropna(subset=["tyre_age"]).groupby("compound"):
        if len(group) < 4 or group["tyre_age"].nunique() < 3:
            degradation[str(compound)] = 0.05
            continue
        slope = float(np.polyfit(group["tyre_age"].astype(float), group["relative"].astype(float), 1)[0])
        degradation[str(compound)] = max(0.0, min(0.35, slope))
    maximum_age: dict[str, int] = {}
    if stint_rows:
        stints = pd.concat(stint_rows, ignore_index=True)
        starts = pd.to_numeric(stints.get("lap_start"), errors="coerce")
        ends = pd.to_numeric(stints.get("lap_end"), errors="coerce")
        stints = stints.assign(length=ends - starts + 1)
        stints["compound"] = stints.get("compound", pd.Series(index=stints.index, dtype="object")).astype(str).str.upper()
        maximum_age = {
            str(compound): max(
                DEFAULT_MAXIMUM_AGE.get(str(compound), 24),
                int(round(float(group["length"].dropna().quantile(0.9)))),
            )
            for compound, group in stints.groupby("compound")
            if group["length"].notna().any()
        }
    compounds = tuple(
        compound for compound in (*DRY_COMPOUNDS, *WET_COMPOUNDS) if compound in compound_delta
    ) or DRY_COMPOUNDS
    return PaceProfile(driver_pace, compound_delta, degradation, maximum_age, compounds)


def _status_at(race_control: pd.DataFrame, decision_time: pd.Timestamp) -> tuple[str, str]:
    status = "GREEN"
    latest_message = ""
    selected = _safe_cut(race_control, decision_time)
    if selected.empty or "event_time" not in selected.columns:
        return status, latest_message
    for row in selected.sort_values("event_time").to_dict(orient="records"):
        message = str(row.get("message") or "").upper()
        flag = str(row.get("flag") or "").upper()
        category = str(row.get("category") or "").upper()
        latest_message = message or flag or category
        if flag == "RED" or "RED FLAG" in message or "SESSION STOPPED" in message:
            status = "RED"
        elif "VIRTUAL SAFETY CAR DEPLOYED" in message:
            status = "VSC"
        elif "SAFETY CAR DEPLOYED" in message:
            status = "SC"
        elif "VIRTUAL SAFETY CAR ENDING" in message or "SAFETY CAR IN THIS LAP" in message:
            status = "GREEN"
        elif flag == "GREEN" or "SESSION STARTED" in message:
            status = "GREEN"
    return status, latest_message


def _latest_by_driver(frame: pd.DataFrame, decision_time: pd.Timestamp) -> pd.DataFrame:
    selected = _safe_cut(frame, decision_time)
    if selected.empty or "driver_number" not in selected.columns:
        return selected.iloc[0:0]
    return selected.sort_values("event_time").drop_duplicates("driver_number", keep="last")


def _forecast_rain_lap(
    forecast: pd.DataFrame,
    decision_time: pd.Timestamp,
    current_lap: int,
    lap_seconds: float,
    planned_laps: int,
) -> int | None:
    if forecast.empty or "valid_time" not in forecast.columns or "available_at" not in forecast.columns:
        return None
    data = forecast.copy()
    data["available_at"] = pd.to_datetime(data["available_at"], utc=True, errors="coerce")
    data["valid_time"] = pd.to_datetime(data["valid_time"], utc=True, errors="coerce")
    data = data[
        data["available_at"].notna()
        & data["available_at"].le(decision_time)
        & data["valid_time"].notna()
        & data["valid_time"].ge(decision_time)
    ]
    if data.empty:
        return None
    rain = pd.to_numeric(data.get("rain"), errors="coerce").fillna(0)
    precipitation = pd.to_numeric(data.get("precipitation"), errors="coerce").fillna(0)
    wet = data[rain.gt(0.1) | precipitation.gt(0.1)].sort_values("valid_time")
    if wet.empty:
        return None
    delta = max(0.0, (wet.iloc[0]["valid_time"] - decision_time).total_seconds())
    return min(planned_laps, current_lap + int(math.ceil(delta / max(1.0, lap_seconds))))


def _current_weather(weather: pd.DataFrame, decision_time: pd.Timestamp) -> bool:
    selected = _safe_cut(weather, decision_time)
    if selected.empty:
        return False
    latest = selected.sort_values("event_time").iloc[-1]
    rainfall = _number(latest.get("rainfall"))
    return rainfall is not None and rainfall > 0


def _current_compounds(
    stints: pd.DataFrame,
    decision_time: pd.Timestamp,
    current_laps: dict[int, int],
) -> tuple[dict[int, str], dict[int, int], dict[int, tuple[str, ...]]]:
    released = _released_stints(stints, decision_time)
    compounds: dict[int, str] = {}
    ages: dict[int, int] = {}
    used: dict[int, list[str]] = {}
    if released.empty:
        return compounds, ages, {}
    released = released.sort_values(["driver_number", "lap_start", "stint_number"])
    for row in released.to_dict(orient="records"):
        driver_value = _number(row.get("driver_number"))
        start_value = _number(row.get("lap_start"))
        if driver_value is None or start_value is None:
            continue
        driver = int(driver_value)
        compound = str(row.get("compound") or "").upper()
        if not compound:
            continue
        used.setdefault(driver, []).append(compound)
        if int(start_value) <= current_laps.get(driver, 1):
            compounds[driver] = compound
            start_age = int(_number(row.get("tyre_age_at_start")) or 0)
            ages[driver] = max(0, current_laps.get(driver, 1) - int(start_value) + start_age)
    return compounds, ages, {driver: tuple(values) for driver, values in used.items()}


def _pace_by_driver(
    laps: pd.DataFrame,
    decision_time: pd.Timestamp,
    profile: PaceProfile,
) -> dict[int, float]:
    selected = _safe_cut(laps, decision_time)
    if selected.empty:
        return dict(profile.driver_pace)
    selected = selected.copy()
    selected["lap_duration_seconds"] = pd.to_numeric(selected.get("lap_duration_seconds"), errors="coerce")
    selected = selected[selected["lap_duration_seconds"].between(45, 300)]
    if "is_pit_out_lap" in selected.columns:
        selected = selected[~selected["is_pit_out_lap"].fillna(False).astype(bool)]
    result = dict(profile.driver_pace)
    for driver, group in selected.sort_values("event_time").groupby("driver_number"):
        recent = group.tail(3)["lap_duration_seconds"].dropna()
        if not recent.empty:
            result[int(driver)] = float(recent.median())
    return result


def _stint_time(
    pace: float,
    compound: str,
    laps: int,
    start_age: int,
    profile: PaceProfile,
) -> float:
    delta = profile.compound_delta.get(compound, 1.5 if compound in WET_COMPOUNDS else 0.8)
    degradation = profile.degradation.get(compound, 0.05)
    return sum(pace + delta + degradation * (start_age + index) for index in range(max(0, laps)))


def _plan_time(
    pace: float,
    plan: list[dict[str, Any]],
    current_lap: int,
    current_age: int,
    profile: PaceProfile,
    pit_loss: PitLoss,
    status: str,
) -> float:
    total = 0.0
    for index, stint in enumerate(plan):
        start = int(stint["start_lap"])
        end = int(stint["end_lap"])
        age = current_age if index == 0 and start == current_lap else 0
        total += _stint_time(pace, str(stint["compound"]), end - start + 1, age, profile)
        if index < len(plan) - 1:
            stop_status = status if int(plan[index + 1]["start_lap"]) <= current_lap + 1 else "GREEN"
            total += pit_loss.for_status(stop_status)
    return total


def _candidate_plans(
    *,
    current_lap: int,
    planned_laps: int,
    current_compound: str | None,
    used_compounds: tuple[str, ...],
    current_age: int,
    allow_immediate_stop: bool,
    profile: PaceProfile,
    rain_lap: int | None,
    currently_wet: bool,
    wet_compound: str,
) -> list[list[dict[str, Any]]]:
    if current_lap > planned_laps:
        return []
    if currently_wet and current_compound not in WET_COMPOUNDS:
        return [[
            {"compound": current_compound or "UNKNOWN", "start_lap": current_lap, "end_lap": current_lap},
            {"compound": wet_compound, "start_lap": current_lap + 1, "end_lap": planned_laps},
        ]]
    if rain_lap is not None and current_compound not in WET_COMPOUNDS:
        start = max(current_lap + 1, rain_lap)
        return [[
            {"compound": current_compound or "MEDIUM", "start_lap": current_lap, "end_lap": start - 1},
            {"compound": wet_compound, "start_lap": start, "end_lap": planned_laps},
        ]]
    dry = tuple(compound for compound in DRY_COMPOUNDS if compound in profile.compounds) or DRY_COMPOUNDS
    plans: list[list[dict[str, Any]]] = []
    if current_compound is None:
        for first in dry:
            for second in dry:
                if first == second:
                    continue
                for stop in range(max(7, current_lap + 6), planned_laps - 4):
                    first_length = stop - current_lap + 1
                    second_length = planned_laps - stop
                    if first_length > profile.maximum_age.get(
                        first, DEFAULT_MAXIMUM_AGE[first]
                    ) or second_length > profile.maximum_age.get(
                        second, DEFAULT_MAXIMUM_AGE[second]
                    ):
                        continue
                    plans.append([
                        {"compound": first, "start_lap": current_lap, "end_lap": stop},
                        {"compound": second, "start_lap": stop + 1, "end_lap": planned_laps},
                    ])
        return plans
    used = set(used_compounds)
    if len(used.intersection(DRY_COMPOUNDS)) >= 2:
        remaining_length = planned_laps - current_lap + 1
        if current_age + remaining_length - 1 <= profile.maximum_age.get(
            current_compound, DEFAULT_MAXIMUM_AGE.get(current_compound, 24)
        ):
            plans.append([{"compound": current_compound, "start_lap": current_lap, "end_lap": planned_laps}])
    minimum_first_stop = current_lap if allow_immediate_stop else current_lap + max(0, 8 - current_age)
    current_limit = profile.maximum_age.get(
        current_compound, DEFAULT_MAXIMUM_AGE.get(current_compound, 24)
    )
    last_first_stop = min(
        planned_laps - 3,
        current_lap + 18,
        current_lap + max(0, current_limit - current_age),
    )
    for second in dry:
        if second == current_compound and len(used.intersection(DRY_COMPOUNDS)) < 2:
            continue
        for stop in range(minimum_first_stop, last_first_stop + 1):
            max_second = profile.maximum_age.get(
                second, DEFAULT_MAXIMUM_AGE[second]
            )
            if planned_laps - stop <= max_second:
                plans.append([
                    {"compound": current_compound, "start_lap": current_lap, "end_lap": stop},
                    {"compound": second, "start_lap": stop + 1, "end_lap": planned_laps},
                ])
            third_start = min(planned_laps - 2, stop + max(8, max_second))
            if third_start > stop + 2:
                for third in dry:
                    if len(used.union({second, third}).intersection(DRY_COMPOUNDS)) < 2:
                        continue
                    if third_start - stop - 1 > max_second:
                        continue
                    if planned_laps - third_start + 1 > profile.maximum_age.get(
                        third, DEFAULT_MAXIMUM_AGE[third]
                    ):
                        continue
                    plans.append([
                        {"compound": current_compound, "start_lap": current_lap, "end_lap": stop},
                        {"compound": second, "start_lap": stop + 1, "end_lap": third_start - 1},
                        {"compound": third, "start_lap": third_start, "end_lap": planned_laps},
                    ])
    return plans


def _projection(
    positions: pd.DataFrame,
    intervals: pd.DataFrame,
    pace: dict[int, float],
    focus_driver: int,
    laps_until_stop: int,
    loss: float,
) -> dict[str, Any]:
    latest_positions = {
        int(row.driver_number): int(row.position)
        for row in positions.dropna(subset=["driver_number", "position"]).itertuples()
    }
    gaps: dict[int, float] = {}
    for row in intervals.to_dict(orient="records"):
        driver = _number(row.get("driver_number"))
        gap = _number(row.get("gap_to_leader_seconds"))
        if driver is not None and gap is not None:
            gaps[int(driver)] = max(0.0, gap)
    if focus_driver not in gaps:
        if latest_positions.get(focus_driver) == 1:
            gaps[focus_driver] = 0.0
        else:
            return {"position": latest_positions.get(focus_driver), "ahead": None, "behind": None, "gap_ahead": None, "gap_behind": None, "traffic": "unknown"}
    focus_pace = pace.get(focus_driver)
    projected: dict[int, float] = {}
    for driver, gap in gaps.items():
        delta = 0.0
        if focus_pace is not None and pace.get(driver) is not None:
            delta = (pace[driver] - focus_pace) * laps_until_stop
        projected[driver] = gap + delta
    focus_gap = projected[focus_driver] + loss
    others = sorted((gap, driver) for driver, gap in projected.items() if driver != focus_driver)
    ahead_rows = [(gap, driver) for gap, driver in others if gap <= focus_gap]
    behind_rows = [(gap, driver) for gap, driver in others if gap > focus_gap]
    ahead = max(ahead_rows, default=None)
    behind = min(behind_rows, default=None)
    position = 1 + sum(gap < focus_gap for gap, _ in others)
    traffic = "clear"
    if ahead is not None and focus_gap - ahead[0] < 2.0:
        traffic = "pass_required" if pace.get(ahead[1], math.inf) > pace.get(focus_driver, math.inf) else "traffic"
    return {
        "position": position,
        "ahead": ahead[1] if ahead else None,
        "behind": behind[1] if behind else None,
        "gap_ahead": focus_gap - ahead[0] if ahead else None,
        "gap_behind": behind[0] - focus_gap if behind else None,
        "traffic": traffic,
    }


def calculate_recommendation(
    race: dict[str, pd.DataFrame],
    *,
    profile: PaceProfile,
    forecast: pd.DataFrame,
    focus_driver: int,
    decision_time: pd.Timestamp,
    race_start: pd.Timestamp,
    planned_laps: int,
    pit_loss: PitLoss,
    trigger_type: str,
    input_manifest_hash: str,
) -> dict[str, Any]:
    laps = _safe_cut(race["laps"], decision_time)
    focus_laps = laps[pd.to_numeric(laps.get("driver_number"), errors="coerce").eq(focus_driver)]
    completed = pd.to_numeric(focus_laps.get("lap_number"), errors="coerce").dropna()
    current_lap = 1 if decision_time <= race_start or completed.empty else int(completed.max()) + 1
    current_lap = min(current_lap, planned_laps)
    current_laps = {
        int(driver): int(group["lap_number"].max()) + 1
        for driver, group in laps.assign(lap_number=pd.to_numeric(laps.get("lap_number"), errors="coerce")).dropna(subset=["driver_number", "lap_number"]).groupby("driver_number")
    }
    stints, ages, used = _current_compounds(race["stints"], decision_time, current_laps)
    current_compound = stints.get(focus_driver)
    current_age = ages.get(focus_driver, 0)
    if decision_time <= race_start:
        current_compound = None
        current_age = 0
        used = {}
    pace = _pace_by_driver(race["laps"], decision_time, profile)
    focus_pace = pace.get(focus_driver)
    if focus_pace is None and profile.driver_pace:
        focus_pace = float(np.median(list(profile.driver_pace.values())))
        pace[focus_driver] = focus_pace
    status, latest_message = _status_at(race["race_control"], decision_time)
    calculation_id = hashlib.sha256(
        f"{input_manifest_hash}:{focus_driver}:{decision_time.isoformat()}:{FEATURE_VERSION}:{CALCULATION_VERSION}".encode()
    ).hexdigest()[:20]
    base = {
        "calculation_id": calculation_id,
        "decision_time": decision_time.isoformat(),
        "trigger_id": hashlib.sha256(f"{trigger_type}:{decision_time.isoformat()}".encode()).hexdigest()[:16],
        "trigger_type": trigger_type,
        "feature_version": FEATURE_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "input_manifest_sha256": input_manifest_hash,
        "driver_number": focus_driver,
        "lap_number": current_lap,
        "race_status": status,
        "current_compound": current_compound,
        "current_position": None,
    }
    positions = _latest_by_driver(race["position"], decision_time)
    intervals = _latest_by_driver(race["intervals"], decision_time)
    focus_position = positions[pd.to_numeric(positions.get("driver_number"), errors="coerce").eq(focus_driver)]
    if not focus_position.empty:
        base["current_position"] = int(focus_position.iloc[-1]["position"])
    if current_lap >= planned_laps:
        return {
            **base,
            "status": "available",
            "action": "FINISH",
            "recommended_pit_lap": None,
            "laps_until_pit": None,
            "next_compound": None,
            "expected_exit_position": base["current_position"],
            "expected_finish_position": base["current_position"],
            "expected_remaining_seconds": 0.0,
            "ahead_driver": None,
            "behind_driver": None,
            "gap_ahead_seconds": None,
            "gap_behind_seconds": None,
            "traffic_state": "finished",
            "plan_json": "[]",
            "reasons_json": json.dumps(["Scheduled race distance reached."]),
            "assumptions_json": json.dumps([pit_loss.neutralized_basis]),
        }
    if status == "RED":
        return {
            **base,
            "status": "partial",
            "action": "HOLD",
            "recommended_pit_lap": None,
            "laps_until_pit": None,
            "next_compound": None,
            "expected_exit_position": None,
            "expected_finish_position": base["current_position"],
            "expected_remaining_seconds": None,
            "ahead_driver": None,
            "behind_driver": None,
            "gap_ahead_seconds": None,
            "gap_behind_seconds": None,
            "traffic_state": "suspended",
            "plan_json": "[]",
            "reasons_json": json.dumps(["Race suspended; replan at restart."]),
            "assumptions_json": json.dumps([pit_loss.neutralized_basis]),
        }
    if focus_pace is None:
        return {
            **base,
            "status": "unavailable",
            "action": "UNAVAILABLE",
            "recommended_pit_lap": None,
            "laps_until_pit": None,
            "next_compound": None,
            "expected_exit_position": None,
            "expected_finish_position": base["current_position"],
            "expected_remaining_seconds": None,
            "ahead_driver": None,
            "behind_driver": None,
            "gap_ahead_seconds": None,
            "gap_behind_seconds": None,
            "traffic_state": "unknown",
            "plan_json": "[]",
            "reasons_json": json.dumps(["No usable point-in-time pace evidence."]),
            "assumptions_json": json.dumps([pit_loss.neutralized_basis]),
        }
    currently_wet = _current_weather(race["weather"], decision_time)
    rain_lap = _forecast_rain_lap(forecast, decision_time, current_lap, focus_pace, planned_laps)
    actionable_rain_lap = (
        rain_lap
        if rain_lap is not None
        and rain_lap <= current_lap + 8
        and rain_lap <= planned_laps - 3
        else None
    )
    wet_compound = "WET" if status == "SC" and "LOW GRIP" in latest_message else "INTERMEDIATE"
    plans = _candidate_plans(
        current_lap=current_lap,
        planned_laps=planned_laps,
        current_compound=current_compound,
        used_compounds=used.get(focus_driver, ()),
        current_age=current_age,
        allow_immediate_stop=status in {"SC", "VSC"},
        profile=profile,
        rain_lap=actionable_rain_lap,
        currently_wet=currently_wet,
        wet_compound=wet_compound,
    )
    if not plans:
        return {**base, "status": "unavailable", "action": "UNAVAILABLE", "recommended_pit_lap": None, "laps_until_pit": None, "next_compound": None, "expected_exit_position": None, "expected_finish_position": base["current_position"], "expected_remaining_seconds": None, "ahead_driver": None, "behind_driver": None, "gap_ahead_seconds": None, "gap_behind_seconds": None, "traffic_state": "unknown", "plan_json": "[]", "reasons_json": json.dumps(["No legal strategy candidate could be built."]), "assumptions_json": json.dumps([pit_loss.neutralized_basis])}
    competitor_remaining: dict[int, float] = {}
    for driver, driver_pace in pace.items():
        if driver == focus_driver:
            continue
        compound = stints.get(driver, "MEDIUM")
        remaining = planned_laps - current_laps.get(driver, current_lap) + 1
        competitor_remaining[driver] = _stint_time(driver_pace, compound, remaining, ages.get(driver, 0), profile)
        if remaining > profile.maximum_age.get(compound, 25):
            competitor_remaining[driver] += pit_loss.green_seconds
    interval_gaps = {
        int(row.driver_number): float(row.gap_to_leader_seconds)
        for row in intervals.dropna(subset=["driver_number", "gap_to_leader_seconds"]).itertuples()
    }
    focus_gap = interval_gaps.get(focus_driver, 0.0 if base["current_position"] == 1 else math.nan)
    scored: list[tuple[int, float, list[dict[str, Any]]]] = []
    for plan in plans:
        remaining_time = _plan_time(focus_pace, plan, current_lap, current_age, profile, pit_loss, status)
        if math.isfinite(focus_gap) and competitor_remaining:
            focus_arrival = focus_gap + remaining_time
            finish_position = 1 + sum(
                interval_gaps.get(driver, math.inf) + value < focus_arrival
                for driver, value in competitor_remaining.items()
            )
        else:
            finish_position = int(base["current_position"] or 99)
        scored.append((finish_position, remaining_time, plan))
    finish_position, remaining_time, best = min(scored, key=lambda item: (item[0], item[1]))
    next_stint = best[1] if len(best) > 1 else None
    pit_lap = int(best[0]["end_lap"]) if next_stint is not None else None
    laps_until = max(0, pit_lap - current_lap) if pit_lap is not None else None
    loss = pit_loss.for_status(status if laps_until == 0 else "GREEN")
    projection = _projection(positions, intervals, pace, focus_driver, laps_until or 0, loss) if pit_lap is not None else {"position": base["current_position"], "ahead": None, "behind": None, "gap_ahead": None, "gap_behind": None, "traffic": "clear"}
    if decision_time <= race_start and current_compound is None:
        action = "START"
        next_compound = str(best[0]["compound"])
    elif pit_lap is None:
        action = "STAY OUT"
        next_compound = None
    elif laps_until == 0:
        action = "PIT NOW"
        next_compound = str(next_stint["compound"])
    else:
        action = f"PIT LAP {pit_lap}"
        next_compound = str(next_stint["compound"])
    reasons = []
    if status in {"SC", "VSC"} and laps_until == 0:
        reasons.append(f"Reduced {status} pit loss.")
    if actionable_rain_lap is not None:
        reasons.append(f"Forecast rain window around lap {actionable_rain_lap}.")
    if currently_wet:
        reasons.append("Rain is already observed at the track.")
    if projection["traffic"] == "pass_required":
        reasons.append("Projected pit exit requires passing a slower car.")
    elif projection["traffic"] == "clear":
        reasons.append("Projected pit exit is in clear air.")
    if not reasons:
        reasons.append("Best position-first candidate; time breaks equal-position ties.")
    status_value = "available"
    if forecast.empty or positions.empty or intervals.empty or current_compound is None and decision_time > race_start:
        status_value = "partial"
    return {
        **base,
        "status": status_value,
        "action": action,
        "recommended_pit_lap": pit_lap,
        "laps_until_pit": laps_until,
        "next_compound": next_compound,
        "expected_exit_position": projection["position"],
        "expected_finish_position": finish_position,
        "expected_remaining_seconds": remaining_time,
        "ahead_driver": projection["ahead"],
        "behind_driver": projection["behind"],
        "gap_ahead_seconds": projection["gap_ahead"],
        "gap_behind_seconds": projection["gap_behind"],
        "traffic_state": projection["traffic"],
        "plan_json": json.dumps(best, separators=(",", ":")),
        "reasons_json": json.dumps(reasons, separators=(",", ":")),
        "assumptions_json": json.dumps(
            [
                pit_loss.neutralized_basis,
                "Future Safety Cars and competitor reactions are not known.",
                "Compound recommendation does not prove physical set availability.",
            ],
            separators=(",", ":"),
        ),
    }


def _triggers(
    race: dict[str, pd.DataFrame],
    forecast: pd.DataFrame,
    focus_driver: int,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[tuple[pd.Timestamp, str]]:
    values: list[tuple[pd.Timestamp, str]] = [(start, "pre_race")]
    laps = race["laps"]
    if not laps.empty:
        focus = laps[pd.to_numeric(laps.get("driver_number"), errors="coerce").eq(focus_driver)]
        values.extend((time, "lap_completion") for time in (_timestamp(value) for value in focus.get("event_time", [])) if time is not None)
    for endpoint, trigger in (("pit", "pit_stop"), ("race_control", "race_control")):
        frame = race[endpoint]
        if not frame.empty:
            values.extend((time, trigger) for time in (_timestamp(value) for value in frame.get("event_time", [])) if time is not None)
    if not forecast.empty:
        values.extend((time, "forecast") for time in (_timestamp(value) for value in forecast.get("available_at", [])) if time is not None)
    priority = {"pre_race": 0, "forecast": 1, "lap_completion": 2, "pit_stop": 3, "race_control": 4}
    selected: dict[pd.Timestamp, str] = {}
    for time, trigger in values:
        if start <= time <= end and priority[trigger] >= priority.get(selected.get(time, "pre_race"), 0):
            selected[time] = trigger
    return sorted(selected.items())


def _evaluations(
    recommendations: pd.DataFrame,
    race: dict[str, pd.DataFrame],
    focus_driver: int,
) -> pd.DataFrame:
    pits = race["pit"]
    stints = race["stints"]
    if pits.empty:
        return pd.DataFrame(columns=EVALUATION_COLUMNS)
    pits = pits[pd.to_numeric(pits.get("driver_number"), errors="coerce").eq(focus_driver)].copy()
    rows: list[dict[str, Any]] = []
    for pit in pits.to_dict(orient="records"):
        exit_time = _timestamp(pit.get("event_time"))
        if exit_time is None:
            continue
        duration = _number(pit.get("lane_duration_seconds")) or _number(pit.get("pit_duration_seconds")) or 0.0
        entry_time = exit_time - pd.to_timedelta(max(0.0, duration), unit="s")
        available_at = _timestamp(pit.get("available_at")) or exit_time
        actual_lap = int(_number(pit.get("lap_number")) or 0)
        next_rows = stints[
            pd.to_numeric(stints.get("driver_number"), errors="coerce").eq(focus_driver)
            & pd.to_numeric(stints.get("lap_start"), errors="coerce").ge(actual_lap)
        ].sort_values("lap_start")
        actual_compound = str(next_rows.iloc[0].get("compound") or "").upper() if not next_rows.empty else None
        prior = recommendations[
            pd.to_datetime(
                recommendations["decision_time"], format="mixed", utc=True
            ).le(entry_time)
        ]
        recommendation = prior.iloc[-1] if not prior.empty else None
        recommended_lap = int(recommendation["recommended_pit_lap"]) if recommendation is not None and pd.notna(recommendation["recommended_pit_lap"]) else None
        recommended_compound = str(recommendation["next_compound"]) if recommendation is not None and pd.notna(recommendation["next_compound"]) else None
        rows.append(
            {
                "actual_pit_lap": actual_lap,
                "actual_compound": actual_compound,
                "entry_time": entry_time.isoformat(),
                "revealed_at": max(exit_time, available_at).isoformat(),
                "recommendation_id": recommendation["calculation_id"] if recommendation is not None else None,
                "recommended_pit_lap": recommended_lap,
                "recommended_compound": recommended_compound,
                "lap_error": actual_lap - recommended_lap if recommended_lap is not None else None,
                "within_one_lap": abs(actual_lap - recommended_lap) <= 1 if recommended_lap is not None else False,
                "within_two_laps": abs(actual_lap - recommended_lap) <= 2 if recommended_lap is not None else False,
                "compound_match": actual_compound == recommended_compound if actual_compound and recommended_compound else False,
            }
        )
    return pd.DataFrame(rows, columns=EVALUATION_COLUMNS)


def calculate_strategy_timeline(
    race: dict[str, pd.DataFrame],
    prior_sessions: list[dict[str, pd.DataFrame]],
    *,
    forecast: pd.DataFrame,
    focus_driver: int,
    race_start: pd.Timestamp,
    decision_time: pd.Timestamp,
    planned_laps: int,
    pit_loss: PitLoss,
    input_manifest_hash: str,
) -> StrategyResult:
    profile = build_pace_profile(prior_sessions)
    triggers = _triggers(race, forecast, focus_driver, race_start, decision_time)
    rows = [
        calculate_recommendation(
            race,
            profile=profile,
            forecast=forecast,
            focus_driver=focus_driver,
            decision_time=time,
            race_start=race_start,
            planned_laps=planned_laps,
            pit_loss=pit_loss,
            trigger_type=trigger,
            input_manifest_hash=input_manifest_hash,
        )
        for time, trigger in triggers
    ]
    recommendations = pd.DataFrame(rows)
    evaluations = (
        _evaluations(recommendations, race, focus_driver)
        if not recommendations.empty
        else pd.DataFrame(columns=EVALUATION_COLUMNS)
    )
    statuses = set(recommendations.get("status", pd.Series(dtype="object")))
    status = "unavailable" if not rows or statuses == {"unavailable"} else "partial" if "partial" in statuses or "unavailable" in statuses else "available"
    return StrategyResult(
        recommendations,
        evaluations,
        status,
        {
            "trigger_count": len(triggers),
            "profile_driver_count": len(profile.driver_pace),
            "profile_compounds": list(profile.compounds),
            "evaluation_stop_count": len(evaluations),
        },
    )


def build_race_strategy(
    pipeline_manifest: dict[str, Any],
    pipeline_manifest_path: Path,
    *,
    focus_driver: int,
    artifact_dir: Path = ARTIFACTS_DIR,
    manifest_dir: Path = CURATED_DATA_DIR / "manifests",
) -> tuple[dict[str, Any], Path]:
    selection = pipeline_manifest.get("selection", {})
    context = pipeline_manifest.get("context") or {}
    target = context.get("target_session") or {}
    if selection.get("purpose") != "race_strategy":
        raise RaceStrategyError("Strategy requires a race_strategy pipeline manifest.")
    target_key = target.get("source_session_key")
    target_id = str(target.get("session_id") or "")
    if target_key is None or not target_id:
        raise RaceStrategyError("Strategy manifest has no target race session.")
    decision_time = decision_timestamp(selection.get("decision_time"))
    race_start = decision_timestamp(target.get("scheduled_start_utc"))
    input_hash = sha256(pipeline_manifest_path)
    identity = json.dumps(
        {
            "session": target_id,
            "focus_driver": int(focus_driver),
            "decision_time": decision_time.isoformat(),
            "input": input_hash,
            "features": FEATURE_VERSION,
            "calculation": CALCULATION_VERSION,
        },
        sort_keys=True,
    )
    run_id = hashlib.sha256(identity.encode()).hexdigest()[:20]
    snapshot_path = manifest_dir / "calculations" / f"race_strategy_{int(target_key)}_{int(focus_driver)}_{run_id}.json"
    if snapshot_path.is_file():
        try:
            existing = json.loads(snapshot_path.read_text(encoding="utf-8"))
            outputs = existing.get("outputs", {})
            if (
                existing.get("calculation_id") == run_id
                and existing.get("input_manifest_sha256") == input_hash
                and all(
                    _project_path(str(value.get("reference"))).is_file()
                    and sha256(_project_path(str(value.get("reference"))))
                    == value.get("sha256")
                    for value in outputs.values()
                    if isinstance(value, dict) and value.get("reference")
                )
            ):
                return existing, snapshot_path
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            pass
    context_by_key = {int(row["source_session_key"]): row for row in context.get("sessions", [])}
    race: dict[str, pd.DataFrame] | None = None
    priors: list[dict[str, pd.DataFrame]] = []
    errors: list[str] = []
    for manifest in pipeline_manifest.get("jobs", {}).get("openf1_weekend_facts", {}).get("sessions", []):
        if not isinstance(manifest, dict) or manifest.get("session_key") is None:
            continue
        key = int(manifest["session_key"])
        try:
            frames = _frames_from_manifest(manifest)
        except RaceStrategyError as exc:
            errors.append(f"Session {key}: {exc}")
            continue
        if key == int(target_key):
            race = frames
        elif key in context_by_key:
            priors.append(frames)
    if race is None:
        raise RaceStrategyError("Target race facts are unavailable for strategy replay.")
    target_drivers = {
        int(value)
        for value in pd.to_numeric(
            race.get("drivers", pd.DataFrame()).get(
                "driver_number", pd.Series(dtype="float64")
            ),
            errors="coerce",
        ).dropna()
    }
    if int(focus_driver) not in target_drivers:
        raise RaceStrategyError(
            f"Focus driver {int(focus_driver)} is not present in target session {int(target_key)}."
        )
    forecast = pd.DataFrame()
    forecast_job = pipeline_manifest.get("jobs", {}).get("open_meteo", {})
    if isinstance(forecast_job, dict) and forecast_job.get("status") in READABLE_STATUSES and forecast_job.get("curated_path"):
        try:
            forecast = _read_parquet(str(forecast_job["curated_path"]), str(forecast_job.get("curated_sha256")) if forecast_job.get("curated_sha256") else None)
        except RaceStrategyError as exc:
            errors.append(str(exc))
    identifiers = (
        context.get("meeting_name"),
        context.get("circuit_location"),
        context.get("circuit_name"),
        context.get("circuit_country"),
    )
    try:
        pit_loss = load_pit_loss(*identifiers)
    except PitLossConfigError as exc:
        raise RaceStrategyError(str(exc)) from exc
    if pit_loss is None:
        raise RaceStrategyError("No circuit pit-loss assumption is configured.")
    planned_laps, distance_basis = _load_race_distance(int(selection["season"]), pit_loss.key)
    if planned_laps is None:
        raise RaceStrategyError(distance_basis)
    result = calculate_strategy_timeline(
        race,
        priors,
        forecast=forecast,
        focus_driver=int(focus_driver),
        race_start=race_start,
        decision_time=decision_time,
        planned_laps=planned_laps,
        pit_loss=pit_loss,
        input_manifest_hash=input_hash,
    )
    output_dir = artifact_dir / "calculations" / "race_strategy" / f"session={int(target_key)}" / f"driver={int(focus_driver)}"
    recommendations_path = output_dir / f"recommendations_{run_id}.parquet"
    evaluations_path = output_dir / f"evaluations_{run_id}.parquet"
    atomic_parquet(result.recommendations, recommendations_path)
    atomic_parquet(result.evaluations, evaluations_path)
    status = result.status
    diagnostics = {**result.diagnostics, "input_errors": errors, "race_distance_basis": distance_basis}
    if errors and status == "available":
        status = "partial"
    snapshot = {
        "schema_version": 1,
        "calculation_id": run_id,
        "strategy_run_id": run_id,
        "calculation_type": "race_strategy",
        "session_id": target_id,
        "target_session_key": int(target_key),
        "focus_driver": int(focus_driver),
        "decision_time": decision_time.isoformat(),
        "trigger_id": run_id,
        "trigger_type": "historical_replay",
        "calculated_at": datetime.now(timezone.utc).isoformat(),
        "feature_version": FEATURE_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "input_manifest_path": _relative_path(pipeline_manifest_path),
        "input_manifest_sha256": input_hash,
        "status": status,
        "pit_loss": {
            "green_seconds": pit_loss.green_seconds,
            "vsc_seconds": pit_loss.vsc_seconds,
            "sc_seconds": pit_loss.sc_seconds,
            "source_asset": pit_loss.source_asset,
            "neutralized_basis": pit_loss.neutralized_basis,
        },
        "planned_laps": planned_laps,
        "outputs": {
            "recommendations": {"reference": _relative_path(recommendations_path), "sha256": sha256(recommendations_path), "row_count": len(result.recommendations)},
            "evaluations": {"reference": _relative_path(evaluations_path), "sha256": sha256(evaluations_path), "row_count": len(result.evaluations)},
        },
        "diagnostics": diagnostics,
    }
    atomic_json(snapshot, snapshot_path)
    return snapshot, snapshot_path
