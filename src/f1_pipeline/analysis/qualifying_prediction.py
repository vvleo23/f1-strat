from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd

from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.settings import ARTIFACTS_DIR, CURATED_DATA_DIR, PROJECT_ROOT
from f1_pipeline.temporal import TemporalCutError, cut_facts, decision_timestamp
from f1_pipeline.weather import WeatherCutError, build_weather_cut

FEATURE_VERSION = "qualifying_features_v1"
CALCULATION_VERSION = "qualifying_baseline_v1"
SIMULATION_COUNT = 10_000
READABLE_STATUSES = frozenset({"available", "partial", "stale"})


class QualifyingPredictionError(RuntimeError):
    pass


@dataclass(frozen=True)
class SessionInput:
    session_key: int
    session_type: str
    scheduled_start: pd.Timestamp
    laps: pd.DataFrame
    stints: pd.DataFrame
    entries: pd.DataFrame
    weather: pd.DataFrame


@dataclass(frozen=True)
class Calibration:
    residuals: pd.DataFrame
    driver_priors: dict[int, float]
    team_priors: dict[str, float]
    leader_ratios: dict[str, tuple[float, ...]]


@dataclass(frozen=True)
class PredictionResult:
    rows: pd.DataFrame
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


def _read_parquet(path_value: str, expected_hash: str | None = None) -> pd.DataFrame:
    path = _project_path(path_value)
    if not path.is_file():
        raise QualifyingPredictionError(f"Prediction input is missing: {path}")
    if expected_hash and sha256(path) != expected_hash:
        raise QualifyingPredictionError(f"Prediction input failed its hash check: {path}")
    try:
        return pd.read_parquet(path)
    except (OSError, ValueError) as exc:
        raise QualifyingPredictionError(f"Could not read prediction input: {exc}") from exc


def _frames_from_manifest(manifest: dict[str, Any]) -> dict[str, pd.DataFrame]:
    endpoints = manifest.get("endpoints", {})
    frames: dict[str, pd.DataFrame] = {}
    for endpoint in ("laps", "stints", "drivers", "weather"):
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
        raise QualifyingPredictionError(str(exc)) from exc


def _numeric_column(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        return pd.Series(math.nan, index=frame.index, dtype="float64")
    return pd.to_numeric(frame[column], errors="coerce")


def _session_weather(weather: pd.DataFrame) -> dict[str, Any]:
    if weather.empty:
        return {"air_temperature": math.nan, "track_temperature": math.nan, "regime": None}
    air = _numeric_column(weather, "air_temperature")
    track = _numeric_column(weather, "track_temperature")
    rainfall = _numeric_column(weather, "rainfall")
    return {
        "air_temperature": float(air.median()) if air.notna().any() else math.nan,
        "track_temperature": float(track.median()) if track.notna().any() else math.nan,
        "regime": (
            "wet" if rainfall.fillna(0).gt(0).any() else "dry"
        ) if rainfall.notna().any() else None,
    }


def _target_weather(
    forecast: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> dict[str, Any]:
    if forecast.empty or "valid_time" not in forecast.columns:
        return {"air_temperature": math.nan, "regime": None}
    data = forecast.copy()
    data["valid_time"] = pd.to_datetime(data["valid_time"], utc=True, errors="coerce")
    within = data[data["valid_time"].between(start, end)]
    if within.empty:
        distances = (data["valid_time"] - start).abs()
        within = data.loc[[distances.idxmin()]] if distances.notna().any() else data.iloc[0:0]
    temperature = _numeric_column(within, "temperature_2m")
    rain = _numeric_column(within, "rain")
    precipitation = _numeric_column(within, "precipitation")
    return {
        "air_temperature": (
            float(temperature.mean()) if temperature.notna().any() else math.nan
        ),
        "regime": (
            "wet"
            if rain.fillna(0).gt(0).any() or precipitation.fillna(0).gt(0).any()
            else "dry"
        ) if rain.notna().any() or precipitation.notna().any() else None,
    }


def _best_laps(
    session: SessionInput, decision_time: pd.Timestamp
) -> tuple[pd.DataFrame, list[float]]:
    laps = _safe_cut(session.laps, decision_time)
    required = {"driver_number", "lap_number", "lap_duration_seconds"}
    if laps.empty or not required.issubset(laps.columns):
        return pd.DataFrame(), []
    laps = laps.copy()
    laps["lap_duration_seconds"] = _numeric_column(laps, "lap_duration_seconds")
    valid = (
        laps["lap_duration_seconds"].notna()
        & np.isfinite(laps["lap_duration_seconds"])
        & laps["lap_duration_seconds"].gt(0)
    )
    if "is_pit_out_lap" in laps.columns:
        valid &= ~laps["is_pit_out_lap"].fillna(False).astype(bool)
    laps = laps[valid & laps["driver_number"].notna()].copy()
    if laps.empty:
        return pd.DataFrame(), []
    laps["driver_number"] = pd.to_numeric(laps["driver_number"], errors="coerce").astype("Int64")
    laps["lap_number"] = pd.to_numeric(laps["lap_number"], errors="coerce").astype("Int64")
    spread: list[float] = []
    for _, group in laps.groupby("driver_number"):
        values = group["lap_duration_seconds"].nsmallest(2).tolist()
        if len(values) == 2 and values[0] > 0:
            spread.append((values[1] - values[0]) / values[0])
    best = laps.sort_values("lap_duration_seconds").drop_duplicates("driver_number")
    stints = _safe_cut(session.stints, decision_time)
    if not stints.empty and {
        "driver_number",
        "lap_start",
        "lap_end",
        "compound",
        "tyre_age_at_start",
    }.issubset(stints.columns):
        stint_data = stints.copy()
        for column in ("driver_number", "lap_start", "lap_end", "tyre_age_at_start"):
            stint_data[column] = _numeric_column(stint_data, column)
        joined = best.merge(
            stint_data[["driver_number", "lap_start", "lap_end", "compound", "tyre_age_at_start"]],
            on="driver_number",
            how="left",
        )
        covering = joined[
            joined["lap_number"].ge(joined["lap_start"])
            & joined["lap_number"].le(joined["lap_end"])
        ].sort_values(["driver_number", "lap_start"], ascending=[True, False])
        covering = covering.drop_duplicates("driver_number")
        best = best.merge(
            covering[["driver_number", "compound", "tyre_age_at_start", "lap_start"]],
            on="driver_number",
            how="left",
        )
        best["tyre_age_laps"] = (
            pd.to_numeric(best["tyre_age_at_start"], errors="coerce")
            + pd.to_numeric(best["lap_number"], errors="coerce")
            - pd.to_numeric(best["lap_start"], errors="coerce")
        )
        best.loc[best["tyre_age_laps"].lt(0), "tyre_age_laps"] = math.nan
        best = best.drop(columns=["tyre_age_at_start", "lap_start"])
    else:
        best["compound"] = pd.NA
        best["tyre_age_laps"] = pd.NA
    leader = float(best["lap_duration_seconds"].min())
    best["session_gap_ratio"] = best["lap_duration_seconds"] / leader - 1.0
    weather = _session_weather(_safe_cut(session.weather, decision_time))
    best["session_key"] = session.session_key
    best["session_type"] = session.session_type
    best["scheduled_start"] = session.scheduled_start
    best["leader_lap_seconds"] = leader
    best["air_temperature"] = weather["air_temperature"]
    best["track_temperature"] = weather["track_temperature"]
    best["weather_regime"] = weather["regime"]
    return best, spread


def _temperature_bin(source: Any, target: Any) -> int | None:
    try:
        delta = float(target) - float(source)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(delta):
        return None
    return int(round(max(-20.0, min(20.0, delta)) / 5.0) * 5)


def _tyre_age_bin(value: Any) -> str | None:
    try:
        age = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(age) or age < 0:
        return None
    if age <= 1:
        return "fresh"
    if age <= 4:
        return "used"
    return "old"


def _residual_adjustment(row: pd.Series, calibration: Calibration) -> tuple[float, np.ndarray]:
    residuals = calibration.residuals
    if residuals.empty:
        return 0.0, np.array([], dtype=float)
    candidates = residuals
    for columns in (
        (
            "session_type",
            "compound",
            "tyre_age_bin",
            "source_regime",
            "target_regime",
            "temperature_bin",
        ),
        ("session_type", "compound", "source_regime", "target_regime"),
        ("session_type", "source_regime", "target_regime"),
        ("session_type",),
        (),
    ):
        selected = candidates
        for column in columns:
            value = row.get(column)
            if value is None or pd.isna(value):
                continue
            selected = selected[selected[column].eq(value)]
        if not selected.empty:
            values = pd.to_numeric(selected["gap_residual"], errors="coerce").dropna().to_numpy()
            if values.size:
                median = float(np.median(values))
                return median, values - median
    return 0.0, np.array([], dtype=float)


def _weighted_median(values: Iterable[float], weights: Iterable[float]) -> float:
    pairs = sorted((float(value), float(weight)) for value, weight in zip(values, weights))
    total = sum(weight for _, weight in pairs)
    threshold = total / 2.0
    running = 0.0
    for value, weight in pairs:
        running += weight
        if running >= threshold:
            return value
    return pairs[-1][0]


def _session_weights(sessions: list[SessionInput]) -> dict[int, float]:
    practices = sorted(
        (session for session in sessions if session.session_type == "practice"),
        key=lambda session: session.scheduled_start,
        reverse=True,
    )
    weights = {session.session_key: 0.75**index for index, session in enumerate(practices)}
    for session in sessions:
        if session.session_type == "sprint_qualifying":
            weights[session.session_key] = 1.25
        elif session.session_type == "sprint":
            weights[session.session_key] = 0.35
    return weights


def calculate_prediction(
    sessions: list[SessionInput],
    *,
    target_session_id: str,
    target_start: pd.Timestamp,
    target_end: pd.Timestamp,
    decision_time: pd.Timestamp,
    forecast: pd.DataFrame,
    calibration: Calibration,
    calculation_id: str,
) -> PredictionResult:
    if not sessions:
        return PredictionResult(
            pd.DataFrame(),
            "unavailable",
            {"errors": ["No eligible session is available."]},
        )
    target_weather = _target_weather(forecast, target_start, target_end)
    evidence_frames: list[pd.DataFrame] = []
    current_spread: list[float] = []
    session_errors: list[str] = []
    weights = _session_weights(sessions)
    for session in sessions:
        try:
            best, spread = _best_laps(session, decision_time)
        except QualifyingPredictionError as exc:
            session_errors.append(f"Session {session.session_key}: {exc}")
            continue
        current_spread.extend(spread)
        if best.empty:
            continue
        best["source_regime"] = best["weather_regime"]
        best["target_regime"] = target_weather["regime"]
        best["temperature_bin"] = [
            _temperature_bin(source, target_weather["air_temperature"])
            for source in best["air_temperature"]
        ]
        best["tyre_age_bin"] = best["tyre_age_laps"].map(_tyre_age_bin)
        best["weight"] = weights.get(session.session_key, 0.0)
        mismatch = (
            best["source_regime"].notna()
            & best["target_regime"].notna()
            & best["source_regime"].ne(best["target_regime"])
        )
        best.loc[mismatch, "weight"] *= 0.25
        best.loc[best["compound"].isna() | best["tyre_age_laps"].isna(), "weight"] *= 0.5
        adjustments: list[float] = []
        pools: list[np.ndarray] = []
        for _, row in best.iterrows():
            adjustment, pool = _residual_adjustment(row, calibration)
            adjustments.append(adjustment)
            pools.append(pool)
        best["adjusted_gap_ratio"] = best["session_gap_ratio"] + adjustments
        best["residual_pool"] = pools
        evidence_frames.append(best)
    if not evidence_frames:
        return PredictionResult(
            pd.DataFrame(),
            "unavailable",
            {
                "errors": ["Eligible sessions contain no valid laps."],
                "session_errors": session_errors,
            },
        )
    evidence = pd.concat(evidence_frames, ignore_index=True)
    latest = max(sessions, key=lambda session: session.scheduled_start)
    roster = latest.entries.copy()
    if roster.empty:
        roster = pd.concat([session.entries for session in sessions], ignore_index=True)
    roster = roster.dropna(subset=["driver_number"]).drop_duplicates("driver_number", keep="last")
    roster["driver_number"] = pd.to_numeric(
        roster["driver_number"], errors="coerce"
    ).astype("Int64")
    if roster.empty:
        roster = evidence[["driver_number"]].drop_duplicates()
        roster["name_acronym"] = roster["driver_number"].astype(str)
        roster["full_name"] = pd.NA
        roster["team_name"] = pd.NA
    rows: list[dict[str, Any]] = []
    all_residuals: list[float] = []
    for driver in roster.itertuples(index=False):
        driver_number = int(driver.driver_number)
        driver_evidence = evidence[evidence["driver_number"].eq(driver_number)]
        values = driver_evidence["adjusted_gap_ratio"].astype(float).tolist()
        driver_weights = driver_evidence["weight"].astype(float).tolist()
        team_name = getattr(driver, "team_name", None)
        prior = calibration.driver_priors.get(driver_number)
        if prior is None and isinstance(team_name, str):
            prior = calibration.team_priors.get(team_name)
        if prior is not None:
            values.append(prior)
            driver_weights.append(0.25)
        if not values or not any(weight > 0 for weight in driver_weights):
            rows.append(
                {
                    "target_session_id": target_session_id,
                    "driver_number": driver_number,
                    "name_acronym": getattr(driver, "name_acronym", None),
                    "full_name": getattr(driver, "full_name", None),
                    "team_name": team_name,
                    "predicted_gap_ratio": math.nan,
                    "evidence_count": len(driver_evidence),
                    "contributing_session_keys": [],
                    "compound_on_fastest_laps": [],
                    "tyre_age_on_fastest_laps": [],
                    "row_status": "unavailable",
                }
            )
            continue
        score = _weighted_median(values, driver_weights)
        pools = [pool for pool in driver_evidence["residual_pool"] if len(pool)]
        if pools:
            all_residuals.extend(np.concatenate(pools).tolist())
        rows.append(
            {
                "target_session_id": target_session_id,
                "driver_number": driver_number,
                "name_acronym": getattr(driver, "name_acronym", None),
                "full_name": getattr(driver, "full_name", None),
                "team_name": team_name,
                "predicted_gap_ratio": max(0.0, score),
                "evidence_count": len(driver_evidence),
                "contributing_session_keys": sorted(
                    driver_evidence["session_key"].astype(int).tolist()
                ),
                "compound_on_fastest_laps": (
                    driver_evidence["compound"].dropna().astype(str).tolist()
                ),
                "tyre_age_on_fastest_laps": (
                    pd.to_numeric(driver_evidence["tyre_age_laps"], errors="coerce")
                    .dropna()
                    .astype(int)
                    .tolist()
                ),
                "row_status": "partial" if driver_evidence.empty else "available",
            }
        )
    result = pd.DataFrame(rows)
    available = result["predicted_gap_ratio"].notna()
    if not available.any():
        return PredictionResult(
            result,
            "unavailable",
            {"errors": ["No target driver has usable evidence."]},
        )
    ordered = result.loc[available].sort_values(["predicted_gap_ratio", "driver_number"]).index
    result["predicted_position"] = pd.Series(pd.NA, index=result.index, dtype="Int64")
    result.loc[ordered, "predicted_position"] = range(1, len(ordered) + 1)
    leader_ratios = calibration.leader_ratios.get(latest.session_type, ())
    leader_lap = evidence[evidence["session_key"].eq(latest.session_key)]["leader_lap_seconds"]
    pole_lap = (
        float(leader_lap.iloc[0]) * float(np.median(leader_ratios))
        if not leader_lap.empty and leader_ratios
        else math.nan
    )
    result["projected_lap_seconds"] = pole_lap * (1.0 + result["predicted_gap_ratio"])
    result["projected_gap_seconds"] = result["projected_lap_seconds"] - pole_lap
    residual_pool = np.asarray(all_residuals, dtype=float)
    calibrated = residual_pool.size > 1
    if not calibrated and current_spread:
        spread = float(np.median([value for value in current_spread if value >= 0]))
        if spread > 0:
            residual_pool = np.array([-spread, 0.0, spread], dtype=float)
    uncertainty_multiplier = 1.0
    if target_weather["regime"] is None and residual_pool.size:
        uncertainty_multiplier = 1.5
        residual_pool = residual_pool * uncertainty_multiplier
    probability_columns = (
        "top_15_probability",
        "top_10_probability",
        "top_5_probability",
        "position_low",
        "position_high",
    )
    for column in probability_columns:
        result[column] = math.nan
    if residual_pool.size > 1:
        indices = result.index[available].tolist()
        scores = result.loc[indices, "predicted_gap_ratio"].to_numpy(dtype=float)
        rng = np.random.default_rng(int(calculation_id[:16], 16))
        noise = rng.choice(residual_pool, size=(SIMULATION_COUNT, len(indices)), replace=True)
        simulated = scores + noise
        order = np.argsort(simulated, axis=1, kind="stable")
        positions = np.empty_like(order)
        positions[np.arange(SIMULATION_COUNT)[:, None], order] = np.arange(1, len(indices) + 1)
        for offset, index in enumerate(indices):
            result.at[index, "top_15_probability"] = float(np.mean(positions[:, offset] <= 15))
            result.at[index, "top_10_probability"] = float(np.mean(positions[:, offset] <= 10))
            result.at[index, "top_5_probability"] = float(np.mean(positions[:, offset] <= 5))
            result.at[index, "position_low"] = float(np.quantile(positions[:, offset], 0.1))
            result.at[index, "position_high"] = float(np.quantile(positions[:, offset], 0.9))
    result = result.sort_values(
        ["predicted_position", "driver_number"], na_position="last"
    ).reset_index(drop=True)
    result.insert(0, "calculation_id", calculation_id)
    result["decision_time"] = decision_time
    reasons: list[str] = []
    if target_weather["regime"] is None:
        reasons.append("Target weather forecast is unavailable.")
    if calibration.residuals.empty:
        reasons.append(
            "Historical calibration is unavailable; current-session uncertainty was used."
        )
    if result["projected_lap_seconds"].isna().any():
        reasons.append(
            "Projected lap times are unavailable without a local leader-time calibration."
        )
    if result["row_status"].ne("available").any():
        reasons.append("At least one known driver has no usable pace evidence.")
    if evidence["compound"].isna().any() or evidence["tyre_age_laps"].isna().any():
        reasons.append("At least one fastest lap has incomplete tyre data.")
    if result["top_5_probability"].isna().any():
        reasons.append("Probability uncertainty could not be derived for every driver.")
    if session_errors:
        reasons.append("At least one eligible session input was unavailable.")
    status = "partial" if reasons else "available"
    diagnostics = {
        "eligible_session_keys": [session.session_key for session in sessions],
        "evidence_rows": len(evidence),
        "driver_rows": len(result),
        "simulation_count": SIMULATION_COUNT if residual_pool.size > 1 else 0,
        "calibrated": calibrated,
        "calibration_rows": len(calibration.residuals),
        "uncertainty_multiplier": uncertainty_multiplier,
        "target_weather": {
            "air_temperature": (
                target_weather["air_temperature"]
                if math.isfinite(target_weather["air_temperature"])
                else None
            ),
            "regime": target_weather["regime"],
        },
        "reasons": reasons,
        "session_errors": session_errors,
    }
    return PredictionResult(result, status, diagnostics)


def _latest_session_manifest(session_key: int, manifest_dir: Path) -> dict[str, Any] | None:
    candidates: list[tuple[float, dict[str, Any]]] = []
    for path in (manifest_dir / "openf1_sessions").glob(f"session_{session_key}_*.json"):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if payload.get("status") in READABLE_STATUSES:
            candidates.append((path.stat().st_mtime, payload))
    return max(candidates, key=lambda item: item[0])[1] if candidates else None


def _local_calibration(
    season: int,
    target_start: pd.Timestamp,
    manifest_dir: Path,
) -> Calibration:
    session_path = CURATED_DATA_DIR / "dimensions" / f"season={season}" / "session.parquet"
    if not session_path.is_file():
        return Calibration(pd.DataFrame(), {}, {}, {})
    sessions = pd.read_parquet(session_path).copy()
    sessions["scheduled_start_utc"] = pd.to_datetime(
        sessions["scheduled_start_utc"], utc=True, errors="coerce"
    )
    sessions = sessions[sessions["scheduled_start_utc"].lt(target_start)]
    target_names = {"qualifying", "sprint qualifying", "sprint shootout"}
    targets = sessions[
        sessions["session_name"].astype(str).str.casefold().isin(target_names)
    ].sort_values("scheduled_start_utc")
    residual_rows: list[dict[str, Any]] = []
    driver_history: dict[int, list[float]] = {}
    team_history: dict[str, list[float]] = {}
    leader_ratios: dict[str, list[float]] = {}
    for target in targets.itertuples():
        target_key = int(str(target.session_id).rsplit(":", 1)[-1])
        target_manifest = _latest_session_manifest(target_key, manifest_dir)
        if target_manifest is None:
            continue
        try:
            target_frames = _frames_from_manifest(target_manifest)
        except QualifyingPredictionError:
            continue
        target_input = SessionInput(
            target_key,
            (
                "sprint_qualifying"
                if "sprint" in str(target.session_name).casefold()
                else "qualifying"
            ),
            target.scheduled_start_utc,
            target_frames["laps"],
            target_frames["stints"],
            target_frames["drivers"],
            target_frames["weather"],
        )
        target_best, _ = _best_laps(target_input, target_start)
        if target_best.empty:
            continue
        target_weather = _session_weather(target_frames["weather"])
        for row in target_best.itertuples():
            driver_history.setdefault(int(row.driver_number), []).append(
                float(row.session_gap_ratio)
            )
            entries = target_frames["drivers"]
            team = (
                entries[entries["driver_number"].eq(row.driver_number)]["team_name"]
                if not entries.empty
                else pd.Series(dtype="object")
            )
            if not team.empty and isinstance(team.iloc[-1], str):
                team_history.setdefault(team.iloc[-1], []).append(float(row.session_gap_ratio))
        meeting_sessions = sessions[
            sessions["meeting_id"].eq(target.meeting_id)
            & sessions["scheduled_start_utc"].lt(target.scheduled_start_utc)
        ].sort_values("scheduled_start_utc")
        for source in meeting_sessions.itertuples():
            name = str(source.session_name).casefold()
            source_type = (
                "sprint_qualifying"
                if "sprint" in name and ("qualifying" in name or "shootout" in name)
                else "sprint" if name == "sprint" else "practice" if "practice" in name else ""
            )
            if not source_type:
                continue
            source_key = int(str(source.session_id).rsplit(":", 1)[-1])
            source_manifest = _latest_session_manifest(source_key, manifest_dir)
            if source_manifest is None:
                continue
            try:
                source_frames = _frames_from_manifest(source_manifest)
            except QualifyingPredictionError:
                continue
            source_input = SessionInput(
                source_key,
                source_type,
                source.scheduled_start_utc,
                source_frames["laps"],
                source_frames["stints"],
                source_frames["drivers"],
                source_frames["weather"],
            )
            source_best, _ = _best_laps(source_input, target_start)
            if source_best.empty:
                continue
            leader_ratios.setdefault(source_type, []).append(
                float(
                    target_best["lap_duration_seconds"].min()
                    / source_best["lap_duration_seconds"].min()
                )
            )
            merged = source_best.merge(
                target_best[["driver_number", "session_gap_ratio"]],
                on="driver_number",
                suffixes=("_source", "_target"),
            )
            for row in merged.itertuples():
                residual_rows.append(
                    {
                        "session_type": source_type,
                        "compound": row.compound,
                        "tyre_age_bin": _tyre_age_bin(row.tyre_age_laps),
                        "source_regime": row.weather_regime,
                        "target_regime": target_weather["regime"],
                        "temperature_bin": _temperature_bin(
                            row.air_temperature, target_weather["air_temperature"]
                        ),
                        "gap_residual": row.session_gap_ratio_target - row.session_gap_ratio_source,
                    }
                )
    return Calibration(
        pd.DataFrame(residual_rows),
        {driver: float(np.median(values[-5:])) for driver, values in driver_history.items()},
        {team: float(np.median(values[-10:])) for team, values in team_history.items()},
        {kind: tuple(values) for kind, values in leader_ratios.items()},
    )


def build_qualifying_prediction(
    pipeline_manifest: dict[str, Any],
    pipeline_manifest_path: Path,
    *,
    artifact_dir: Path = ARTIFACTS_DIR,
    manifest_dir: Path = CURATED_DATA_DIR / "manifests",
) -> tuple[dict[str, Any], Path]:
    selection = pipeline_manifest.get("selection", {})
    context = pipeline_manifest.get("context") or {}
    target = context.get("target_session") or {}
    if selection.get("purpose") != "qualifying_prediction":
        raise QualifyingPredictionError(
            "Prediction requires a qualifying_prediction pipeline manifest."
        )
    target_session_id = str(target.get("session_id", ""))
    target_session_key = target.get("source_session_key")
    if not target_session_id or target_session_key is None:
        raise QualifyingPredictionError("Prediction manifest has no target session.")
    decision_time = decision_timestamp(selection.get("decision_time"))
    target_start = decision_timestamp(target.get("scheduled_start_utc"))
    target_end = decision_timestamp(target.get("scheduled_end_utc"))
    input_hash = sha256(pipeline_manifest_path)
    identity = json.dumps(
        {
            "target_session_id": target_session_id,
            "decision_time": decision_time.isoformat(),
            "input_manifest_sha256": input_hash,
            "feature_version": FEATURE_VERSION,
            "calculation_version": CALCULATION_VERSION,
        },
        sort_keys=True,
    )
    calculation_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:20]
    snapshot_path = (
        manifest_dir
        / "calculations"
        / f"qualifying_prediction_{int(target_session_key)}_{calculation_id}.json"
    )
    if snapshot_path.is_file():
        try:
            existing = json.loads(snapshot_path.read_text(encoding="utf-8"))
            output = _project_path(str(existing["output_reference"]))
            if (
                existing.get("input_manifest_sha256") == input_hash
                and output.is_file()
                and sha256(output) == existing.get("output_sha256")
            ):
                return existing, snapshot_path
        except (OSError, json.JSONDecodeError, KeyError, TypeError):
            pass
    session_context = {int(row["source_session_key"]): row for row in context.get("sessions", [])}
    session_inputs: list[SessionInput] = []
    input_errors: list[str] = []
    session_manifests = (
        pipeline_manifest.get("jobs", {}).get("openf1_weekend_facts", {}).get("sessions", [])
    )
    for manifest in session_manifests:
        if not isinstance(manifest, dict):
            continue
        key = int(manifest.get("session_key"))
        row = session_context.get(key)
        if row is None:
            continue
        try:
            frames = _frames_from_manifest(manifest)
        except QualifyingPredictionError as exc:
            input_errors.append(f"Session {key}: {exc}")
            continue
        session_inputs.append(
            SessionInput(
                key,
                str(row.get("normalized_session_type", "")),
                decision_timestamp(row.get("scheduled_start_utc")),
                frames["laps"],
                frames["stints"],
                frames["drivers"],
                frames["weather"],
            )
        )
    forecast_result = pipeline_manifest.get("jobs", {}).get("open_meteo", {})
    forecast = pd.DataFrame()
    if (
        isinstance(forecast_result, dict)
        and forecast_result.get("status") in READABLE_STATUSES
        and forecast_result.get("curated_path")
    ):
        try:
            forecast = _read_parquet(
                str(forecast_result["curated_path"]),
                str(forecast_result["curated_sha256"])
                if forecast_result.get("curated_sha256")
                else None,
            )
            forecast = build_weather_cut(
                forecast,
                pd.DataFrame(),
                decision_time=decision_time,
            ).forecast
        except (QualifyingPredictionError, WeatherCutError) as exc:
            input_errors.append(f"Target weather: {exc}")
            forecast = pd.DataFrame()
    try:
        calibration = _local_calibration(int(selection["season"]), target_start, manifest_dir)
    except (QualifyingPredictionError, OSError, ValueError, KeyError, TypeError) as exc:
        calibration = Calibration(pd.DataFrame(), {}, {}, {})
        input_errors.append(f"Local calibration: {exc}")
    result = calculate_prediction(
        session_inputs,
        target_session_id=target_session_id,
        target_start=target_start,
        target_end=target_end,
        decision_time=decision_time,
        forecast=forecast,
        calibration=calibration,
        calculation_id=calculation_id,
    )
    if input_errors:
        diagnostics = dict(result.diagnostics)
        diagnostics["input_errors"] = input_errors
        reasons = list(diagnostics.get("reasons", []))
        reasons.append("At least one independent prediction input was unavailable.")
        diagnostics["reasons"] = reasons
        result = PredictionResult(
            result.rows,
            "partial" if result.status == "available" else result.status,
            diagnostics,
        )
    output_path = (
        artifact_dir
        / "calculations"
        / "qualifying_prediction"
        / f"session={int(target_session_key)}"
        / f"prediction_{calculation_id}.parquet"
    )
    atomic_parquet(result.rows, output_path)
    calculated_at = datetime.now(timezone.utc).isoformat()
    snapshot = {
        "schema_version": 1,
        "calculation_id": calculation_id,
        "calculation_type": "qualifying_prediction",
        "session_id": target_session_id,
        "target_session_key": int(target_session_key),
        "decision_time": decision_time.isoformat(),
        "trigger_id": calculation_id,
        "trigger_type": "manual_pre_session",
        "calculated_at": calculated_at,
        "feature_version": FEATURE_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "input_manifest_path": _relative_path(pipeline_manifest_path),
        "input_manifest_sha256": input_hash,
        "status": result.status,
        "output_reference": _relative_path(output_path),
        "output_sha256": sha256(output_path),
        "row_count": len(result.rows),
        "diagnostics": result.diagnostics,
    }
    atomic_json(snapshot, snapshot_path)
    return snapshot, snapshot_path
