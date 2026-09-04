from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from f1_pipeline.analysis.qualifying_prediction import (
    CALCULATION_VERSION,
    DEFAULT_PARAMETERS,
    FEATURE_VERSION,
    Calibration,
    PredictionParameters,
    SessionInput,
    _best_laps,
    _frames_from_manifest,
    _latest_session_manifest,
    _local_calibration,
    calculate_prediction,
)
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.settings import ARTIFACTS_DIR, CURATED_DATA_DIR, PROJECT_ROOT

BACKTEST_VERSION = "qualifying_walk_forward_v1"


class QualifyingBacktestError(RuntimeError):
    pass


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _manifest_hash(manifest: dict[str, Any]) -> str:
    return hashlib.sha256(
        json.dumps(manifest, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()


def _session_type(name: Any) -> str | None:
    normalized = str(name).casefold()
    if "sprint" in normalized and ("qualifying" in normalized or "shootout" in normalized):
        return "sprint_qualifying"
    if normalized == "sprint":
        return "sprint"
    if "practice" in normalized:
        return "practice"
    return None


def _load_session(row: Any, manifest_dir: Path) -> tuple[SessionInput, dict[str, Any]] | None:
    key = int(str(row.session_id).rsplit(":", 1)[-1])
    manifest = _latest_session_manifest(key, manifest_dir)
    if manifest is None:
        return None
    frames = _frames_from_manifest(manifest)
    kind = _session_type(row.session_name)
    if kind is None and str(row.session_name).casefold() == "qualifying":
        kind = "qualifying"
    if kind is None:
        return None
    return (
        SessionInput(
            key,
            kind,
            row.scheduled_start_utc,
            frames["laps"],
            frames["stints"],
            frames["drivers"],
            frames["weather"],
        ),
        manifest,
    )


def _actual_classification(target: SessionInput, as_of: pd.Timestamp) -> pd.DataFrame:
    best, _ = _best_laps(target, as_of)
    if best.empty:
        return best
    actual = best[["driver_number", "session_gap_ratio", "lap_duration_seconds"]].copy()
    actual = actual.sort_values(["session_gap_ratio", "driver_number"]).reset_index(drop=True)
    actual["actual_position"] = np.arange(1, len(actual) + 1)
    return actual.rename(
        columns={
            "session_gap_ratio": "actual_gap_ratio",
            "lap_duration_seconds": "actual_lap_seconds",
        }
    )


def _score_prediction(
    prediction: pd.DataFrame,
    actual: pd.DataFrame,
    *,
    model: str,
    target_key: int,
    decision_time: pd.Timestamp,
) -> pd.DataFrame:
    scored = prediction.merge(actual, on="driver_number", how="inner")
    if scored.empty:
        return scored
    leader_seconds = float(actual["actual_lap_seconds"].min())
    scored["model"] = model
    scored["target_session_key"] = target_key
    scored["decision_time"] = decision_time
    scored["absolute_position_error"] = (
        scored["predicted_position"].astype(float) - scored["actual_position"]
    ).abs()
    scored["absolute_gap_error_seconds"] = (
        scored["predicted_gap_ratio"] - scored["actual_gap_ratio"]
    ).abs() * leader_seconds
    for cutoff in (5, 10, 15):
        probability = f"top_{cutoff}_probability"
        brier = f"top_{cutoff}_brier"
        actual_top = scored["actual_position"].le(cutoff).astype(float)
        scored[brier] = (scored[probability] - actual_top) ** 2
    correlation = scored["predicted_position"].corr(
        scored["actual_position"], method="spearman"
    )
    scored["spearman"] = correlation
    for cutoff in (5, 10, 15):
        predicted = set(scored.loc[scored["predicted_position"].le(cutoff), "driver_number"])
        observed = set(scored.loc[scored["actual_position"].le(cutoff), "driver_number"])
        scored[f"top_{cutoff}_overlap"] = len(predicted & observed) / min(cutoff, len(observed))
    return scored


def _summary(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return pd.DataFrame()
    event_metrics = (
        rows.groupby(["model", "target_session_key"], as_index=False)
        .agg(
            drivers=("driver_number", "size"),
            position_mae=("absolute_position_error", "mean"),
            gap_mae_seconds=("absolute_gap_error_seconds", "mean"),
            spearman=("spearman", "first"),
            top_5_overlap=("top_5_overlap", "first"),
            top_10_overlap=("top_10_overlap", "first"),
            top_15_overlap=("top_15_overlap", "first"),
            top_5_brier=("top_5_brier", "mean"),
            top_10_brier=("top_10_brier", "mean"),
            top_15_brier=("top_15_brier", "mean"),
        )
    )
    return (
        event_metrics.groupby("model", as_index=False)
        .agg(
            events=("target_session_key", "nunique"),
            drivers=("drivers", "sum"),
            position_mae=("position_mae", "mean"),
            gap_mae_seconds=("gap_mae_seconds", "mean"),
            mean_spearman=("spearman", "mean"),
            top_5_overlap=("top_5_overlap", "mean"),
            top_10_overlap=("top_10_overlap", "mean"),
            top_15_overlap=("top_15_overlap", "mean"),
            top_5_brier=("top_5_brier", "mean"),
            top_10_brier=("top_10_brier", "mean"),
            top_15_brier=("top_15_brier", "mean"),
        )
        .sort_values(["position_mae", "gap_mae_seconds"])
        .reset_index(drop=True)
    )


def build_backtest(
    season: int,
    *,
    as_of: pd.Timestamp,
    dimension_dir: Path = CURATED_DATA_DIR / "dimensions",
    manifest_dir: Path = CURATED_DATA_DIR / "manifests",
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    session_path = dimension_dir / f"season={season}" / "session.parquet"
    if not session_path.is_file():
        raise QualifyingBacktestError(f"Session dimension is missing: {session_path}")
    sessions = pd.read_parquet(session_path).copy()
    sessions["scheduled_start_utc"] = pd.to_datetime(
        sessions["scheduled_start_utc"], utc=True, errors="coerce"
    )
    sessions["scheduled_end_utc"] = pd.to_datetime(
        sessions["scheduled_end_utc"], utc=True, errors="coerce"
    )
    completed = sessions["scheduled_end_utc"].lt(as_of)
    targets = sessions[
        completed & sessions["session_name"].astype(str).str.casefold().eq("qualifying")
    ].sort_values("scheduled_start_utc")
    variants = {
        "equal_sessions": PredictionParameters(
            practice_decay=1.0,
            sprint_qualifying_weight=1.0,
            sprint_weight=1.0,
        ),
        "decay_0_50_calibrated": PredictionParameters(practice_decay=0.5),
        "current_weights": DEFAULT_PARAMETERS,
        "current_calibrated": DEFAULT_PARAMETERS,
        "no_sprint_race_calibrated": PredictionParameters(sprint_weight=0.0),
    }
    rows: list[pd.DataFrame] = []
    skipped: list[dict[str, Any]] = []
    input_manifests: dict[int, str] = {}
    for input_row in sessions[completed].itertuples():
        if (
            _session_type(input_row.session_name) is None
            and str(input_row.session_name).casefold() != "qualifying"
        ):
            continue
        input_key = int(str(input_row.session_id).rsplit(":", 1)[-1])
        input_manifest = _latest_session_manifest(input_key, manifest_dir)
        if input_manifest is not None:
            input_manifests[input_key] = _manifest_hash(input_manifest)
    empty_calibration = Calibration(pd.DataFrame(), {}, {}, {})
    for target_row in targets.itertuples():
        target_loaded = _load_session(target_row, manifest_dir)
        target_key = int(str(target_row.session_id).rsplit(":", 1)[-1])
        if target_loaded is None:
            skipped.append({"target_session_key": target_key, "reason": "target manifest unavailable"})
            continue
        target, target_manifest = target_loaded
        input_manifests[target_key] = _manifest_hash(target_manifest)
        actual = _actual_classification(target, as_of)
        if actual.empty:
            skipped.append({"target_session_key": target_key, "reason": "target laps unavailable"})
            continue
        source_rows = sessions[
            sessions["meeting_id"].eq(target_row.meeting_id)
            & sessions["scheduled_start_utc"].lt(target_row.scheduled_start_utc)
        ].sort_values("scheduled_start_utc")
        source_inputs: list[SessionInput] = []
        for source_row in source_rows.itertuples():
            if _session_type(source_row.session_name) is None:
                continue
            loaded = _load_session(source_row, manifest_dir)
            if loaded is None:
                continue
            source, source_manifest = loaded
            source_inputs.append(source)
            input_manifests[source.session_key] = _manifest_hash(source_manifest)
        one_lap = [
            source
            for source in source_inputs
            if source.session_type in {"practice", "sprint_qualifying"}
        ]
        if not one_lap:
            skipped.append({"target_session_key": target_key, "reason": "source sessions unavailable"})
            continue
        latest = max(one_lap, key=lambda session: session.scheduled_start)
        decision_time = target_row.scheduled_start_utc
        latest_prediction = calculate_prediction(
            [latest],
            target_session_id=str(target_row.session_id),
            target_start=target_row.scheduled_start_utc,
            target_end=target_row.scheduled_end_utc,
            decision_time=decision_time,
            forecast=pd.DataFrame(),
            calibration=empty_calibration,
            calculation_id=hashlib.sha256(f"latest:{target_key}".encode()).hexdigest()[:20],
        )
        rows.append(
            _score_prediction(
                latest_prediction.rows,
                actual,
                model="latest_one_lap",
                target_key=target_key,
                decision_time=decision_time,
            )
        )
        calibration = _local_calibration(season, decision_time, manifest_dir)
        for name, parameters in variants.items():
            selected_calibration = calibration if name.endswith("calibrated") else empty_calibration
            prediction = calculate_prediction(
                source_inputs,
                target_session_id=str(target_row.session_id),
                target_start=target_row.scheduled_start_utc,
                target_end=target_row.scheduled_end_utc,
                decision_time=decision_time,
                forecast=pd.DataFrame(),
                calibration=selected_calibration,
                calculation_id=hashlib.sha256(f"{name}:{target_key}".encode()).hexdigest()[:20],
                parameters=parameters,
            )
            rows.append(
                _score_prediction(
                    prediction.rows,
                    actual,
                    model=name,
                    target_key=target_key,
                    decision_time=decision_time,
                )
            )
    scored = pd.concat([frame for frame in rows if not frame.empty], ignore_index=True) if rows else pd.DataFrame()
    summary = _summary(scored)
    diagnostics = {
        "target_sessions_considered": len(targets),
        "target_sessions_scored": int(scored["target_session_key"].nunique()) if not scored.empty else 0,
        "skipped": skipped,
        "target_forecast_policy": "not evaluated because point-in-time forecast coverage is incomplete",
        "input_manifests": [
            {"session_key": key, "manifest_content_sha256": value}
            for key, value in sorted(input_manifests.items())
        ],
        "variants": {name: asdict(parameters) for name, parameters in variants.items()},
    }
    return scored, summary, diagnostics


def write_backtest(
    season: int,
    *,
    as_of: pd.Timestamp,
    artifact_dir: Path = ARTIFACTS_DIR,
    manifest_dir: Path = CURATED_DATA_DIR / "manifests",
) -> dict[str, Path]:
    scored, summary, diagnostics = build_backtest(
        season,
        as_of=as_of,
        manifest_dir=manifest_dir,
    )
    if scored.empty:
        raise QualifyingBacktestError("No qualifying session could be scored.")
    session_path = CURATED_DATA_DIR / "dimensions" / f"season={season}" / "session.parquet"
    identity_payload = {
        "season": season,
        "as_of": as_of.isoformat(),
        "session_dimension_sha256": sha256(session_path),
        "input_manifests": diagnostics["input_manifests"],
        "feature_version": FEATURE_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "backtest_version": BACKTEST_VERSION,
    }
    input_hash = hashlib.sha256(
        json.dumps(identity_payload, sort_keys=True).encode("utf-8")
    ).hexdigest()
    calculation_id = input_hash[:20]
    output_dir = artifact_dir / "calculations" / "qualifying_backtest" / f"season={season}"
    rows_path = output_dir / f"rows_{calculation_id}.parquet"
    summary_path = output_dir / f"summary_{calculation_id}.parquet"
    snapshot_path = (
        manifest_dir / "calculations" / f"qualifying_backtest_{season}_{calculation_id}.json"
    )
    atomic_parquet(scored, rows_path)
    atomic_parquet(summary, summary_path)
    snapshot = {
        "schema_version": 1,
        "calculation_id": calculation_id,
        "calculation_type": "qualifying_backtest",
        "season": season,
        "decision_time_policy": "target qualifying scheduled_start_utc",
        "as_of": as_of.isoformat(),
        "trigger_id": calculation_id,
        "trigger_type": "manual_backtest",
        "calculated_at": datetime.now(timezone.utc).isoformat(),
        "feature_version": FEATURE_VERSION,
        "calculation_version": CALCULATION_VERSION,
        "backtest_version": BACKTEST_VERSION,
        "input_reference": _relative_path(session_path),
        "input_hash": input_hash,
        "status": "partial" if diagnostics["skipped"] else "available",
        "output_reference": _relative_path(rows_path),
        "output_sha256": sha256(rows_path),
        "summary_reference": _relative_path(summary_path),
        "summary_sha256": sha256(summary_path),
        "row_count": len(scored),
        "diagnostics": diagnostics,
        "summary": summary.to_dict(orient="records"),
    }
    atomic_json(snapshot, snapshot_path)
    return {"rows": rows_path, "summary": summary_path, "snapshot": snapshot_path}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Backtest the qualifying prediction baseline.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--as-of", type=pd.Timestamp, default=pd.Timestamp.now(tz="UTC"))
    args = parser.parse_args(argv)
    as_of = args.as_of.tz_localize("UTC") if args.as_of.tzinfo is None else args.as_of.tz_convert("UTC")
    try:
        outputs = write_backtest(args.season, as_of=as_of)
    except (OSError, ValueError, KeyError, QualifyingBacktestError) as exc:
        print(f"Qualifying backtest could not be built: {exc}")
        return 1
    summary = pd.read_parquet(outputs["summary"])
    print(summary.to_string(index=False))
    for name, path in outputs.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
