"""Build a first reproducible pace and source cross-check analysis."""

from __future__ import annotations

import argparse
import json
import math
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.data_validation import validate_frame
from f1_pipeline.settings import ARTIFACTS_DIR, PROJECT_ROOT, RAW_DATA_DIR

SESSION_KEY = 11342
OPENF1_LAPS_PATH = RAW_DATA_DIR / f"openf1_{SESSION_KEY}_laps.parquet"
OPENF1_STINTS_PATH = RAW_DATA_DIR / f"openf1_{SESSION_KEY}_stints.parquet"
FASTF1_LAPS_PATH = RAW_DATA_DIR / "fastf1_hungary_2026_race_laps.parquet"
OUTPUT_DIR = ARTIFACTS_DIR / "analysis"


class PaceAnalysisError(RuntimeError):
    """Describe an invalid or insufficient analysis input."""


def _duration_seconds(values: pd.Series) -> pd.Series:
    if pd.api.types.is_timedelta64_dtype(values):
        return values.dt.total_seconds()
    return pd.to_numeric(values, errors="coerce")


def _validate_openf1_laps(laps: pd.DataFrame) -> None:
    validate_frame(
        laps,
        name="OpenF1 laps analysis input",
        required_columns={
            "session_key",
            "driver_number",
            "lap_number",
            "date_start",
            "lap_duration",
        },
        key_columns=("session_key", "driver_number", "lap_number"),
        datetime_columns=("date_start",),
        numeric_columns=("session_key", "driver_number", "lap_number", "lap_duration"),
        required_non_null=("session_key", "driver_number", "lap_number", "date_start"),
        expected_session_key=SESSION_KEY,
    )


def _validate_openf1_stints(stints: pd.DataFrame) -> None:
    validate_frame(
        stints,
        name="OpenF1 stints analysis input",
        required_columns={
            "session_key",
            "driver_number",
            "stint_number",
            "lap_start",
            "lap_end",
            "compound",
        },
        key_columns=("session_key", "driver_number", "stint_number"),
        numeric_columns=(
            "session_key",
            "driver_number",
            "stint_number",
            "lap_start",
            "lap_end",
        ),
        required_non_null=("session_key", "driver_number", "stint_number", "lap_start", "lap_end"),
        expected_session_key=SESSION_KEY,
    )


def _validate_fastf1_laps(laps: pd.DataFrame) -> None:
    validate_frame(
        laps,
        name="FastF1 laps analysis input",
        required_columns={"Driver", "DriverNumber", "LapNumber", "LapTime"},
        required_non_null=("Driver", "DriverNumber", "LapNumber"),
    )


def load_inputs(
    *,
    openf1_laps_path: Path = OPENF1_LAPS_PATH,
    openf1_stints_path: Path = OPENF1_STINTS_PATH,
    fastf1_laps_path: Path = FASTF1_LAPS_PATH,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    try:
        laps = pd.read_parquet(openf1_laps_path)
        stints = pd.read_parquet(openf1_stints_path)
        fastf1_laps = pd.read_parquet(fastf1_laps_path)
    except (OSError, ValueError) as exc:
        raise PaceAnalysisError(f"Could not read analysis input: {exc}") from exc
    _validate_openf1_laps(laps)
    _validate_openf1_stints(stints)
    _validate_fastf1_laps(fastf1_laps)
    return laps, stints, fastf1_laps


def build_openf1_pace_by_stint(
    laps: pd.DataFrame, stints: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, int]]:
    _validate_openf1_laps(laps)
    _validate_openf1_stints(stints)
    if stints.empty:
        raise PaceAnalysisError("OpenF1 stints are required for pace-by-stint analysis.")

    lap_data = laps.copy()
    lap_data["lap_duration_seconds"] = _duration_seconds(lap_data["lap_duration"])
    valid_laps = lap_data[
        lap_data["lap_duration_seconds"].notna()
        & lap_data["lap_duration_seconds"].map(math.isfinite)
        & lap_data["lap_duration_seconds"].gt(0)
    ].copy()
    valid_laps["lap_number"] = pd.to_numeric(valid_laps["lap_number"], errors="coerce")
    stint_data = stints.copy()
    for column in ("lap_start", "lap_end"):
        stint_data[column] = pd.to_numeric(stint_data[column], errors="coerce")

    merged = valid_laps.merge(
        stint_data,
        on=["session_key", "driver_number"],
        how="left",
        suffixes=("", "_stint"),
    )
    assigned = merged[
        merged["stint_number"].notna()
        & merged["lap_number"].ge(merged["lap_start"])
        & merged["lap_number"].le(merged["lap_end"])
    ].copy()
    assigned = assigned.sort_values(
        ["session_key", "driver_number", "lap_number", "stint_number"]
    ).drop_duplicates(
        ["session_key", "driver_number", "lap_number"], keep="first"
    )
    pace = (
        assigned.groupby(
            ["session_key", "driver_number", "stint_number"],
            as_index=False,
            dropna=False,
        )
        .agg(
            lap_start=("lap_start", "first"),
            lap_end=("lap_end", "first"),
            compound=("compound", "first"),
            tyre_age_at_start=("tyre_age_at_start", "first"),
            measured_laps=("lap_number", "count"),
            best_lap_seconds=("lap_duration_seconds", "min"),
            median_lap_seconds=("lap_duration_seconds", "median"),
            mean_lap_seconds=("lap_duration_seconds", "mean"),
        )
        .sort_values(["driver_number", "stint_number"])
        .reset_index(drop=True)
    )
    pace.insert(0, "source_system", "openf1")
    diagnostics = {
        "openf1_lap_rows": len(laps),
        "openf1_valid_lap_rows": len(valid_laps),
        "openf1_stint_rows": len(stints),
        "openf1_assigned_lap_rows": len(assigned),
        "openf1_unassigned_valid_lap_rows": len(valid_laps) - len(assigned),
    }
    return pace, diagnostics


def _openf1_driver_summary(laps: pd.DataFrame) -> pd.DataFrame:
    data = laps.copy()
    data["lap_seconds"] = _duration_seconds(data["lap_duration"])
    data = data[data["lap_seconds"].notna() & data["lap_seconds"].gt(0)]
    return (
        data.groupby("driver_number", as_index=False)
        .agg(
            openf1_lap_count=("lap_number", "count"),
            openf1_best_lap_seconds=("lap_seconds", "min"),
            openf1_median_lap_seconds=("lap_seconds", "median"),
        )
        .assign(driver_number=lambda frame: pd.to_numeric(frame["driver_number"], errors="coerce"))
    )


def _fastf1_driver_summary(laps: pd.DataFrame) -> pd.DataFrame:
    data = laps.copy()
    data["driver_number"] = pd.to_numeric(data["DriverNumber"], errors="coerce")
    data["lap_seconds"] = _duration_seconds(data["LapTime"])
    valid = data["lap_seconds"].notna() & data["lap_seconds"].gt(0)
    if "Deleted" in data.columns:
        valid &= ~data["Deleted"].fillna(False).astype(bool)
    data = data[valid & data["driver_number"].notna()]
    return (
        data.groupby("driver_number", as_index=False)
        .agg(
            fastf1_lap_count=("LapNumber", "count"),
            fastf1_best_lap_seconds=("lap_seconds", "min"),
            fastf1_median_lap_seconds=("lap_seconds", "median"),
        )
    )


def build_source_pace_comparison(
    openf1_laps: pd.DataFrame, fastf1_laps: pd.DataFrame
) -> pd.DataFrame:
    _validate_openf1_laps(openf1_laps)
    _validate_fastf1_laps(fastf1_laps)
    comparison = _openf1_driver_summary(openf1_laps).merge(
        _fastf1_driver_summary(fastf1_laps),
        on="driver_number",
        how="outer",
    )
    comparison["median_delta_seconds"] = (
        comparison["fastf1_median_lap_seconds"]
        - comparison["openf1_median_lap_seconds"]
    )
    comparison["absolute_median_delta_seconds"] = comparison[
        "median_delta_seconds"
    ].abs()
    comparison["comparison_status"] = "comparable"
    comparison.loc[
        comparison["openf1_lap_count"].isna(), "comparison_status"
    ] = "fastf1_only"
    comparison.loc[
        comparison["fastf1_lap_count"].isna(), "comparison_status"
    ] = "openf1_only"
    comparison.insert(0, "comparison_type", "driver_level_summary")
    return comparison.sort_values("driver_number").reset_index(drop=True)


def _relative_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _atomic_parquet(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=f".{path.name}.", suffix=".parquet", delete=False
        ) as temporary:
            temporary_path = Path(temporary.name)
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


def write_analysis(
    *,
    session_key: int = SESSION_KEY,
    openf1_laps_path: Path = OPENF1_LAPS_PATH,
    openf1_stints_path: Path = OPENF1_STINTS_PATH,
    fastf1_laps_path: Path = FASTF1_LAPS_PATH,
    output_dir: Path = OUTPUT_DIR,
) -> dict[str, Path]:
    if session_key != SESSION_KEY:
        raise PaceAnalysisError(f"This first analysis supports session {SESSION_KEY} only.")
    laps, stints, fastf1_laps = load_inputs(
        openf1_laps_path=openf1_laps_path,
        openf1_stints_path=openf1_stints_path,
        fastf1_laps_path=fastf1_laps_path,
    )
    pace, diagnostics = build_openf1_pace_by_stint(laps, stints)
    comparison = build_source_pace_comparison(laps, fastf1_laps)
    output_dir.mkdir(parents=True, exist_ok=True)
    pace_path = output_dir / f"hungary_{session_key}_openf1_pace_by_stint.parquet"
    comparison_path = output_dir / f"hungary_{session_key}_source_pace_comparison.parquet"
    metadata_path = output_dir / f"hungary_{session_key}_pace_analysis.json"
    _atomic_parquet(pace, pace_path)
    _atomic_parquet(comparison, comparison_path)
    metadata: dict[str, Any] = {
        "schema_version": 1,
        "analysis": "pace_by_stint_and_source_comparison",
        "session_key": session_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source_policy": {
            "openf1": "primary observed laps and stints for pace analysis",
            "fastf1": "separate historical cross-check aggregated by driver",
            "deduplication": "sources are not unioned or double-counted",
        },
        "inputs": {
            "openf1_laps": _relative_path(openf1_laps_path),
            "openf1_stints": _relative_path(openf1_stints_path),
            "fastf1_laps": _relative_path(fastf1_laps_path),
        },
        "outputs": {
            "pace_by_stint": _relative_path(pace_path),
            "source_comparison": _relative_path(comparison_path),
        },
        "row_counts": {
            "pace_by_stint": len(pace),
            "source_comparison": len(comparison),
        },
        "diagnostics": diagnostics,
    }
    _atomic_json(metadata, metadata_path)
    return {
        "pace_by_stint": pace_path,
        "source_comparison": comparison_path,
        "metadata": metadata_path,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build the first Hungary pace analysis.")
    parser.add_argument("--session-key", type=int, default=SESSION_KEY)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args(argv)
    try:
        outputs = write_analysis(session_key=args.session_key, output_dir=args.output_dir)
    except (OSError, PaceAnalysisError, ValueError) as exc:
        print(f"Pace analysis could not be built: {exc}")
        return 1
    for name, path in outputs.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

