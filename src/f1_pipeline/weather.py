from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from f1_pipeline.temporal import TemporalCutError, cut_facts, decision_timestamp


class WeatherCutError(RuntimeError):
    pass


@dataclass(frozen=True)
class WeatherCut:
    decision_time: pd.Timestamp
    forecast: pd.DataFrame
    observations: pd.DataFrame
    status: str


def _timestamps(frame: pd.DataFrame, column: str) -> pd.Series:
    if column not in frame.columns:
        raise WeatherCutError(f"Weather data is missing column '{column}'.")
    return pd.to_datetime(frame[column], utc=True, errors="coerce")


def build_weather_cut(
    forecasts: pd.DataFrame,
    observations: pd.DataFrame,
    *,
    decision_time: str | datetime | pd.Timestamp,
) -> WeatherCut:
    try:
        cut_time = decision_timestamp(decision_time)
    except TemporalCutError as exc:
        raise WeatherCutError(str(exc)) from exc
    selected_forecast = forecasts.iloc[0:0].copy()
    if not forecasts.empty:
        for column in ("snapshot_id", "available_at", "run_initialized_at", "valid_time"):
            if column not in forecasts.columns:
                raise WeatherCutError(f"Forecast data is missing column '{column}'.")
        candidates = forecasts.copy()
        candidates["_available_at"] = _timestamps(candidates, "available_at")
        candidates["_run_initialized_at"] = _timestamps(
            candidates, "run_initialized_at"
        )
        candidates = candidates[
            candidates["_available_at"].notna()
            & candidates["_run_initialized_at"].notna()
            & candidates["_available_at"].le(cut_time)
        ]
        if not candidates.empty:
            snapshot = (
                candidates[
                    ["snapshot_id", "_available_at", "_run_initialized_at"]
                ]
                .drop_duplicates()
                .sort_values(
                    ["_available_at", "_run_initialized_at", "snapshot_id"]
                )
                .iloc[-1]["snapshot_id"]
            )
            selected_forecast = forecasts[forecasts["snapshot_id"].eq(snapshot)].copy()
            selected_forecast["valid_time"] = _timestamps(
                selected_forecast, "valid_time"
            )
            selected_forecast = selected_forecast.sort_values("valid_time").reset_index(
                drop=True
            )

    selected_observations = observations.iloc[0:0].copy()
    if not observations.empty:
        try:
            selected_observations = cut_facts(
                observations, decision_time=cut_time
            )
        except TemporalCutError as exc:
            raise WeatherCutError(str(exc)) from exc
        selected_observations = selected_observations.sort_values(
            ["event_time", "available_at"]
        ).reset_index(drop=True)

    available_parts = int(not selected_forecast.empty) + int(
        not selected_observations.empty
    )
    status = "available" if available_parts == 2 else "partial" if available_parts else "unavailable"
    return WeatherCut(cut_time, selected_forecast, selected_observations, status)

 