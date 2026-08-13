from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import pandas as pd

from f1_pipeline.sources.open_meteo import utc_timestamp


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
    cut_time = utc_timestamp(decision_time, "decision_time")
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
        event_times = _timestamps(observations, "event_time")
        available_times = _timestamps(observations, "available_at")
        selected_observations = observations[
            event_times.notna()
            & available_times.notna()
            & event_times.le(cut_time)
            & available_times.le(cut_time)
        ].copy()
        selected_observations["event_time"] = event_times.loc[
            selected_observations.index
        ]
        selected_observations["available_at"] = available_times.loc[
            selected_observations.index
        ]
        selected_observations = selected_observations.sort_values(
            ["event_time", "available_at"]
        ).reset_index(drop=True)

    available_parts = int(not selected_forecast.empty) + int(
        not selected_observations.empty
    )
    status = "available" if available_parts == 2 else "partial" if available_parts else "unavailable"
    return WeatherCut(cut_time, selected_forecast, selected_observations, status)

 