from __future__ import annotations

from datetime import datetime

import pandas as pd


class TemporalCutError(RuntimeError):
    pass


def decision_timestamp(value: str | datetime | pd.Timestamp) -> pd.Timestamp:
    try:
        timestamp = pd.Timestamp(value)
    except (TypeError, ValueError) as exc:
        raise TemporalCutError("decision_time must be a valid timestamp.") from exc
    if pd.isna(timestamp):
        raise TemporalCutError("decision_time must be a valid timestamp.")
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def cut_facts(
    frame: pd.DataFrame,
    *,
    decision_time: str | datetime | pd.Timestamp,
    event_time_column: str = "event_time",
    available_at_column: str = "available_at",
) -> pd.DataFrame:
    cut_time = decision_timestamp(decision_time)
    if frame.empty:
        return frame.copy()
    missing = [
        column
        for column in (event_time_column, available_at_column)
        if column not in frame.columns
    ]
    if missing:
        raise TemporalCutError(
            "Fact data is missing temporal column(s): " + ", ".join(missing) + "."
        )
    event_times = pd.to_datetime(
        frame[event_time_column], format="mixed", utc=True, errors="coerce"
    )
    available_times = pd.to_datetime(
        frame[available_at_column], format="mixed", utc=True, errors="coerce"
    )
    selected = frame[
        event_times.notna()
        & available_times.notna()
        & event_times.le(cut_time)
        & available_times.le(cut_time)
    ].copy()
    selected[event_time_column] = event_times.loc[selected.index]
    selected[available_at_column] = available_times.loc[selected.index]
    return selected.reset_index(drop=True)
