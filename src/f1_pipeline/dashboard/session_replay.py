from __future__ import annotations

import math
from typing import Any

import pandas as pd
import streamlit as st

from f1_pipeline.dashboard.pit_loss import PitLossConfigError, load_pit_loss
from f1_pipeline.dashboard.read_models import (
    DashboardDataError,
    SeasonCatalog,
    load_forecast,
    load_session_bundle,
    source_meeting_key,
)
from f1_pipeline.dashboard.re_live_component import render_re_live
from f1_pipeline.dashboard.replay_service import build_replay_view
from f1_pipeline.temporal import TemporalCutError, cut_facts

REPLAY_CACHE_VERSION = 3


@st.cache_data(show_spinner="Building historical replay…", max_entries=8)
def replay_for(
        session_key: int,
        season: int,
        meeting_key: int,
        circuit_id: str,
        focus_driver: int,
        decision_time: str,
        pit_loss_seconds: float | None,
        cache_version: int,
):
    if cache_version != REPLAY_CACHE_VERSION:
        raise ValueError("Unsupported replay cache version.")
    return build_replay_view(
        session_key,
        season=season,
        meeting_key=meeting_key,
        circuit_id=circuit_id,
        focus_driver=focus_driver,
        decision_time=decision_time,
        frame_seconds=4,
        pit_loss_seconds=pit_loss_seconds,
    )


@st.cache_data(show_spinner=False, max_entries=64)
def session_silver(session_key: int, endpoint: str) -> pd.DataFrame:
    try:
        return load_session_bundle(session_key, (endpoint,)).frames[endpoint]
    except DashboardDataError:
        return pd.DataFrame()


@st.cache_data(show_spinner=False, max_entries=16)
def forecast_for(session_key: int) -> pd.DataFrame:
    return load_forecast(session_key)


def _timestamp(value: Any) -> str | None:
    timestamp = pd.to_datetime(value, utc=True, errors="coerce")
    if not isinstance(timestamp, pd.Timestamp) or pd.isna(timestamp):
        return None
    return timestamp.isoformat()


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _text(value: Any) -> str:
    return "" if value is None or pd.isna(value) else str(value)


def _cut(frame: pd.DataFrame, decision_time: pd.Timestamp) -> pd.DataFrame:
    if frame.empty:
        return frame
    try:
        return cut_facts(frame, decision_time=decision_time)
    except TemporalCutError:
        return frame.iloc[0:0]


def _weather_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        event_time = _timestamp(record.get("event_time"))
        if event_time is None:
            continue
        rows.append(
            {
                "event_time": event_time,
                "available_at": _timestamp(record.get("available_at")) or event_time,
                "air_temperature": _number(record.get("air_temperature")),
                "track_temperature": _number(record.get("track_temperature")),
                "humidity": _number(record.get("humidity")),
                "pressure": _number(record.get("pressure")),
                "rainfall": _number(record.get("rainfall")),
                "wind_speed": _number(record.get("wind_speed")),
                "wind_direction": _number(record.get("wind_direction")),
            }
        )
    return rows


def _forecast_records(
    frame: pd.DataFrame,
    decision_time: pd.Timestamp,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        available_at = pd.to_datetime(
            record.get("available_at"), utc=True, errors="coerce"
        )
        valid_time = _timestamp(record.get("valid_time"))
        if (
            not isinstance(available_at, pd.Timestamp)
            or pd.isna(available_at)
            or available_at > decision_time
            or valid_time is None
        ):
            continue
        rows.append(
            {
                "snapshot_id": _text(record.get("snapshot_id")),
                "available_at": available_at.isoformat(),
                "run_initialized_at": _timestamp(record.get("run_initialized_at")),
                "valid_time": valid_time,
                "temperature": _number(record.get("temperature_2m")),
                "precipitation": _number(record.get("precipitation")),
                "rain": _number(record.get("rain")),
                "wind_speed": _number(record.get("wind_speed_10m")),
                "wind_direction": _number(record.get("wind_direction_10m")),
            }
        )
    return rows


def _race_control_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        event_time = _timestamp(record.get("event_time"))
        if event_time is None:
            continue
        available_at = _timestamp(record.get("available_at")) or event_time
        rows.append(
            {
                "event_time": event_time,
                "available_at_ms": pd.Timestamp(available_at).timestamp() * 1000,
                "lap_number": _number(record.get("lap_number")),
                "category": _text(record.get("category")),
                "flag": _text(record.get("flag")),
                "message": _text(record.get("message")),
            }
        )
    return rows


def _pit_records(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for record in frame.to_dict(orient="records"):
        event_time = _timestamp(record.get("event_time"))
        if event_time is None:
            continue
        pit_duration = _number(record.get("pit_duration_seconds"))
        lane_duration = _number(record.get("lane_duration_seconds"))
        interval_duration = lane_duration if lane_duration is not None else pit_duration
        entry_time = (
            (
                pd.Timestamp(event_time)
                - pd.to_timedelta(interval_duration, unit="s")
            ).isoformat()
            if interval_duration is not None and interval_duration >= 0
            else None
        )
        rows.append(
            {
                "event_time": event_time,
                "available_at": _timestamp(record.get("available_at")) or event_time,
                "entry_time": entry_time,
                "exit_time": event_time,
                "driver_number": _number(record.get("driver_number")),
                "lap_number": _number(record.get("lap_number")),
                "pit_duration": pit_duration,
                "lane_duration": lane_duration,
                "stop_duration": _number(record.get("stop_duration_seconds")),
            }
        )
    return rows


def _replay_styles() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stHeader"] { display: none; }
        .stMainBlockContainer { max-width: none; padding: 0.35rem 0.5rem 0; }
        [data-testid="stAppViewBlockContainer"] { max-width: none; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _return_to_dashboard() -> None:
    st.session_state["view"] = "dashboard"
    for key in ("replay_session_key", "replay_focus_driver", "replay_job_id"):
        st.session_state.pop(key, None)


def render_session_replay(catalog: SeasonCatalog, session_key: int) -> None:
    _replay_styles()
    session_id = f"openf1:session:{session_key}"
    matches = catalog.sessions[catalog.sessions["session_id"].eq(session_id)]
    if matches.empty:
        st.error("The selected session is not part of this season.")
        return
    selected = matches.iloc[0]
    meeting_rows = catalog.meetings[
        catalog.meetings["meeting_id"].eq(selected["meeting_id"])
    ]
    if meeting_rows.empty:
        st.error("The race weekend for this session is unavailable.")
        return
    meeting = meeting_rows.iloc[0]
    circuit_rows = catalog.circuits[
        catalog.circuits["circuit_id"].eq(meeting["circuit_id"])
    ]
    country_rows = catalog.countries[
        catalog.countries["country_id"].eq(meeting["country_id"])
    ]
    circuit_name = (
        str(circuit_rows.iloc[0].get("circuit_name") or "")
        if not circuit_rows.empty
        else ""
    )
    country_name = (
        str(country_rows.iloc[0].get("country_name") or "")
        if not country_rows.empty
        else ""
    )
    decision_time = pd.to_datetime(
        selected["scheduled_end_utc"], utc=True, errors="coerce"
    )
    if not isinstance(decision_time, pd.Timestamp) or pd.isna(decision_time):
        st.error("The session end time is unavailable.")
        return
    focus_driver = st.session_state.get("replay_focus_driver")
    if focus_driver is None:
        st.error("No focus driver was selected for Re-Live.")
        return
    try:
        pit_loss = load_pit_loss(
            meeting.get("meeting_name"),
            meeting.get("location"),
            circuit_name,
        )
    except PitLossConfigError as exc:
        st.warning(str(exc))
        pit_loss = None
    try:
        view = replay_for(
            session_key,
            catalog.season,
            source_meeting_key(selected["meeting_id"]),
            str(meeting["circuit_id"]),
            int(focus_driver),
            decision_time.isoformat(),
            pit_loss.seconds if pit_loss is not None else None,
            REPLAY_CACHE_VERSION,
        )
    except (DashboardDataError, ValueError, OSError) as exc:
        st.warning(f"Replay data is incomplete: {exc}")
        st.button("Back to dashboard", on_click=_return_to_dashboard)
        return
    observations = _cut(session_silver(session_key, "weather"), decision_time)
    race_control = _cut(session_silver(session_key, "race_control"), decision_time)
    pits = _cut(session_silver(session_key, "pit"), decision_time)
    forecasts = forecast_for(session_key)
    payload = {
        **view.payload,
        "meeting": {
            "name": str(meeting.get("meeting_name") or "Race Re-Live"),
            "location": str(meeting.get("location") or country_name),
            "circuit": circuit_name,
        },
        "session": {
            "name": str(selected.get("session_name") or "Race"),
            "start": _timestamp(selected.get("scheduled_start_utc")),
            "end": decision_time.isoformat(),
        },
        "weather_observations": _weather_records(observations),
        "forecasts": _forecast_records(forecasts, decision_time),
        "race_control": _race_control_records(race_control),
        "pits": _pit_records(pits),
        "availability": {
            "weather": "available" if not observations.empty else "unavailable",
            "forecast": "available" if not forecasts.empty else "unavailable",
            "race_control": "available" if not race_control.empty else "unavailable",
            "pits": "available" if not pits.empty else "unavailable",
        },
        "pit_loss_source": pit_loss.source_asset if pit_loss is not None else None,
    }
    render_re_live(
        payload,
        key=f"re_live_{session_key}_{int(focus_driver)}_v{REPLAY_CACHE_VERSION}",
        on_back=_return_to_dashboard,
    )
