from __future__ import annotations

import math
import os
from typing import Any, cast

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from f1_pipeline.dashboard.job_client import JobServiceError, get_job
from f1_pipeline.dashboard.read_models import (
    DashboardDataError,
    SeasonCatalog,
    load_forecast,
    load_session_bundle,
    source_meeting_key,
)
from f1_pipeline.dashboard.replay_service import build_replay_view
from f1_pipeline.temporal import TemporalCutError, cut_facts
from f1_pipeline.weather import WeatherCutError, build_weather_cut

JOB_SERVICE_URL = os.getenv("F1_JOB_SERVICE_URL", "http://127.0.0.1:8765")


@st.cache_data(show_spinner="Building historical replay…")
def replay_for(
        session_key: int,
        season: int,
        meeting_key: int,
        circuit_id: str,
        focus_driver: int | None,
        decision_time: str,
):
    return build_replay_view(
        session_key,
        season=season,
        meeting_key=meeting_key,
        circuit_id=circuit_id,
        focus_driver=focus_driver,
        decision_time=decision_time,
        frame_seconds=4,
    )


@st.cache_data(show_spinner=False)
def session_silver(session_key: int, endpoint: str) -> pd.DataFrame:
    try:
        return load_session_bundle(session_key, (endpoint,)).frames[endpoint]
    except DashboardDataError:
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def forecast_for(session_key: int) -> pd.DataFrame:
    return load_forecast(session_key)


def show_replay_job_status() -> None:
    identifier = st.session_state.get("replay_job_id")
    if not identifier:
        return
    try:
        status = get_job(JOB_SERVICE_URL, identifier)
    except JobServiceError as exc:
        st.warning(str(exc))
        return
    state = str(status.get("status", "unknown"))
    if state in {"available", "stale"}:
        st.success(f"Replay data status: {state}")
    elif state == "partial":
        st.warning("Replay data loaded with partial source availability.")
    elif state == "unavailable":
        st.error(f"Replay data load failed: {status.get('error', 'data unavailable')}")
    else:
        st.info(f"Replay data status: {state}")


def weather_panel(
        session_key: int,
        decision_time: pd.Timestamp,
        observations: pd.DataFrame,
) -> None:
    try:
        weather = build_weather_cut(
            forecast_for(session_key),
            observations,
            decision_time=decision_time,
        )
    except WeatherCutError as exc:
        st.warning(str(exc))
        return
    st.caption(f"Point-in-time weather status: {weather.status}")
    if weather.forecast.empty:
        st.info("No forecast was available at the selected decision time.")
    else:
        display = weather.forecast.copy()
        display["valid_time"] = pd.to_datetime(
            display["valid_time"], utc=True, errors="coerce"
        )
        columns = [
            column
            for column in ("temperature_2m", "precipitation", "rain", "wind_speed_10m")
            if column in display
        ]
        for column in columns:
            display[column] = pd.to_numeric(display[column], errors="coerce").astype(float)
        display = display.dropna(subset=["valid_time"])
        if columns and not display.dropna(how="all", subset=columns).empty:
            st.line_chart(display, x="valid_time", y=columns)
    if weather.observations.empty:
        st.info("No track weather observations are available for this time.")
        return
    latest = weather.observations.iloc[-1]

    def value(name: str, precision: int, suffix: str = "") -> str:
        try:
            number = float(cast(Any, latest.get(name)))
        except (TypeError, ValueError):
            return "Unavailable"
        return f"{number:.{precision}f}{suffix}" if math.isfinite(number) else "Unavailable"

    metrics = st.columns(4)
    metrics[0].metric("Air", value("air_temperature", 1, " °C"))
    metrics[1].metric("Track", value("track_temperature", 1, " °C"))
    metrics[2].metric("Humidity", value("humidity", 0, " %"))
    metrics[3].metric("Wind", value("wind_speed", 1))


def render_session_replay(catalog: SeasonCatalog, session_key: int) -> None:
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
    decision_time = pd.to_datetime(
        selected["scheduled_end_utc"], utc=True, errors="coerce"
    )
    if not isinstance(decision_time, pd.Timestamp) or pd.isna(decision_time):
        decision_time = pd.Timestamp.now(tz="UTC")

    st.title("Session Re-Live")
    st.caption(
        f"{meeting['meeting_name']} · {selected['session_name']} · "
        f"{decision_time.strftime('%d %b %Y, %H:%M UTC')}"
    )
    show_replay_job_status()
    if st.button("Refresh loaded data", use_container_width=False):
        st.cache_data.clear()
        st.rerun()

    drivers = session_silver(session_key, "drivers")
    if drivers.empty:
        st.info("Replay data is being loaded or is not yet available. Refresh shortly.")
        return
    driver_options: list[int] = []
    driver_labels: dict[int, str] = {}
    for row in drivers.itertuples():
        number = int(cast(Any, row.driver_number))
        driver_options.append(number)
        driver_labels[number] = f"{row.name_acronym} · #{number}"
    focus_driver = st.selectbox(
        "Highlighted driver",
        sorted(driver_options),
        format_func=lambda value: driver_labels[value],
    )

    try:
        view = replay_for(
            session_key,
            catalog.season,
            source_meeting_key(selected["meeting_id"]),
            str(meeting["circuit_id"]),
            focus_driver,
            decision_time.isoformat(),
        )
    except (DashboardDataError, ValueError, OSError) as exc:
        st.warning(f"Replay data is incomplete: {exc}")
        st.info("Return to the dashboard and select Re-Live again to load missing data.")
        return

    st.caption(f"{view.frame_count} replay frames · manifest {view.manifest_path.name}")
    circle_tab, track_tab = st.tabs(["Circle of Doom", "Stored track"])
    with circle_tab:
        components.html(view.circle_html, height=930, scrolling=True)
    with track_tab:
        if view.track_html is None:
            st.info("No stored track geometry is available; using the synthetic circle.")
            components.html(view.circle_html, height=930, scrolling=True)
        else:
            components.html(view.track_html, height=930, scrolling=True)

    left, right = st.columns(2)
    with left:
        st.subheader("Final reconstructed positions")
        st.dataframe(view.positions, hide_index=True, use_container_width=True)
    race_control = session_silver(session_key, "race_control")
    if not race_control.empty:
        try:
            race_control = cut_facts(race_control, decision_time=decision_time)
        except TemporalCutError as exc:
            st.warning(str(exc))
            race_control = race_control.iloc[0:0]
    with right:
        st.subheader("Race Control events")
        if race_control.empty:
            st.info("Race Control data is unavailable for this session.")
        else:
            columns = [
                column
                for column in ("event_time", "lap_number", "category", "flag", "message")
                if column in race_control
            ]
            st.dataframe(
                race_control.sort_values("event_time", ascending=False)[columns],
                hide_index=True,
                use_container_width=True,
            )

    st.subheader("Weather")
    weather_panel(session_key, decision_time, session_silver(session_key, "weather"))
