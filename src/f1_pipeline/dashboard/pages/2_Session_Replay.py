from __future__ import annotations

import math
import os
from typing import Any, cast

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from f1_pipeline.dashboard.job_client import JobServiceError, get_job, submit_job
from f1_pipeline.dashboard.read_models import (
    DashboardDataError,
    available_seasons,
    load_forecast,
    load_season_catalog,
    load_session_bundle,
    source_meeting_key,
    source_session_key,
)
from f1_pipeline.dashboard.replay_service import build_replay_view
from f1_pipeline.weather import WeatherCutError, build_weather_cut

JOB_SERVICE_URL = os.getenv("F1_JOB_SERVICE_URL", "http://127.0.0.1:8765")


@st.cache_data(show_spinner=False)
def catalog_for(season: int):
    return load_season_catalog(season)


@st.cache_data(show_spinner="Building historical replay…")
def replay_for(session_key: int, focus_driver: int | None):
    return build_replay_view(session_key, focus_driver=focus_driver)


@st.cache_data(show_spinner=False)
def session_silver(session_key: int, endpoint: str) -> pd.DataFrame:
    try:
        return load_session_bundle(session_key, (endpoint,)).frames[endpoint]
    except DashboardDataError:
        return pd.DataFrame()


@st.cache_data(show_spinner=False)
def forecast_for(session_key: int) -> pd.DataFrame:
    return load_forecast(session_key)


def submit_replay_job(
        *,
        season: int,
        meeting_key: int,
        session_key: int,
        decision_time: str,
) -> None:
    payload: dict[str, Any] = {
        "season": season,
        "meeting_key": meeting_key,
        "purpose": "replay",
        "target_session_key": session_key,
        "decision_time": decision_time,
        "refresh": False,
        "session_keys": [session_key],
    }
    try:
        result = submit_job(JOB_SERVICE_URL, payload)
    except JobServiceError as exc:
        st.error(str(exc))
        st.code("PYTHONPATH=src python -m f1_pipeline.job_service", language="bash")
        return
    st.session_state["replay_job_id"] = result["job_id"]
    st.success(f"Replay data job {result['job_id']} was submitted.")


def show_job_status() -> None:
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
        st.success(f"Replay data job status: {state}")
    elif state == "partial":
        st.warning("Replay data job completed with partial source availability.")
    elif state == "unavailable":
        st.error(f"Replay data job failed: {status.get('error', 'data unavailable')}")
    else:
        st.info(f"Replay data job status: {state}")


def weather_panel(
        session_key: int,
        decision_time: pd.Timestamp,
        observations: pd.DataFrame,
) -> None:
    forecasts = forecast_for(session_key)
    try:
        weather = build_weather_cut(
            forecasts,
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
        if columns:
            for column in columns:
                display[column] = pd.to_numeric(display[column], errors="coerce").astype(
                    float
                )
            display = display.dropna(subset=["valid_time"]).dropna(
                how="all", subset=columns
            )
            if not display.empty:
                st.line_chart(display, x="valid_time", y=columns)
    if weather.observations.empty:
        st.info("No track weather observations are available for this time.")
    else:
        latest = weather.observations.iloc[-1]

        def value(name: str, precision: int, suffix: str = "") -> str:
            try:
                number = float(cast(Any, latest.get(name)))
            except (TypeError, ValueError):
                return "Unavailable"
            if not math.isfinite(number):
                return "Unavailable"
            return f"{number:.{precision}f}{suffix}"

        metrics = st.columns(4)
        metrics[0].metric("Air", value("air_temperature", 1, " °C"))
        metrics[1].metric("Track", value("track_temperature", 1, " °C"))
        metrics[2].metric("Humidity", value("humidity", 0, " %"))
        metrics[3].metric("Wind", value("wind_speed", 1))


def main() -> None:
    st.set_page_config(page_title="F1 Strat · Session Replay", layout="wide")
    st.title("F1 Strat · Session Replay")
    st.caption("Historical replay from persisted OpenF1 snapshots. No strategy output.")
    seasons = available_seasons()
    if not seasons:
        st.error("No validated season data is available.")
        return
    default_season = st.session_state.get("replay_season", seasons[0])
    season_index = seasons.index(default_season) if default_season in seasons else 0
    season = st.selectbox("Season", seasons, index=season_index)
    try:
        catalog = catalog_for(int(season))
    except DashboardDataError as exc:
        st.error(str(exc))
        return
    races = catalog.sessions[
        catalog.sessions["session_type"].astype(str).str.casefold().eq("race")
        & catalog.sessions["status"].eq("completed")
        ].sort_values("scheduled_start_utc", ascending=False)
    if races.empty:
        st.info("No completed race sessions are available for this season.")
        return
    meetings = catalog.meetings[["meeting_id", "meeting_name"]]
    races = races.merge(meetings, on="meeting_id", how="left")
    race_labels = {
        source_session_key(row.session_id): (
            f"{row.meeting_name} · {row.session_name} · "
            f"{str(row.scheduled_start_utc)[:10]}"
        )
        for row in races.itertuples()
    }
    race_keys = list(race_labels)
    preferred = st.session_state.get("replay_session_key")
    race_index = race_keys.index(preferred) if preferred in race_keys else 0
    session_key = st.selectbox(
        "Race session",
        race_keys,
        index=race_index,
        format_func=lambda value: race_labels[value],
    )
    selected = races[
        races["session_id"].eq(f"openf1:session:{session_key}")
    ].iloc[0]
    decision_time = pd.to_datetime(
        selected["scheduled_end_utc"], utc=True, errors="coerce"
    )
    if not isinstance(decision_time, pd.Timestamp) or pd.isna(decision_time):
        decision_time = pd.Timestamp.now(tz="UTC")

    drivers = session_silver(session_key, "drivers")
    driver_options: list[int] = []
    driver_labels: dict[int, str] = {}
    if not drivers.empty:
        for row in drivers.itertuples():
            number = int(cast(Any, row.driver_number))
            driver_options.append(number)
            driver_labels[number] = f"{row.name_acronym} · #{number}"
    focus_driver = (
        st.selectbox(
            "Highlighted driver",
            sorted(driver_options),
            format_func=lambda value: driver_labels[value],
        )
        if driver_options
        else None
    )

    actions = st.columns(3)
    if actions[0].button("Load replay data", use_container_width=True):
        submit_replay_job(
            season=int(season),
            meeting_key=source_meeting_key(selected["meeting_id"]),
            session_key=int(session_key),
            decision_time=decision_time.isoformat(),
        )
    if actions[1].button("Refresh job status", use_container_width=True):
        show_job_status()
    if actions[2].button("Clear dashboard cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    show_job_status()

    try:
        view = replay_for(int(session_key), focus_driver)
    except (DashboardDataError, ValueError, OSError) as exc:
        st.warning(f"Replay data is incomplete: {exc}")
        st.info("Use Load replay data, then refresh after the job completes.")
        return

    st.caption(
        f"{view.frame_count} replay frames · manifest {view.manifest_path.name}"
    )
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
    with right:
        st.subheader("Race Control events")
        if race_control.empty:
            st.info("Race Control data is unavailable for this session.")
        else:
            columns = [
                column
                for column in (
                    "event_time",
                    "lap_number",
                    "category",
                    "flag",
                    "message",
                )
                if column in race_control
            ]
            st.dataframe(
                race_control.sort_values("event_time", ascending=False)[columns],
                hide_index=True,
                use_container_width=True,
            )

    st.subheader("Weather")
    observations = session_silver(session_key, "weather")
    weather_panel(session_key, decision_time, observations)


if __name__ == "__main__":
    main()
