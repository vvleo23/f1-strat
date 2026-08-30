from __future__ import annotations

import os
from typing import Any

import pandas as pd
import streamlit as st

from f1_pipeline.dashboard.job_client import JobServiceError, get_job, submit_job
from f1_pipeline.dashboard.read_models import (
    DashboardDataError,
    available_seasons,
    load_latest_standings,
    load_season_catalog,
    load_season_results,
    load_session_bundle,
    source_meeting_key,
    source_session_key,
)

JOB_SERVICE_URL = os.getenv("F1_JOB_SERVICE_URL", "http://127.0.0.1:8765")
JOB_SERVICE_COMMAND = (
    '$env:PYTHONPATH = "src"; python -m f1_pipeline.job_service'
    if os.name == "nt"
    else "PYTHONPATH=src python -m f1_pipeline.job_service"
)
JOB_SERVICE_COMMAND_LANGUAGE = "powershell" if os.name == "nt" else "bash"


@st.cache_data(show_spinner=False)
def catalog_for(season: int):
    return load_season_catalog(season)


@st.cache_data(show_spinner=False)
def season_results_for(season: int):
    return load_season_results(catalog_for(season))


@st.cache_data(show_spinner=False)
def standings_for(season: int, endpoint: str):
    return load_latest_standings(catalog_for(season), endpoint)


def _submit(payload: dict[str, Any]) -> None:
    try:
        result = submit_job(JOB_SERVICE_URL, payload)
    except JobServiceError as exc:
        st.error(str(exc))
        st.code(
            JOB_SERVICE_COMMAND,
            language=JOB_SERVICE_COMMAND_LANGUAGE,
        )
        return
    st.session_state["job_id"] = result["job_id"]
    st.success(f"Data job {result['job_id']} was submitted.")


def _job_status() -> None:
    identifier = st.session_state.get("job_id")
    if not identifier:
        return
    try:
        status = get_job(JOB_SERVICE_URL, identifier)
    except JobServiceError as exc:
        st.warning(str(exc))
        return
    state = status.get("status", "unknown")
    if state in {"available", "stale"}:
        st.success(f"Data job status: {state}")
    elif state == "partial":
        st.warning("Data job completed with partial source availability.")
    elif state == "unavailable":
        st.error(f"Data job failed: {status.get('error', 'source data unavailable')}")
    else:
        st.info(f"Data job status: {state}")


def _display_results(results: pd.DataFrame, drivers: pd.DataFrame) -> None:
    if results.empty:
        st.info("No curated result snapshot is available for this session.")
        return
    display = results.merge(
        drivers[["driver_number", "full_name", "name_acronym", "team_name"]],
        on="driver_number",
        how="left",
    ).sort_values("position")
    columns = [
        "position",
        "name_acronym",
        "full_name",
        "team_name",
        "number_of_laps",
        "points",
        "gap_to_leader_raw",
        "dnf",
        "dns",
        "dsq",
    ]
    st.dataframe(display[[column for column in columns if column in display]], hide_index=True)


def _points_chart(display: pd.DataFrame, label_column: str) -> None:
    chart = display[[label_column, "points_current"]].copy()
    chart["points_current"] = pd.to_numeric(
        chart["points_current"], errors="coerce"
    ).astype(float)
    chart = chart.dropna(subset=[label_column, "points_current"])
    if not chart.empty:
        st.bar_chart(
            chart,
            x=label_column,
            y="points_current",
            horizontal=True,
            stack=False,
        )


def main() -> None:
    st.set_page_config(page_title="F1 Strat · Season Overview", layout="wide")
    st.title("F1 Strat · Season Overview")
    st.caption("Curated historical Formula 1 data with explicit source lineage.")
    seasons = available_seasons()
    if not seasons:
        st.error("No validated season data is available.")
        return
    season = st.selectbox("Season", seasons)
    try:
        catalog = catalog_for(int(season))
    except DashboardDataError as exc:
        st.error(str(exc))
        return

    meetings = catalog.meetings.sort_values("planned_start_utc")
    meeting_labels = {
        row.meeting_id: f"{row.meeting_name} · {str(row.planned_start_utc)[:10]}"
        for row in meetings.itertuples()
    }
    meeting_id = st.selectbox(
        "Race weekend",
        meetings["meeting_id"].tolist(),
        format_func=lambda value: str(meeting_labels.get(value) or value),
    )
    sessions = catalog.sessions[
        catalog.sessions["meeting_id"].eq(meeting_id)
    ].sort_values("scheduled_start_utc")
    if sessions.empty:
        st.warning("This race weekend has no sessions.")
        return
    session_labels = {
        row.session_id: f"{row.session_name} · {str(row.scheduled_start_utc)[:16]}"
        for row in sessions.itertuples()
    }
    session_id = st.selectbox(
        "Session",
        sessions["session_id"].tolist(),
        format_func=lambda value: str(session_labels.get(value) or value),
    )
    selected_session = sessions[sessions["session_id"].eq(session_id)].iloc[0]
    session_key = source_session_key(session_id)
    meeting_key = source_meeting_key(meeting_id)
    decision_time = pd.to_datetime(
        selected_session["scheduled_end_utc"], utc=True, errors="coerce"
    )
    if not isinstance(decision_time, pd.Timestamp) or pd.isna(decision_time):
        decision_time = pd.Timestamp.now(tz="UTC")
    decision_time_text = decision_time.isoformat()

    metrics = st.columns(4)
    metrics[0].metric("Meetings", len(catalog.meetings))
    metrics[1].metric("Sessions", len(catalog.sessions))
    metrics[2].metric("Drivers", len(catalog.drivers))
    metrics[3].metric("Teams", len(catalog.teams))

    actions = st.columns(3)
    if actions[0].button("Load selected session", use_container_width=True):
        _submit(
            {
                "season": int(season),
                "meeting_key": meeting_key,
                "purpose": "weekend",
                "target_session_key": session_key,
                "decision_time": decision_time_text,
                "refresh": False,
                "session_keys": [session_key],
            }
        )
    race_rows = sessions[
        sessions["session_type"].astype(str).str.casefold().eq("race")
    ]
    target_key = (
        source_session_key(race_rows.iloc[-1]["session_id"])
        if not race_rows.empty
        else session_key
    )
    meeting_end = pd.to_datetime(
        meetings[meetings["meeting_id"].eq(meeting_id)].iloc[0]["planned_end_utc"],
        utc=True,
        errors="coerce",
    )
    full_decision_time = (
        meeting_end.isoformat()
        if isinstance(meeting_end, pd.Timestamp) and pd.notna(meeting_end)
        else decision_time_text
    )
    if actions[1].button("Load complete weekend V1", use_container_width=True):
        _submit(
            {
                "season": int(season),
                "meeting_key": meeting_key,
                "purpose": "weekend_complete_v1",
                "target_session_key": target_key,
                "decision_time": full_decision_time,
                "refresh": False,
                "session_keys": [],
            }
        )
    if actions[2].button("Refresh job status", use_container_width=True):
        _job_status()
    _job_status()

    st.subheader("Selected session result")
    try:
        result = load_session_bundle(session_key, ("session_result",)).frames[
            "session_result"
        ]
    except DashboardDataError:
        result = pd.DataFrame()
    _display_results(result, catalog.drivers)

    st.subheader("Championship standings")
    drivers_tab, teams_tab = st.tabs(["Drivers", "Teams"])
    driver_standings = standings_for(int(season), "championship_drivers")
    with drivers_tab:
        if driver_standings.empty:
            st.info("Driver standings are not available for this season.")
        else:
            display = driver_standings.merge(
                catalog.drivers[["driver_number", "full_name", "team_name"]],
                on="driver_number",
                how="left",
            ).sort_values("position_current")
            st.dataframe(
                display[
                    [
                        "position_current",
                        "full_name",
                        "team_name",
                        "points_current",
                    ]
                ],
                hide_index=True,
            )
            _points_chart(display, "full_name")
    team_standings = standings_for(int(season), "championship_teams")
    with teams_tab:
        if team_standings.empty:
            st.info("Team standings are not available for this season.")
        else:
            display = team_standings.sort_values("position_current")
            st.dataframe(
                display[["position_current", "team_name", "points_current"]],
                hide_index=True,
            )
            _points_chart(display, "team_name")

    st.subheader("Season results summary")
    season_results = season_results_for(int(season))
    if season_results.empty:
        st.info("Race wins and podium summaries are not available yet.")
    else:
        summary = season_results.merge(
            catalog.drivers[["driver_number", "full_name", "team_name"]],
            on="driver_number",
            how="left",
        )
        summary = (
            summary.groupby(["full_name", "team_name"], dropna=False)
            .agg(
                Starts=("session_id", "nunique"),
                Wins=("position", lambda values: int(values.eq(1).sum())),
                Podiums=("position", lambda values: int(values.le(3).sum())),
                Points=("points", "sum"),
            )
            .reset_index()
            .sort_values(["Wins", "Podiums", "Points"], ascending=False)
        )
        st.dataframe(summary, hide_index=True)

    if str(selected_session["session_type"]).casefold() == "race":
        st.session_state["replay_season"] = int(season)
        st.session_state["replay_session_key"] = session_key
        st.page_link(
            "pages/2_Session_Replay.py",
            label="Open selected race in Session Replay",
            icon="🏁",
        )


if __name__ == "__main__":
    main()
