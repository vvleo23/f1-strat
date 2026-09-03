from __future__ import annotations

import json
import os
import re
from html import escape
from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from f1_pipeline.dashboard.job_client import JobServiceError, get_job, submit_job
from f1_pipeline.dashboard.read_models import (
    DashboardDataError,
    SeasonCatalog,
    SessionDataState,
    available_seasons,
    load_forecast,
    load_latest_standings,
    load_qualifying_results,
    load_season_catalog,
    load_season_results,
    load_standings_history,
    load_session_bundle,
    load_session_data_states,
    source_meeting_key,
    source_session_key,
)
from f1_pipeline.dashboard.replay_service import REPLAY_REQUIRED_ENDPOINTS
from f1_pipeline.dashboard.session_replay import render_session_replay
from f1_pipeline.geometry import TrackGeometryError, load_track_geometry

JOB_SERVICE_URL = os.getenv("F1_JOB_SERVICE_URL", "http://127.0.0.1:8765")
JOB_SERVICE_COMMAND = (
    '$env:PYTHONPATH = "src"; python -m f1_pipeline.job_service'
    if os.name == "nt"
    else "PYTHONPATH=src python -m f1_pipeline.job_service"
)
JOB_SERVICE_COMMAND_LANGUAGE = "powershell" if os.name == "nt" else "bash"


@st.cache_data(show_spinner=False)
def catalog_for(season: int) -> SeasonCatalog:
    return load_season_catalog(season)


@st.cache_data(show_spinner=False)
def session_states_for(season: int) -> dict[int, SessionDataState]:
    catalog = catalog_for(season)
    return load_session_data_states(
        source_session_key(session_id) for session_id in catalog.sessions["session_id"]
    )


@st.cache_data(show_spinner=False)
def season_results_for(season: int) -> pd.DataFrame:
    return load_season_results(catalog_for(season))


@st.cache_data(show_spinner=False)
def qualifying_results_for(season: int) -> pd.DataFrame:
    return load_qualifying_results(catalog_for(season))


@st.cache_data(show_spinner=False)
def race_endpoint_frames_for(season: int, endpoint: str) -> pd.DataFrame:
    catalog = catalog_for(season)
    races = catalog.sessions[
        catalog.sessions["status"].eq("completed")
        & catalog.sessions["session_name"].astype(str).str.casefold().eq("race")
    ].sort_values("scheduled_start_utc")
    frames = [
        frame
        for session_id in races["session_id"]
        if not (
            frame := session_frame_for(source_session_key(session_id), endpoint)
        ).empty
    ]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


@st.cache_data(show_spinner=False)
def standings_for(season: int, endpoint: str) -> pd.DataFrame:
    return load_latest_standings(catalog_for(season), endpoint)


@st.cache_data(show_spinner=False)
def standings_history_for(season: int, endpoint: str) -> pd.DataFrame:
    return load_standings_history(catalog_for(season), endpoint)


@st.cache_data(show_spinner=False)
def driver_directory_for(season: int) -> pd.DataFrame:
    catalog = catalog_for(season)
    columns = ["driver_number", "full_name", "team_name"]
    frames = [catalog.drivers[columns].copy()]
    states = session_states_for(season)
    sessions = catalog.sessions.sort_values("scheduled_start_utc", ascending=False)
    for session_id in sessions["session_id"]:
        session_key = source_session_key(session_id)
        if "drivers" not in states[session_key].endpoints:
            continue
        try:
            drivers = load_session_bundle(session_key, ("drivers",)).frames["drivers"]
        except DashboardDataError:
            continue
        if set(columns).issubset(drivers.columns):
            frames.append(drivers[columns].copy())
    return (
        pd.concat(frames, ignore_index=True)
        .dropna(subset=["driver_number"])
        .drop_duplicates("driver_number", keep="first")
    )


@st.cache_data(show_spinner=False)
def weather_status_for(session_keys: tuple[int, ...]) -> tuple[int, int | None]:
    for session_key in reversed(session_keys):
        weather = load_forecast(session_key)
        if not weather.empty:
            return len(weather), session_key
    return 0, None


@st.cache_data(show_spinner=False)
def session_frame_for(session_key: int, endpoint: str) -> pd.DataFrame:
    try:
        return load_session_bundle(session_key, (endpoint,)).frames[endpoint]
    except DashboardDataError:
        return pd.DataFrame()


def _inject_styles() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"], [data-testid="collapsedControl"] { display: none; }
        .st-key-home button {
            border: 0;
            background: transparent;
            color: #e10600;
            font-size: 1.65rem;
            font-weight: 800;
            padding: 0;
        }
        .st-key-home button:hover { color: #ff3b30; background: transparent; }
        div[class*="st-key-weekend_"] { min-height: 100%; }
        div[class*="st-key-weekend_"] div[data-testid="stVerticalBlockBorderWrapper"] {
            height: 100%;
            background: rgba(128, 128, 128, 0.045);
        }
        div[class*="st-key-weekend_"] div[data-testid="stVerticalBlock"] {
            gap: 0.2rem;
        }
        div[class*="st-key-weekend_"] h4 {
            min-height: 2.15rem;
            margin: 0;
            font-size: 0.76rem;
            line-height: 1.12;
        }
        div[class*="st-key-weekend_"] button {
            min-height: 1.65rem;
            padding: 0.1rem 0.2rem;
            font-size: 0.67rem;
            line-height: 1;
        }
        div[class*="st-key-meeting_dialog_"] button {
            min-height: 2.15rem;
            justify-content: flex-start;
            border: 0;
            background: transparent;
            padding: 0;
            color: inherit;
            font-size: 0.76rem;
            font-weight: 650;
            line-height: 1.12;
            text-align: left;
            white-space: normal;
        }
        div[class*="st-key-meeting_dialog_"] button:hover {
            color: #e10600;
            background: transparent;
        }
        div[class*="st-key-loaded_session_"] button { border-left: 4px solid #21a366; }
        div[class*="st-key-missing_session_"] button {
            color: #8b8b8b;
            background: rgba(128, 128, 128, 0.10);
        }
        .st-key-calendar_scroll div[data-testid="stHorizontalBlock"] {
            flex-wrap: nowrap;
            align-items: stretch;
            overflow-x: auto;
            overflow-y: hidden;
            padding: 0.15rem 0.1rem 0.8rem;
            scrollbar-width: thin;
            scrollbar-color: #e10600 rgba(128, 128, 128, 0.15);
        }
        .st-key-calendar_scroll div[data-testid="stColumn"] {
            flex: 0 0 8.5rem;
            width: 8.5rem;
            min-width: 8.5rem;
        }
        .st-key-calendar_scroll div[data-testid="stHorizontalBlock"]::-webkit-scrollbar {
            height: 0.45rem;
        }
        .st-key-calendar_scroll div[data-testid="stHorizontalBlock"]::-webkit-scrollbar-thumb {
            border-radius: 1rem;
            background: #e10600;
        }
        .st-key-calendar_scroll div[data-testid="stHorizontalBlock"]::-webkit-scrollbar-track {
            border-radius: 1rem;
            background: rgba(128, 128, 128, 0.15);
        }
        .calendar-legend { color: #8b8b8b; font-size: 0.9rem; margin-bottom: 0.8rem; }
        .weekend-date {
            min-height: 2rem;
            color: #8b8b8b;
            font-size: 0.61rem;
            line-height: 1.15;
            margin-bottom: 0.15rem;
        }
        .weekend-badge {
            color: #e10600;
            font-size: 0.52rem;
            font-weight: 800;
            letter-spacing: 0.08em;
        }
        .race-results-scroll {
            max-height: 275px;
            overflow: auto;
            border: 1px solid rgba(128, 128, 128, 0.25);
            border-radius: 0.45rem;
        }
        .race-results-table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.78rem;
        }
        .race-results-table th {
            position: sticky;
            top: 0;
            z-index: 1;
            padding: 0.48rem 0.4rem;
            background: #262730;
            color: #fafafa;
            text-align: left;
            white-space: nowrap;
        }
        .race-results-table td {
            padding: 0.42rem 0.4rem;
            border-top: 1px solid rgba(128, 128, 128, 0.16);
            white-space: nowrap;
        }
        .race-results-table th:first-child,
        .race-results-table td:first-child { width: 5.8rem; }
        .position-gain { color: #21a366; font-weight: 700; }
        .position-loss { color: #e10600; font-weight: 700; }
        .position-same { color: #8b8b8b; font-weight: 700; }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _brand() -> None:
    if st.button("F1-Strat", key="home", help="Return to the analysis dashboard"):
        st.session_state["view"] = "dashboard"
        st.session_state.pop("replay_session_key", None)
        st.session_state.pop("active_session_dialog", None)
        st.session_state.pop("active_meeting_dialog", None)
        st.rerun()


def _submit(payload: dict[str, Any], session_key: int) -> dict[str, Any] | None:
    try:
        result = submit_job(JOB_SERVICE_URL, payload)
    except JobServiceError as exc:
        st.error(str(exc))
        st.code(JOB_SERVICE_COMMAND, language=JOB_SERVICE_COMMAND_LANGUAGE)
        return None
    jobs = st.session_state.setdefault("session_job_ids", {})
    jobs[str(session_key)] = result["job_id"]
    st.success(f"Data job {result['job_id']} was submitted.")
    return result


def _job_status(session_key: int) -> None:
    identifier = st.session_state.get("session_job_ids", {}).get(str(session_key))
    if not identifier:
        return
    try:
        status = get_job(JOB_SERVICE_URL, identifier)
    except JobServiceError as exc:
        st.warning(str(exc))
        return
    state = str(status.get("status", "unknown"))
    if state in {"available", "stale"}:
        st.success(f"Data status: {state}")
    elif state == "partial":
        st.warning("Data loaded with partial source availability.")
    elif state == "unavailable":
        st.error(f"Data load failed: {status.get('error', 'source data unavailable')}")
    else:
        st.info(f"Data status: {state}")


def _session_label(session: pd.Series) -> str:
    name = str(session["session_name"]).strip()
    normalized = name.casefold()
    practice = re.search(r"(?:practice|free practice)\s*(\d+)", normalized)
    if practice:
        return f"FP{practice.group(1)}"
    if normalized == "qualifying":
        return "Quali"
    if normalized in {"sprint qualifying", "sprint shootout"}:
        return "Sprint Quali"
    return name


def _decision_time(session: pd.Series) -> str:
    value = pd.to_datetime(session["scheduled_end_utc"], utc=True, errors="coerce")
    if not isinstance(value, pd.Timestamp) or pd.isna(value):
        value = pd.Timestamp.now(tz="UTC")
    return value.isoformat()


def _job_payload(
        season: int,
        session: pd.Series,
        *,
        purpose: str,
        refresh: bool,
) -> dict[str, Any]:
    session_key = source_session_key(session["session_id"])
    return {
        "season": season,
        "meeting_key": source_meeting_key(session["meeting_id"]),
        "purpose": purpose,
        "target_session_key": session_key,
        "decision_time": _decision_time(session),
        "refresh": refresh,
        "session_keys": [session_key],
    }


def _weekend_job_payload(
        season: int,
        meeting: pd.Series,
        sessions: pd.DataFrame,
) -> dict[str, Any]:
    races = sessions[sessions["session_name"].astype(str).str.casefold().eq("race")]
    target = races.iloc[-1] if not races.empty else sessions.iloc[-1]
    meeting_end = pd.to_datetime(
        meeting["planned_end_utc"], utc=True, errors="coerce"
    )
    now = pd.Timestamp.now(tz="UTC")
    decision_time = (
        min(meeting_end, now)
        if isinstance(meeting_end, pd.Timestamp) and pd.notna(meeting_end)
        else now
    )
    return {
        "season": season,
        "meeting_key": source_meeting_key(meeting["meeting_id"]),
        "purpose": "weekend_complete_v1",
        "target_session_key": source_session_key(target["session_id"]),
        "decision_time": decision_time.isoformat(),
        "refresh": False,
        "session_keys": [],
    }


def _track_preview(points: tuple[tuple[float, float], ...]) -> go.Figure:
    frame = pd.DataFrame(points, columns=["x", "y"])
    figure = go.Figure(
        go.Scatter(
            x=frame["x"],
            y=frame["y"],
            mode="lines",
            line={"color": "#e10600", "width": 4},
            hoverinfo="skip",
        )
    )
    figure.update_layout(
        height=135,
        margin={"l": 4, "r": 4, "t": 2, "b": 2},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=False,
        xaxis={"visible": False},
        yaxis={"visible": False, "scaleanchor": "x", "scaleratio": 1},
    )
    return figure


def _session_result_display(
        season: int,
        session_key: int,
) -> pd.DataFrame:
    results = session_frame_for(session_key, "session_result")
    if results.empty:
        return pd.DataFrame()
    session_drivers = session_frame_for(session_key, "drivers")
    identity_columns = ["driver_number", "full_name", "team_name"]
    identities = driver_directory_for(season)
    if set(identity_columns).issubset(session_drivers.columns):
        identities = pd.concat(
            [session_drivers[identity_columns], identities], ignore_index=True
        ).drop_duplicates("driver_number", keep="first")
    return results.merge(identities, on="driver_number", how="left")


def _qualifying_gap(value: Any) -> str:
    values: list[Any]
    if isinstance(value, str) and value.strip().startswith("["):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            parsed = []
        values = parsed if isinstance(parsed, list) else []
    elif isinstance(value, (list, tuple)):
        values = list(value)
    else:
        values = [value]
    numeric = [pd.to_numeric(item, errors="coerce") for item in values]
    available = [float(item) for item in numeric if pd.notna(item)]
    if not available:
        return "Unavailable"
    gap = available[-1]
    return "Leader" if gap == 0 else f"+{gap:.3f}s"


def _result_flag(row: pd.Series, name: str) -> bool:
    value = row.get(name)
    return bool(value) if pd.notna(value) else False


def _race_gap(row: pd.Series) -> str:
    if _result_flag(row, "dsq"):
        return "DSQ"
    if _result_flag(row, "dns"):
        return "DNS"
    if _result_flag(row, "dnf"):
        return "DNF"
    raw = row.get("gap_to_leader_raw")
    if isinstance(raw, str):
        value = raw.strip()
        if "LAP" in value.upper():
            return value.upper()
        numeric = pd.to_numeric(value, errors="coerce")
    else:
        numeric = pd.to_numeric(raw, errors="coerce")
    if pd.notna(numeric):
        gap = float(numeric)
        return "Leader" if gap == 0 else f"+{gap:.3f}s"
    laps = pd.to_numeric(row.get("laps_behind"), errors="coerce")
    if pd.notna(laps) and int(laps) > 0:
        count = int(laps)
        return f"+{count} {'LAP' if count == 1 else 'LAPS'}"
    return "Unavailable"


def _qualifying_results_table(season: int, session_key: int) -> pd.DataFrame:
    display = _session_result_display(season, session_key)
    if display.empty:
        return display
    display = display.sort_values("position", na_position="last").copy()
    display["Time to Leader"] = display["gap_to_leader_raw"].map(_qualifying_gap)
    display = display.rename(
        columns={
            "position": "Position",
            "team_name": "Team",
            "full_name": "Driver",
        }
    )
    return display[["Position", "Team", "Driver", "Time to Leader"]]


def _race_position_change(
        row: pd.Series,
        qualifying_positions: dict[int, int],
) -> str:
    race_position = pd.to_numeric(row.get("position"), errors="coerce")
    if pd.isna(race_position):
        return "NC"
    position = int(race_position)
    driver_number = pd.to_numeric(row.get("driver_number"), errors="coerce")
    if pd.isna(driver_number) or int(driver_number) not in qualifying_positions:
        return str(position)
    change = qualifying_positions[int(driver_number)] - position
    if change > 0:
        return f"{position} ▲ {change}"
    if change < 0:
        return f"{position} ▼ {abs(change)}"
    return f"{position} ● 0"


def _race_results_table(
        season: int,
        session_key: int,
        qualifying_session_key: int | None,
) -> pd.DataFrame:
    display = _session_result_display(season, session_key)
    if display.empty:
        return display
    qualifying_positions: dict[int, int] = {}
    if qualifying_session_key is not None:
        qualifying_results = session_frame_for(
            qualifying_session_key, "session_result"
        )
        required_columns = {"driver_number", "position"}
        if required_columns.issubset(qualifying_results.columns):
            for row in qualifying_results.dropna(
                    subset=["driver_number", "position"]
            ).itertuples():
                qualifying_positions[int(row.driver_number)] = int(row.position)
    display = display.sort_values("position", na_position="last").copy()
    display["Position"] = display.apply(
        _race_position_change,
        axis=1,
        qualifying_positions=qualifying_positions,
    )
    display["Gap to Leader"] = display.apply(_race_gap, axis=1)
    display = display.rename(
        columns={
            "team_name": "Team",
            "full_name": "Driver",
        }
    )
    return display[["Position", "Team", "Driver", "Gap to Leader"]]


def _race_results_html(display: pd.DataFrame) -> str:
    rows: list[str] = []
    change_pattern = re.compile(r"^(\d+)\s+([▲▼●])\s+(\d+)$")
    change_classes = {
        "▲": "position-gain",
        "▼": "position-loss",
        "●": "position-same",
    }
    for row in display.itertuples(index=False):
        position = str(row[0])
        match = change_pattern.fullmatch(position)
        if match:
            base, symbol, amount = match.groups()
            position_html = (
                f'<span class="position-value">{escape(base)}</span> '
                f'<span class="{change_classes[symbol]}">'
                f"{escape(symbol)} {escape(amount)}</span>"
            )
        else:
            position_html = f'<span class="position-value">{escape(position)}</span>'
        values = [
            position_html,
            escape("Unavailable" if pd.isna(row[1]) else str(row[1])),
            escape("Unavailable" if pd.isna(row[2]) else str(row[2])),
            escape("Unavailable" if pd.isna(row[3]) else str(row[3])),
        ]
        rows.append("<tr>" + "".join(f"<td>{value}</td>" for value in values) + "</tr>")
    headers = "".join(
        f"<th>{label}</th>"
        for label in ("Position", "Team", "Driver", "Gap to Leader")
    )
    return (
        '<div class="race-results-scroll"><table class="race-results-table">'
        f"<thead><tr>{headers}</tr></thead><tbody>{''.join(rows)}</tbody>"
        "</table></div>"
    )


def _meeting_dialog(
        season: int,
        catalog: SeasonCatalog,
        meeting: pd.Series,
        states: dict[int, SessionDataState],
) -> None:
    meeting_key = source_meeting_key(meeting["meeting_id"])
    sessions = catalog.sessions[
        catalog.sessions["meeting_id"].eq(meeting["meeting_id"])
        & ~catalog.sessions["is_cancelled"].astype(bool)
    ].sort_values("scheduled_start_utc")
    session_keys = tuple(source_session_key(value) for value in sessions["session_id"])
    loaded_count = sum(states[key].loaded for key in session_keys)
    weather_rows, weather_session_key = weather_status_for(session_keys)
    circuit_rows = catalog.circuits[
        catalog.circuits["circuit_id"].eq(meeting["circuit_id"])
    ]
    circuit = circuit_rows.iloc[0] if not circuit_rows.empty else pd.Series(dtype=object)
    latitude = pd.to_numeric(circuit.get("reference_latitude"), errors="coerce")
    longitude = pd.to_numeric(circuit.get("reference_longitude"), errors="coerce")
    coordinates_available = pd.notna(latitude) and pd.notna(longitude)
    target_key = session_keys[-1]
    races = sessions[sessions["session_name"].astype(str).str.casefold().eq("race")]
    if not races.empty:
        target_key = source_session_key(races.iloc[-1]["session_id"])
    try:
        geometry = load_track_geometry(
            target_key,
            season=season,
            meeting_key=meeting_key,
            circuit_id=str(meeting["circuit_id"]),
        )
    except TrackGeometryError:
        geometry = None
    now = pd.Timestamp.now(tz="UTC")
    session_ends = pd.to_datetime(
        sessions["scheduled_end_utc"], utc=True, errors="coerce"
    )
    load_available = bool(
        sessions["status"].astype(str).str.casefold().eq("completed")
        .where(session_ends.le(now), False)
        .any()
    )
    race_meeting_ids = set(
        catalog.sessions[
            catalog.sessions["session_name"].astype(str).str.casefold().eq("race")
        ]["meeting_id"]
    )
    season_rounds = catalog.meetings[
        catalog.meetings["meeting_id"].isin(race_meeting_ids)
        & ~catalog.meetings["is_cancelled"].astype(bool)
    ].sort_values("planned_start_utc")
    round_ids = season_rounds["meeting_id"].tolist()
    round_number = round_ids.index(meeting["meeting_id"]) + 1
    qualifying = sessions[
        sessions["session_name"].astype(str).str.casefold().eq("qualifying")
    ]
    qualifying_key = (
        source_session_key(qualifying.iloc[-1]["session_id"])
        if not qualifying.empty
        else None
    )
    qualifying_results = (
        _qualifying_results_table(season, qualifying_key)
        if qualifying_key is not None
        else pd.DataFrame()
    )
    race_results = (
        _race_results_table(
            season,
            source_session_key(races.iloc[-1]["session_id"]),
            qualifying_key,
        )
        if not races.empty
        else pd.DataFrame()
    )

    def dismiss() -> None:
        st.session_state.pop("active_meeting_dialog", None)

    @st.dialog(
        f"{meeting['meeting_name']} · Round {round_number}/{len(season_rounds)}",
        width="large",
        on_dismiss=dismiss,
    )
    def dialog() -> None:
        start = pd.to_datetime(meeting["planned_start_utc"], utc=True, errors="coerce")
        end = pd.to_datetime(meeting["planned_end_utc"], utc=True, errors="coerce")
        if isinstance(start, pd.Timestamp) and isinstance(end, pd.Timestamp):
            st.caption(
                f"{start.strftime('%d %B %Y')}–{end.strftime('%d %B %Y')} · "
                f"{meeting['location']}"
            )
        details, preview = st.columns([1.15, 1], gap="large")
        with details:
            session_status = " · ".join(
                f"{'✓' if states[key].loaded else '○'} {_session_label(row)}"
                for key, (_, row) in zip(session_keys, sessions.iterrows())
            )
            st.markdown(
                f"**Local pipeline data:** {loaded_count}/{len(session_keys)} sessions · "
                f"{session_status}"
            )
            if coordinates_available:
                verification = circuit.get("coordinate_verification_status")
                verification_text = (
                    f" · {verification}" if pd.notna(verification) else ""
                )
                st.markdown(
                    f"**Coordinates:** ✓ {float(latitude):.6f}, "
                    f"{float(longitude):.6f}{verification_text}"
                )
            else:
                st.markdown("**Coordinates:** ○ Not available locally")
            if weather_rows:
                st.markdown(
                    f"**Weather:** ✓ {weather_rows} forecast rows · "
                    f"session {weather_session_key}"
                )
            else:
                st.markdown("**Weather:** ○ Not available locally")
        with preview:
            st.markdown("#### Stored track layout")
            if geometry is None:
                st.info("No local track layout is available.")
            else:
                st.plotly_chart(
                    _track_preview(geometry.points),
                    width="stretch",
                    config={"displayModeBar": False, "staticPlot": True},
                    key=f"track_preview_{meeting_key}",
                )
                st.caption(geometry.label)

        result_columns = st.columns(2, gap="large")
        if not qualifying_results.empty:
            with result_columns[0]:
                st.markdown("#### Qualifying Results")
                st.dataframe(
                    qualifying_results,
                    hide_index=True,
                    height=min(275, 40 + 35 * len(qualifying_results)),
                    width="stretch",
                    column_config={
                        "Position": st.column_config.NumberColumn(width="small"),
                        "Team": st.column_config.TextColumn(width="medium"),
                        "Driver": st.column_config.TextColumn(width="medium"),
                        "Time to Leader": st.column_config.TextColumn(width="small"),
                    },
                )
        if not race_results.empty:
            with result_columns[1]:
                st.markdown("#### Race Results")
                st.markdown(
                    _race_results_html(race_results),
                    unsafe_allow_html=True,
                )

        actions = st.columns([1.6, 1, 1])
        if actions[0].button(
            "Load all weekend data",
            type="primary",
            disabled=not load_available,
            use_container_width=True,
            help=(
                None
                if load_available
                else "Weekend data is not available from the source yet."
            ),
            key=f"load_weekend_{meeting_key}",
        ):
            _submit(_weekend_job_payload(season, meeting, sessions), target_key)
        if actions[1].button(
            "Refresh status",
            use_container_width=True,
            key=f"refresh_weekend_{meeting_key}",
        ):
            st.cache_data.clear()
            st.rerun()
        if actions[2].button(
            "Close",
            use_container_width=True,
            key=f"close_weekend_{meeting_key}",
        ):
            st.session_state.pop("active_meeting_dialog", None)
            st.rerun()
        _job_status(target_key)

    dialog()


def _open_replay(
        season: int,
        session: pd.Series,
        focus_driver: int,
) -> None:
    session_key = source_session_key(session["session_id"])
    st.session_state["view"] = "replay"
    st.session_state["replay_season"] = season
    st.session_state["replay_session_key"] = session_key
    st.session_state["replay_focus_driver"] = int(focus_driver)
    st.session_state.pop("replay_job_id", None)
    st.session_state.pop("active_session_dialog", None)
    st.rerun()


def _session_dialog(
        season: int,
        meeting_name: str,
        session: pd.Series,
        state: SessionDataState,
) -> None:
    session_key = source_session_key(session["session_id"])
    label = _session_label(session)
    scheduled_start = pd.to_datetime(
        session["scheduled_start_utc"], utc=True, errors="coerce"
    )
    available = (
        str(session["status"]).casefold() == "completed"
        and isinstance(scheduled_start, pd.Timestamp)
        and pd.notna(scheduled_start)
        and scheduled_start <= pd.Timestamp.now(tz="UTC")
        and not bool(session["is_cancelled"])
    )

    def dismiss() -> None:
        st.session_state.pop("active_session_dialog", None)

    @st.dialog(f"{meeting_name} · {label}", on_dismiss=dismiss)
    def dialog() -> None:
        if isinstance(scheduled_start, pd.Timestamp) and pd.notna(scheduled_start):
            st.caption(scheduled_start.strftime("%A, %d %B %Y · %H:%M UTC"))
        if state.loaded:
            st.success("Data is available locally.")
        elif available:
            st.info("Data has not been loaded yet.")
        else:
            st.warning("This session is not available for download yet.")

        session_type = str(session["session_type"]).casefold()
        session_name = str(session["session_name"]).casefold()
        feature_label: str | None = None
        if "qualifying" in session_type or "qualifying" in session_name:
            feature_label = "Quali Prediction"
        elif session_name == "race":
            feature_label = "Race Strategy"
        replay_ready = any(
            set(REPLAY_REQUIRED_ENDPOINTS).issubset(endpoint_set)
            for endpoint_set in state.raw_endpoint_sets
        )
        drivers = session_frame_for(session_key, "drivers")
        driver_options: list[int] = []
        driver_labels: dict[int, str] = {}
        if not drivers.empty:
            for row in drivers.sort_values("name_acronym").itertuples():
                number = int(row.driver_number)
                driver_options.append(number)
                driver_labels[number] = f"{row.name_acronym} · #{number}"
        focus_driver = (
            st.selectbox(
                "Focus driver",
                driver_options,
                format_func=lambda value: driver_labels[value],
                key=f"focus_driver_{session_key}",
            )
            if driver_options
            else None
        )
        if not driver_options:
            st.caption("Load session data before selecting a Re-Live driver.")
        columns = st.columns(3 if feature_label else 2)
        load_clicked = columns[0].button(
            "Load data",
            disabled=not available,
            width="stretch",
            key=f"load_data_{session_key}",
        )
        replay_label = "Re-Live" if replay_ready else "Prepare Re-Live"
        if columns[1].button(
            replay_label,
            disabled=not available or (replay_ready and focus_driver is None),
            width="stretch",
            key=f"re_live_{session_key}",
        ):
            if replay_ready and focus_driver is not None:
                _open_replay(season, session, int(focus_driver))
            else:
                _submit(
                    _job_payload(season, session, purpose="replay", refresh=False),
                    session_key,
                )
        if feature_label:
            columns[2].button(
                feature_label,
                disabled=True,
                width="stretch",
                help="Planned for a later product version.",
                key=f"future_feature_{session_key}",
            )

        confirmation_key = f"confirm_reload_{session_key}"
        if load_clicked and state.loaded:
            st.session_state[confirmation_key] = True
        elif load_clicked:
            _submit(
                _job_payload(season, session, purpose="weekend", refresh=False),
                session_key,
            )
        if st.session_state.get(confirmation_key):
            st.warning("Local data already exists. Reload it from the sources?")
            confirm, cancel = st.columns(2)
            if confirm.button(
                "Reload data",
                type="primary",
                width="stretch",
                key=f"confirm_reload_button_{session_key}",
            ):
                st.session_state.pop(confirmation_key, None)
                _submit(
                    _job_payload(season, session, purpose="weekend", refresh=True),
                    session_key,
                )
            if cancel.button(
                "Cancel",
                width="stretch",
                key=f"cancel_reload_button_{session_key}",
            ):
                st.session_state.pop(confirmation_key, None)
                st.rerun()
        _job_status(session_key)
        if not replay_ready and st.button(
            "Refresh Re-Live status",
            key=f"refresh_re_live_{session_key}",
        ):
            st.cache_data.clear()
            st.rerun()
        if st.button("Close", key=f"close_dialog_{session_key}"):
            st.session_state.pop("active_session_dialog", None)
            st.rerun()

    dialog()


def _highlighted_meeting(meetings: pd.DataFrame) -> str | None:
    now = pd.Timestamp.now(tz="UTC")
    dated = meetings.copy()
    dated["planned_start_utc"] = pd.to_datetime(
        dated["planned_start_utc"], utc=True, errors="coerce"
    )
    dated["planned_end_utc"] = pd.to_datetime(
        dated["planned_end_utc"], utc=True, errors="coerce"
    )
    current = dated[
        dated["planned_start_utc"].le(now) & dated["planned_end_utc"].ge(now)
    ]
    if not current.empty:
        return str(current.sort_values("planned_start_utc").iloc[0]["meeting_id"])
    future = dated[dated["planned_start_utc"].gt(now)]
    if not future.empty:
        return str(future.sort_values("planned_start_utc").iloc[0]["meeting_id"])
    return None


def _calendar(
        season: int,
        catalog: SeasonCatalog,
        states: dict[int, SessionDataState],
) -> None:
    race_meeting_ids = set(
        catalog.sessions[
            catalog.sessions["session_name"].astype(str).str.casefold().eq("race")
        ]["meeting_id"]
    )
    meetings = catalog.meetings[
        catalog.meetings["meeting_id"].isin(race_meeting_ids)
        & ~catalog.meetings["is_cancelled"].astype(bool)
    ].sort_values("planned_start_utc")
    highlighted = _highlighted_meeting(meetings)
    complete_meetings: set[int] = set()
    for meeting in meetings.itertuples():
        sessions = catalog.sessions[
            catalog.sessions["meeting_id"].eq(meeting.meeting_id)
            & ~catalog.sessions["is_cancelled"].astype(bool)
        ]
        if not sessions.empty and all(
            states[source_session_key(session_id)].loaded
            for session_id in sessions["session_id"]
        ):
            complete_meetings.add(source_meeting_key(meeting.meeting_id))

    css: list[str] = []
    for meeting_key in complete_meetings:
        css.append(
            f".st-key-weekend_{meeting_key} "
            "{ border-radius: 0.55rem; outline: 2px solid #e10600; "
            "outline-offset: -2px; }"
        )
    if highlighted:
        meeting_key = source_meeting_key(highlighted)
        css.append(
            f".st-key-weekend_{meeting_key} "
            "{ box-shadow: 0 0 0 3px rgba(255, 193, 7, 0.45); "
            "background: rgba(255, 193, 7, 0.07); }"
        )
    if css:
        st.markdown(f"<style>{''.join(css)}</style>", unsafe_allow_html=True)

    st.subheader("Race calendar")
    st.markdown(
        '<div class="calendar-legend">✓ Loaded &nbsp;&nbsp; ○ Not loaded '
        '&nbsp;&nbsp; — Not yet available &nbsp;&nbsp; Red frame: complete weekend</div>',
        unsafe_allow_html=True,
    )
    with st.container(key="calendar_scroll"):
        columns = st.columns(len(meetings), gap="small")
        for index, meeting in enumerate(meetings.itertuples()):
            meeting_key = source_meeting_key(meeting.meeting_id)
            sessions = catalog.sessions[
                catalog.sessions["meeting_id"].eq(meeting.meeting_id)
                & ~catalog.sessions["is_cancelled"].astype(bool)
            ].sort_values("scheduled_start_utc")
            with columns[index]:
                with st.container(border=True, key=f"weekend_{meeting_key}"):
                    if str(meeting.meeting_id) == highlighted:
                        st.markdown('<div class="weekend-badge">CURRENT / NEXT</div>', unsafe_allow_html=True)
                    if st.button(
                        str(meeting.meeting_name),
                        key=f"meeting_dialog_{meeting_key}",
                        type="tertiary",
                        use_container_width=True,
                        help="Open weekend details",
                    ):
                        st.session_state["active_meeting_dialog"] = meeting_key
                        st.session_state.pop("active_session_dialog", None)
                    start = pd.to_datetime(meeting.planned_start_utc, utc=True, errors="coerce")
                    end = pd.to_datetime(meeting.planned_end_utc, utc=True, errors="coerce")
                    if isinstance(start, pd.Timestamp) and isinstance(end, pd.Timestamp):
                        st.markdown(
                            f'<div class="weekend-date">{start.strftime("%d %b")}–'
                            f'{end.strftime("%d %b %Y")} · {meeting.location}</div>',
                            unsafe_allow_html=True,
                        )
                    for _, session in sessions.iterrows():
                        session_key = source_session_key(session["session_id"])
                        state = states[session_key]
                        end_time = pd.to_datetime(
                            session["scheduled_end_utc"], utc=True, errors="coerce"
                        )
                        downloadable = (
                            str(session["status"]).casefold() == "completed"
                            and isinstance(end_time, pd.Timestamp)
                            and pd.notna(end_time)
                            and end_time <= pd.Timestamp.now(tz="UTC")
                        )
                        marker = "✓" if state.loaded else ("○" if downloadable else "—")
                        style = "loaded" if state.loaded else "missing"
                        if st.button(
                            f"{marker}  {_session_label(session)}",
                            key=f"{style}_session_{session_key}",
                            disabled=not downloadable,
                            use_container_width=True,
                        ):
                            st.session_state["active_session_dialog"] = session_key
                            st.session_state.pop("active_meeting_dialog", None)

    active_meeting_key = st.session_state.get("active_meeting_dialog")
    if active_meeting_key is not None:
        meeting_id = f"openf1:meeting:{int(active_meeting_key)}"
        meeting_rows = catalog.meetings[catalog.meetings["meeting_id"].eq(meeting_id)]
        if not meeting_rows.empty:
            _meeting_dialog(season, catalog, meeting_rows.iloc[0], states)
    else:
        active_session_key = st.session_state.get("active_session_dialog")
        if active_session_key is None:
            return
        session_id = f"openf1:session:{int(active_session_key)}"
        matches = catalog.sessions[catalog.sessions["session_id"].eq(session_id)]
        if not matches.empty:
            active_session = matches.iloc[0]
            meeting_rows = catalog.meetings[
                catalog.meetings["meeting_id"].eq(active_session["meeting_id"])
            ]
            if not meeting_rows.empty:
                _session_dialog(
                    season,
                    str(meeting_rows.iloc[0]["meeting_name"]),
                    active_session,
                    states[int(active_session_key)],
                )


def _standings_history_chart(
        season: int,
        catalog: SeasonCatalog,
        endpoint: str,
) -> go.Figure | None:
    history = standings_history_for(season, endpoint).copy()
    if history.empty:
        return None
    races = catalog.sessions[
        catalog.sessions["session_name"].astype(str).str.casefold().eq("race")
        & ~catalog.sessions["is_cancelled"].astype(bool)
    ].sort_values("scheduled_start_utc")
    meeting_names = catalog.meetings.set_index("meeting_id")["meeting_name"].to_dict()
    round_details: dict[str, tuple[int, str]] = {}
    for round_number, row in enumerate(races.itertuples(), start=1):
        meeting_name = str(meeting_names.get(row.meeting_id, f"Round {round_number}"))
        short_name = meeting_name.removesuffix(" Grand Prix")
        round_details[str(row.session_id)] = (
            round_number,
            f"R{round_number} {short_name}",
        )
    history["Round"] = history["session_id"].astype(str).map(
        lambda value: round_details.get(value, (None, None))[0]
    )
    history["Round label"] = history["session_id"].astype(str).map(
        lambda value: round_details.get(value, (None, None))[1]
    )
    history["Position"] = pd.to_numeric(history["position_current"], errors="coerce")
    history["Points"] = pd.to_numeric(history["points_current"], errors="coerce")
    history = history.dropna(subset=["Round", "Position"])
    if history.empty:
        return None

    if endpoint == "championship_drivers":
        directory = driver_directory_for(season)[
            ["driver_number", "full_name", "team_name"]
        ].drop_duplicates("driver_number", keep="first")
        history = history.merge(directory, on="driver_number", how="left")
        history["Entity"] = history.apply(
            lambda row: (
                str(row["full_name"])
                if pd.notna(row["full_name"])
                else f"Driver #{int(row['driver_number'])}"
            ),
            axis=1,
        )
        entity_column = "driver_number"
        height = 560
    else:
        history["Entity"] = history["team_name"].astype(str)
        entity_column = "team_name"
        height = 430

    team_colors: dict[str, str] = {}
    for row in catalog.teams.itertuples():
        value = str(row.team_colour).lstrip("#")
        if re.fullmatch(r"[0-9A-Fa-f]{6}", value):
            team_colors[str(row.team_name)] = f"#{value}"
    fallback_colors = (
        "#e10600", "#00a1e8", "#ff8700", "#229971", "#6692ff", "#f50537",
        "#52e252", "#64c4ff", "#b6babd", "#ffd700", "#9b59b6", "#00bcd4",
    )
    latest = (
        history.sort_values("Round")
        .drop_duplicates(entity_column, keep="last")
        .sort_values("Position")
    )
    figure = go.Figure()
    team_trace_counts: dict[str, int] = {}
    dash_patterns = ("solid", "dot", "dash", "dashdot")
    for index, latest_row in enumerate(latest.itertuples(index=False)):
        entity = getattr(latest_row, entity_column)
        rows = history[history[entity_column].eq(entity)].sort_values("Round")
        label = str(rows.iloc[-1]["Entity"])
        team = str(rows.iloc[-1].get("team_name", label))
        team_index = team_trace_counts.get(team, 0)
        team_trace_counts[team] = team_index + 1
        figure.add_trace(
            go.Scatter(
                x=rows["Round"],
                y=rows["Position"],
                customdata=rows[["Round label", "Points"]],
                mode="lines+markers",
                name=label,
                line={
                    "color": team_colors.get(team, fallback_colors[index % len(fallback_colors)]),
                    "width": 2,
                    "dash": dash_patterns[team_index % len(dash_patterns)],
                },
                marker={"size": 7},
                hovertemplate=(
                    "<b>%{fullData.name}</b><br>%{customdata[0]}<br>"
                    "Position %{y:.0f}<br>Points %{customdata[1]:.0f}<extra></extra>"
                ),
            )
        )
    rounds = sorted(int(value) for value in history["Round"].unique())
    labels = [
        str(history.loc[history["Round"].eq(value), "Round label"].iloc[0])
        for value in rounds
    ]
    maximum_position = int(history["Position"].max())
    figure.update_layout(
        height=height,
        margin={"l": 50, "r": 190, "t": 15, "b": 85},
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        hovermode="x unified",
        legend={
            "orientation": "v",
            "yanchor": "top",
            "y": 1,
            "xanchor": "left",
            "x": 1.01,
        },
        xaxis={
            "title": "Race weekend",
            "tickmode": "array",
            "tickvals": rounds,
            "ticktext": labels,
            "tickangle": -25,
            "range": [min(rounds) - 0.25, max(rounds) + 0.25],
            "gridcolor": "rgba(128,128,128,0.16)",
        },
        yaxis={
            "title": "Championship position",
            "dtick": 1,
            "range": [maximum_position + 0.5, 0.5],
            "gridcolor": "rgba(128,128,128,0.16)",
        },
    )
    return figure


def _head_to_head(
        results: pd.DataFrame,
        first_driver: int,
        second_driver: int,
) -> tuple[int, int, int]:
    if results.empty:
        return 0, 0, 0
    frame = results.copy()
    frame["driver_number"] = pd.to_numeric(frame["driver_number"], errors="coerce")
    frame["position"] = pd.to_numeric(frame["position"], errors="coerce")
    first_wins = 0
    second_wins = 0
    compared = 0
    for _, session in frame.groupby("session_id"):
        first = session[session["driver_number"].eq(first_driver)]
        second = session[session["driver_number"].eq(second_driver)]
        if first.empty or second.empty:
            continue
        first_position = first.iloc[-1]["position"]
        second_position = second.iloc[-1]["position"]
        if pd.isna(first_position) and pd.isna(second_position):
            continue
        compared += 1
        if pd.isna(second_position) or (
                pd.notna(first_position) and first_position < second_position
        ):
            first_wins += 1
        elif pd.isna(first_position) or second_position < first_position:
            second_wins += 1
    return first_wins, second_wins, compared


def _driver_comparison_metrics(
        race_results: pd.DataFrame,
        qualifying_results: pd.DataFrame,
        driver_number: int,
) -> list[str]:
    races = race_results[
        pd.to_numeric(race_results["driver_number"], errors="coerce").eq(driver_number)
    ].copy()
    if qualifying_results.empty or "driver_number" not in qualifying_results:
        qualifying = pd.DataFrame(columns=["position"])
    else:
        qualifying = qualifying_results[
            pd.to_numeric(qualifying_results["driver_number"], errors="coerce").eq(
                driver_number
            )
        ].copy()
    race_positions = pd.to_numeric(races.get("position"), errors="coerce").dropna()
    qualifying_positions = pd.to_numeric(
        qualifying.get("position"), errors="coerce"
    ).dropna()
    points = pd.to_numeric(races.get("points"), errors="coerce").fillna(0).sum()
    dnf_count = (
        races.get("dnf", pd.Series(False, index=races.index))
        .fillna(False)
        .astype(bool)
        .sum()
    )
    return [
        str(races["session_id"].nunique()),
        f"{float(points):g}",
        str(int(race_positions.eq(1).sum())),
        str(int(race_positions.le(3).sum())),
        f"P{int(race_positions.min())}" if not race_positions.empty else "—",
        f"{race_positions.mean():.1f}" if not race_positions.empty else "—",
        str(int(dnf_count)),
        f"{qualifying_positions.mean():.1f}"
        if not qualifying_positions.empty
        else "—",
    ]


def _teammate_comparison(season: int, catalog: SeasonCatalog) -> None:
    race_results = season_results_for(season)
    qualifying_results = qualifying_results_for(season)
    if race_results.empty:
        st.info("Load race weekends to compare teammates.")
        return
    directory = driver_directory_for(season)[
        ["driver_number", "full_name", "team_name"]
    ].copy()
    directory["driver_number"] = pd.to_numeric(
        directory["driver_number"], errors="coerce"
    )
    race_numbers = set(
        pd.to_numeric(race_results["driver_number"], errors="coerce").dropna().astype(int)
    )
    directory = directory[
        directory["driver_number"].isin(race_numbers)
        & directory["team_name"].notna()
    ].drop_duplicates("driver_number", keep="first")
    team_counts = directory.groupby("team_name")["driver_number"].nunique()
    teams = sorted(str(team) for team, count in team_counts.items() if count >= 2)
    if not teams:
        st.info("No team has two drivers with locally loaded race results.")
        return
    control_column, table_column = st.columns([0.8, 2.7], gap="large")
    with control_column:
        selected_team = st.selectbox(
            "Team",
            teams,
            key=f"teammate_team_{season}",
            width=230,
        )
    race_starts = (
        race_results.groupby("driver_number")["session_id"].nunique().rename("race_starts")
    )
    drivers = directory[directory["team_name"].astype(str).eq(selected_team)].copy()
    drivers = drivers.merge(
        race_starts,
        left_on="driver_number",
        right_index=True,
        how="left",
    ).sort_values(["race_starts", "full_name"], ascending=[False, True]).head(2)
    if len(drivers) < 2:
        st.info("Two drivers with loaded results are required for this comparison.")
        return
    first, second = tuple(drivers.itertuples(index=False))
    first_number = int(first.driver_number)
    second_number = int(second.driver_number)
    first_name = str(first.full_name)
    second_name = str(second.full_name)
    metrics = [
        "Races",
        "Points",
        "Wins",
        "Podiums",
        "Best finish",
        "Average finish",
        "DNFs",
        "Average qualifying position",
    ]
    comparison = pd.DataFrame(
        {
            "Metric": metrics,
            f"{first_name} · #{first_number}": _driver_comparison_metrics(
                race_results, qualifying_results, first_number
            ),
            f"{second_name} · #{second_number}": _driver_comparison_metrics(
                race_results, qualifying_results, second_number
            ),
        }
    )
    qualifying_score = _head_to_head(
        qualifying_results, first_number, second_number
    )
    race_score = _head_to_head(race_results, first_number, second_number)
    with control_column:
        st.caption(f"Score order: {first_name} — {second_name}")
        st.metric(
            "Qualifying head-to-head",
            f"{qualifying_score[0]} – {qualifying_score[1]}",
            help=f"{qualifying_score[2]} comparable qualifying sessions",
        )
        st.metric(
            "Race head-to-head",
            f"{race_score[0]} – {race_score[1]}",
            help=f"{race_score[2]} comparable races",
        )
    with table_column:
        st.dataframe(
            comparison,
            hide_index=True,
            height=40 + 35 * len(comparison),
            width="stretch",
        )


def _season_performance_highlights(season: int, catalog: SeasonCatalog) -> None:
    all_races = catalog.sessions[
        catalog.sessions["session_name"].astype(str).str.casefold().eq("race")
        & ~catalog.sessions["is_cancelled"].astype(bool)
    ].sort_values("scheduled_start_utc")
    meeting_names = catalog.meetings.set_index("meeting_id")["meeting_name"].to_dict()
    context_rows = []
    for round_number, race in enumerate(all_races.itertuples(), start=1):
        if str(race.status).casefold() != "completed":
            continue
        meeting_name = str(meeting_names.get(race.meeting_id, f"Round {round_number}"))
        context_rows.append(
            {
                "session_id": str(race.session_id),
                "Weekend": (
                    f"R{round_number} · {meeting_name.removesuffix(' Grand Prix')}"
                ),
            }
        )
    context = pd.DataFrame(context_rows)
    completed_races = len(context)
    identities = race_endpoint_frames_for(season, "drivers")
    identity_columns = ["session_id", "driver_number", "full_name", "team_name"]
    if set(identity_columns).issubset(identities.columns):
        identities = identities[identity_columns].drop_duplicates(
            ["session_id", "driver_number"], keep="last"
        )
    else:
        identities = pd.DataFrame(columns=identity_columns)

    grid_column, pit_column = st.columns(2, gap="large")

    grid_column.markdown("### Grid to finish")
    grids = race_endpoint_frames_for(season, "starting_grid")
    results = season_results_for(season).copy()
    if grids.empty or results.empty:
        grid_column.caption(
            f"0 comparable driver results · based on 0/{completed_races} "
            "completed races"
        )
        grid_column.info("Reload race data to add official starting-grid information.")
    else:
        grids = grids[["session_id", "driver_number", "grid_position"]].copy()
        grids["driver_number"] = pd.to_numeric(
            grids["driver_number"], errors="coerce"
        )
        grids["Grid"] = pd.to_numeric(grids["grid_position"], errors="coerce")
        results["driver_number"] = pd.to_numeric(
            results["driver_number"], errors="coerce"
        )
        results["Finish"] = pd.to_numeric(results["position"], errors="coerce")
        for flag in ("dns", "dsq"):
            results[flag] = results.get(
                flag, pd.Series(False, index=results.index)
            ).fillna(False).astype(bool)
        comparisons = grids.merge(
            results[["session_id", "driver_number", "Finish", "dns", "dsq"]],
            on=["session_id", "driver_number"],
            how="inner",
        )
        comparisons = comparisons[
            comparisons["Grid"].notna()
            & comparisons["Finish"].notna()
            & comparisons["Grid"].gt(0)
            & comparisons["Finish"].gt(0)
            & ~comparisons["dns"]
            & ~comparisons["dsq"]
        ].copy()
        comparisons["Positions"] = comparisons["Grid"] - comparisons["Finish"]
        comparisons = comparisons.merge(
            identities,
            on=["session_id", "driver_number"],
            how="left",
        ).merge(context, on="session_id", how="left")
        comparisons["Driver"] = comparisons["full_name"].fillna(
            comparisons["driver_number"].map(
                lambda value: f"Driver #{int(value)}"
            )
        )
        comparisons["Team"] = comparisons["team_name"].fillna("Unavailable")
        comparisons["Grid"] = comparisons["Grid"].astype(int)
        comparisons["Finish"] = comparisons["Finish"].astype(int)
        comparable_races = comparisons["session_id"].nunique()
        grid_column.caption(
            f"{len(comparisons)} comparable classified driver results · "
            f"based on {comparable_races}/{completed_races} completed races"
        )
        selections = (
            ("Top 3 positions gained", comparisons.nlargest(3, "Positions")),
            ("Flop 3 positions lost", comparisons.nsmallest(3, "Positions")),
        )
        for title, selection in selections:
            grid_column.markdown(f"#### {title}")
            display = selection.copy()
            display["Change"] = display["Positions"].map(
                lambda value: f"{int(value):+d}"
            )
            grid_column.dataframe(
                display[["Driver", "Team", "Weekend", "Grid", "Finish", "Change"]],
                hide_index=True,
                height=40 + 35 * len(display),
                width="stretch",
            )

    pit_column.markdown("### Fastest pit stops")
    pits = race_endpoint_frames_for(season, "pit")
    if pits.empty:
        pit_column.caption(
            f"0 valid stop durations from 0 pit records · based on "
            f"0/{completed_races} completed races"
        )
        pit_column.info("Load race pit data to rank the fastest stops.")
        return
    pits = pits.copy()
    pits["Stop seconds"] = pd.to_numeric(
        pits["stop_duration_seconds"], errors="coerce"
    )
    valid_pits = pits[pits["Stop seconds"].gt(0)].copy()
    pit_races = valid_pits["session_id"].nunique()
    pit_column.caption(
        f"{len(valid_pits)} valid stop durations from {len(pits)} pit records · "
        f"based on {pit_races}/{completed_races} completed races"
    )
    if valid_pits.empty:
        pit_column.info("No valid stationary stop durations are available.")
        return
    fastest = valid_pits.nsmallest(3, "Stop seconds").merge(
        identities,
        on=["session_id", "driver_number"],
        how="left",
    ).merge(context, on="session_id", how="left")
    fastest["Driver"] = fastest["full_name"].fillna(
        fastest["driver_number"].map(lambda value: f"Driver #{int(value)}")
    )
    fastest["Team"] = fastest["team_name"].fillna("Unavailable")
    fastest["Lap"] = pd.to_numeric(fastest["lap_number"], errors="coerce").astype(
        "Int64"
    )
    fastest["Stop time"] = fastest["Stop seconds"].map(
        lambda value: f"{value:.2f}s"
    )
    pit_column.dataframe(
        fastest[["Driver", "Team", "Weekend", "Lap", "Stop time"]],
        hide_index=True,
        height=40 + 35 * len(fastest),
        width="stretch",
    )


def _season_analysis(season: int, catalog: SeasonCatalog) -> None:
    st.subheader("Championship standings")
    driver_standings = standings_for(season, "championship_drivers")
    constructor_standings = standings_for(season, "championship_teams")
    constructor_names = set(catalog.teams["team_name"].dropna().astype(str))
    if not constructor_standings.empty and "team_name" in constructor_standings:
        constructor_standings = constructor_standings[
            constructor_standings["team_name"].astype(str).isin(constructor_names)
        ].drop_duplicates("team_name", keep="last")
    driver_column, constructor_column = st.columns([1.75, 1], gap="large")
    with driver_column:
        st.markdown("#### Drivers")
        if driver_standings.empty:
            st.info("Driver standings are not available for this season.")
        else:
            display = driver_standings.merge(
                driver_directory_for(season),
                on="driver_number",
                how="left",
            ).sort_values("position_current")
            results = season_results_for(season)
            if results.empty:
                statistics = pd.DataFrame(
                    columns=["driver_number", "Wins", "Podiums"]
                )
            else:
                statistics = (
                    results.groupby("driver_number", as_index=False)
                    .agg(
                        Wins=("position", lambda values: int(values.eq(1).sum())),
                        Podiums=("position", lambda values: int(values.le(3).sum())),
                    )
                )
            display = display.merge(statistics, on="driver_number", how="left")
            display[["Wins", "Podiums"]] = (
                display[["Wins", "Podiums"]].fillna(0).astype(int)
            )
            display = display.rename(
                columns={
                    "position_current": "Position",
                    "full_name": "Driver",
                    "driver_number": "Number",
                    "team_name": "Team",
                    "points_current": "Points",
                }
            )
            st.dataframe(
                display[
                    ["Position", "Driver", "Number", "Team", "Points", "Wins", "Podiums"]
                ],
                hide_index=True,
                height=455,
                width="stretch",
                column_config={
                    "Position": st.column_config.NumberColumn(width="small"),
                    "Driver": st.column_config.TextColumn(width="medium"),
                    "Number": st.column_config.NumberColumn(width="small"),
                    "Team": st.column_config.TextColumn(width="medium"),
                    "Points": st.column_config.NumberColumn(width="small"),
                    "Wins": st.column_config.NumberColumn(width="small"),
                    "Podiums": st.column_config.NumberColumn(width="small"),
                },
            )
    with constructor_column:
        st.markdown("#### Constructors")
        if constructor_standings.empty:
            st.info("Constructor standings are not available for this season.")
        else:
            display = constructor_standings.sort_values("position_current").rename(
                columns={
                    "position_current": "Position",
                    "team_name": "Constructor",
                    "points_current": "Points",
                }
            )
            st.dataframe(
                display[["Position", "Constructor", "Points"]],
                hide_index=True,
                height=40 + 35 * len(display),
                width="stretch",
                column_config={
                    "Position": st.column_config.NumberColumn(width="small"),
                    "Constructor": st.column_config.TextColumn(width="medium"),
                    "Points": st.column_config.NumberColumn(width="small"),
                },
            )

    st.markdown("### Standings progression")
    st.caption(
        "Championship position after each race with locally loaded standings data. "
    )
    driver_history = _standings_history_chart(
        season, catalog, "championship_drivers"
    )
    constructor_history = _standings_history_chart(
        season, catalog, "championship_teams"
    )
    if driver_history is None and constructor_history is None:
        st.info("Load complete race weekends to build the standings progression.")
    if driver_history is not None:
        st.markdown("#### Drivers")
        st.plotly_chart(
            driver_history,
            width="stretch",
            config={"displayModeBar": False},
            key=f"driver_standings_history_{season}",
        )
    if constructor_history is not None:
        st.markdown("#### Constructors")
        st.plotly_chart(
            constructor_history,
            width="stretch",
            config={"displayModeBar": False},
            key=f"constructor_standings_history_{season}",
        )
    st.markdown("### Teammate comparison")
    st.caption("Comparison uses locally loaded qualifying and race results.")
    _teammate_comparison(season, catalog)
    _season_performance_highlights(season, catalog)


def _dashboard(seasons: tuple[int, ...]) -> None:
    st.title("Analysis Dashboard")
    season_column, refresh_column, _ = st.columns(
        [1, 1.2, 7.8], gap="small", vertical_alignment="bottom"
    )
    season = int(season_column.selectbox("Season", seasons))
    if refresh_column.button("Refresh status", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    try:
        catalog = catalog_for(season)
        states = session_states_for(season)
    except DashboardDataError as exc:
        st.error(str(exc))
        return
    if "demo_data" in catalog.manifest_path.parts:
        st.info("Using the bundled demo data until local session data is loaded.")
    _calendar(season, catalog, states)
    st.divider()
    _season_analysis(season, catalog)


def main() -> None:
    st.set_page_config(page_title="F1-Strat", page_icon="🏁", layout="wide")
    _inject_styles()
    seasons = available_seasons()
    if not seasons:
        st.error("No validated season data is available.")
        return
    if st.session_state.get("view") == "replay":
        season = int(st.session_state.get("replay_season", seasons[0]))
        session_key = st.session_state.get("replay_session_key")
        if session_key is None:
            st.session_state["view"] = "dashboard"
            st.rerun()
        try:
            render_session_replay(catalog_for(season), int(session_key))
        except DashboardDataError as exc:
            st.error(str(exc))
        return
    _brand()
    _dashboard(seasons)


if __name__ == "__main__":
    main()
