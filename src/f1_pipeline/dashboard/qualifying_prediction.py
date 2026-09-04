from __future__ import annotations

from typing import Callable

import pandas as pd
import streamlit as st

from f1_pipeline.dashboard.read_models import QualifyingPrediction, SeasonCatalog


def _probability(value: object) -> str:
    number = pd.to_numeric(value, errors="coerce")
    return "Unavailable" if pd.isna(number) else f"{float(number):.1%}"


def render_qualifying_prediction(
    catalog: SeasonCatalog,
    session_key: int,
    prediction: QualifyingPrediction,
    *,
    go_back: Callable[[], None],
) -> None:
    session_id = f"openf1:session:{session_key}"
    sessions = catalog.sessions[catalog.sessions["session_id"].eq(session_id)]
    title = "Qualifying prediction"
    if not sessions.empty:
        title = f"{sessions.iloc[0]['session_name']} prediction"
    if st.button("Back to dashboard", icon=":material/arrow_back:"):
        go_back()
    st.title(title)
    snapshot = prediction.snapshot
    status = str(snapshot.get("status", "unavailable"))
    decision_time = pd.to_datetime(snapshot.get("decision_time"), utc=True, errors="coerce")
    st.caption(
        f"Status: {status} · Decision time: "
        + (
            decision_time.strftime("%d %B %Y, %H:%M UTC")
            if pd.notna(decision_time)
            else "Unavailable"
        )
    )
    diagnostics = snapshot.get("diagnostics", {})
    reasons = diagnostics.get("reasons", []) if isinstance(diagnostics, dict) else []
    if status == "partial":
        message = " ".join(str(reason) for reason in reasons)
        st.warning(f"This prediction uses partial inputs. {message}")
    rows = prediction.rows.copy()
    if rows.empty:
        st.info("No qualifying prediction rows are available.")
        return
    rows = rows.sort_values("predicted_position", na_position="last")
    options = rows[rows["predicted_position"].notna()]["driver_number"].astype(int).tolist()
    labels = {
        int(row.driver_number): f"{row.name_acronym} · #{int(row.driver_number)}"
        for row in rows.itertuples()
        if pd.notna(row.predicted_position)
    }
    focus = st.selectbox(
        "Focus driver",
        options,
        format_func=lambda value: labels.get(value, str(value)),
        key=f"prediction_focus_driver_{session_key}",
    )
    selected = rows[rows["driver_number"].eq(focus)].iloc[0]
    with st.container(horizontal=True):
        st.metric("Top 15", _probability(selected["top_15_probability"]), border=True)
        st.metric("Top 10", _probability(selected["top_10_probability"]), border=True)
        st.metric("Top 5", _probability(selected["top_5_probability"]), border=True)
    display = rows[
        [
            "predicted_position",
            "name_acronym",
            "team_name",
            "projected_lap_seconds",
            "projected_gap_seconds",
            "top_15_probability",
            "top_10_probability",
            "top_5_probability",
            "evidence_count",
            "row_status",
        ]
    ].rename(
        columns={
            "predicted_position": "Position",
            "name_acronym": "Driver",
            "team_name": "Team",
            "projected_lap_seconds": "Projected lap",
            "projected_gap_seconds": "Projected gap",
            "top_15_probability": "Top 15",
            "top_10_probability": "Top 10",
            "top_5_probability": "Top 5",
            "evidence_count": "Sessions",
            "row_status": "Status",
        }
    )
    with st.container(border=True):
        st.subheader("Predicted classification")
        st.dataframe(
            display,
            hide_index=True,
            column_config={
                "Position": st.column_config.NumberColumn(format="%d", pinned=True),
                "Driver": st.column_config.TextColumn(pinned=True),
                "Projected lap": st.column_config.NumberColumn(format="%.3f s"),
                "Projected gap": st.column_config.NumberColumn(format="+%.3f s"),
                "Top 15": st.column_config.NumberColumn(format="percent"),
                "Top 10": st.column_config.NumberColumn(format="percent"),
                "Top 5": st.column_config.NumberColumn(format="percent"),
            },
        )
