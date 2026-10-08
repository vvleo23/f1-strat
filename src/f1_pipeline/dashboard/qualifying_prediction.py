from __future__ import annotations

import math
from typing import Callable

import pandas as pd
import streamlit as st

from f1_pipeline.analysis.qualifying_prediction import compare_prediction_models
from f1_pipeline.dashboard.read_models import (
    DashboardDataError,
    QualifyingPrediction,
    SeasonCatalog,
    load_session_bundle,
)


def _probability(value: object) -> str:
    number = pd.to_numeric(value, errors="coerce")
    return "Unavailable" if pd.isna(number) else f"{float(number):.1%}"


def _lap_time(value: object) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number) or not math.isfinite(float(number)) or float(number) < 0:
        return "Unavailable"
    total_milliseconds = round(float(number) * 1_000)
    minutes, remaining_milliseconds = divmod(total_milliseconds, 60_000)
    seconds, milliseconds = divmod(remaining_milliseconds, 1_000)
    return f"{minutes}:{seconds:02d}.{milliseconds:03d}"


def _position(value: object) -> str:
    number = pd.to_numeric(value, errors="coerce")
    return "—" if pd.isna(number) else f"P{int(number)}"


def _actual_results(session_key: int) -> pd.DataFrame:
    try:
        return load_session_bundle(session_key, ("session_result",)).frames[
            "session_result"
        ]
    except DashboardDataError:
        return pd.DataFrame()


def _metric(summary: pd.DataFrame, model: str, column: str) -> float | None:
    selected = summary[summary["model"].eq(model)]
    if selected.empty:
        return None
    value = pd.to_numeric(selected.iloc[0].get(column), errors="coerce")
    return None if pd.isna(value) else float(value)


def _metric_value(value: float | None, *, percent: bool = False) -> str:
    if value is None:
        return "Pending"
    return f"{value:.0%}" if percent else f"{value:.3f}"


def _comparison_styles(row: pd.Series) -> list[str]:
    styles = pd.Series("", index=row.index)
    for column, background, color in (
        ("Actual", "#312e63", "#e0e7ff"),
        ("Baseline", "#493015", "#fed7aa"),
        ("Complex", "#17324d", "#dbeafe"),
    ):
        if column in row.index:
            styles[column] = f"background-color: {background}; color: {color}"
    if {"Baseline error", "Complex error"}.issubset(row.index):
        baseline_error = pd.to_numeric(row["Baseline error"], errors="coerce")
        complex_error = pd.to_numeric(row["Complex error"], errors="coerce")
        if pd.notna(baseline_error) and pd.notna(complex_error):
            if baseline_error == complex_error:
                for column in ("Baseline error", "Complex error"):
                    styles[column] = "background-color: #443719; color: #fde68a"
            else:
                better = (
                    "Baseline error"
                    if baseline_error < complex_error
                    else "Complex error"
                )
                worse = "Complex error" if better == "Baseline error" else "Baseline error"
                styles[better] = "background-color: #123d2a; color: #bbf7d0"
                styles[worse] = "background-color: #4a1f24; color: #fecaca"
    if "Closer" in row.index:
        closer_styles = {
            "Baseline": "background-color: #493015; color: #fed7aa",
            "Complex": "background-color: #17324d; color: #dbeafe",
            "Tie": "background-color: #443719; color: #fde68a",
        }
        styles["Closer"] = closer_styles.get(str(row["Closer"]), "")
    return styles.tolist()


def _model_card(
    *,
    label: str,
    color: str,
    icon: str,
    mae: float | None,
    rank: float | None,
    top_five: float | None,
    winner: bool,
) -> None:
    with st.container(border=True, height="stretch"):
        with st.container(
            horizontal=True,
            horizontal_alignment="distribute",
            vertical_alignment="center",
        ):
            st.badge(label, color=color, icon=icon)
            if winner:
                st.badge("Best fit", color="green", icon=":material/check_circle:")
        st.metric(
            "Mean position error",
            "Pending" if mae is None else f"{mae:.2f} positions",
        )
        rank_column, top_column = st.columns(2)
        rank_column.metric("Rank correlation", _metric_value(rank))
        top_column.metric("Top-5 overlap", _metric_value(top_five, percent=True))


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
    status_colors = {
        "available": "green",
        "partial": "orange",
        "stale": "yellow",
        "unavailable": "red",
    }
    status_icons = {
        "available": ":material/check_circle:",
        "partial": ":material/warning:",
        "stale": ":material/history:",
        "unavailable": ":material/error:",
    }
    with st.container(horizontal=True, vertical_alignment="center"):
        st.badge(
            status.capitalize(),
            color=status_colors.get(status, "gray"),
            icon=status_icons.get(status),
        )
        st.caption(
            "Decision time: "
            + (
                decision_time.strftime("%d %B %Y, %H:%M UTC")
                if pd.notna(decision_time)
                else "Unavailable"
            )
        )

    diagnostics = snapshot.get("diagnostics", {})
    reasons = diagnostics.get("reasons", []) if isinstance(diagnostics, dict) else []
    if status == "partial":
        with st.expander(
            "Why are these inputs partial?",
            icon=":material/warning:",
            type="compact",
        ):
            message = " ".join(str(reason) for reason in reasons)
            st.warning(message or "Some required inputs are unavailable.")

    rows = prediction.rows.copy()
    if rows.empty:
        st.info("No qualifying prediction rows are available.")
        return
    rows = rows.sort_values("predicted_position", na_position="last")
    comparison, summary = compare_prediction_models(rows, _actual_results(session_key))
    has_baseline = "baseline_position" in comparison.columns
    has_actual = comparison.get(
        "actual_position", pd.Series(dtype="Int64")
    ).notna().any()
    comparison = comparison.sort_values(
        "actual_position" if has_actual else "predicted_position",
        na_position="last",
    )

    baseline_mae = _metric(summary, "Simple baseline", "position_mae")
    complex_mae = _metric(summary, "Complex prediction", "position_mae")
    baseline_rank = _metric(summary, "Simple baseline", "spearman")
    complex_rank = _metric(summary, "Complex prediction", "spearman")
    baseline_top = _metric(summary, "Simple baseline", "top_5_overlap")
    complex_top = _metric(summary, "Complex prediction", "top_5_overlap")
    winner: str | None = None
    if baseline_mae is not None and complex_mae is not None:
        if math.isclose(baseline_mae, complex_mae):
            winner = "tie"
        elif baseline_mae < complex_mae:
            winner = "baseline"
        else:
            winner = "complex"

    st.subheader("Head-to-head")
    st.caption(
        "Orange ranks the latest valid practice laps. Blue combines sessions, weather, "
        "tyres and historical calibration. Lower mean position error is better."
    )
    baseline_column, complex_column = st.columns(2)
    with baseline_column:
        _model_card(
            label="Simple baseline",
            color="orange",
            icon=":material/speed:",
            mae=baseline_mae,
            rank=baseline_rank,
            top_five=baseline_top,
            winner=winner == "baseline",
        )
    with complex_column:
        _model_card(
            label="Complex prediction",
            color="blue",
            icon=":material/analytics:",
            mae=complex_mae,
            rank=complex_rank,
            top_five=complex_top,
            winner=winner == "complex",
        )
    if winner == "tie":
        st.info("Both models have the same mean position error for this session.")
    elif winner is not None:
        winner_name = "Simple baseline" if winner == "baseline" else "Complex prediction"
        margin = abs(float(baseline_mae) - float(complex_mae))
        st.success(
            f"{winner_name} is closer to the actual result by {margin:.2f} positions MAE."
        )
    elif not has_actual:
        st.info(
            "The actual qualifying result is not available locally yet. "
            "Performance metrics will appear after it is loaded."
        )
    if not has_baseline:
        st.info("Recalculate this prediction to add the simple baseline comparison.")

    compact_columns = ["name_acronym", "team_name"]
    if has_actual:
        compact_columns.append("actual_position")
    compact_columns.extend(["baseline_position", "predicted_position"])
    if has_actual:
        compact_columns.extend(
            ["baseline_position_error", "complex_position_error", "closer_model"]
        )
    display = comparison[
        [column for column in compact_columns if column in comparison.columns]
    ].rename(
        columns={
            "name_acronym": "Driver",
            "team_name": "Team",
            "actual_position": "Actual",
            "baseline_position": "Baseline",
            "predicted_position": "Complex",
            "baseline_position_error": "Baseline error",
            "complex_position_error": "Complex error",
            "closer_model": "Closer",
        }
    )
    for column in ("Actual", "Baseline", "Complex"):
        if column in display.columns:
            display[column] = display[column].map(_position)
    styled_display = display.style.apply(_comparison_styles, axis=1)
    st.subheader("Grid comparison")
    st.caption(
        "Green marks the smaller driver-level error, red the larger error and yellow a tie."
        if has_actual
        else "Actual positions and errors appear when the qualifying result is available."
    )
    st.dataframe(
        styled_display,
        hide_index=True,
        height=520,
        key=f"qualifying_grid_comparison_{session_key}",
        column_config={
            "Driver": st.column_config.TextColumn(pinned=True),
            "Baseline error": st.column_config.NumberColumn(format="%.0f"),
            "Complex error": st.column_config.NumberColumn(format="%.0f"),
        },
    )

    st.subheader("Driver focus")
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
    selected = comparison[comparison["driver_number"].eq(focus)].iloc[0]
    with st.container(border=True):
        with st.container(horizontal=True):
            if has_actual:
                st.badge(
                    f"Actual {_position(selected.get('actual_position'))}",
                    color="violet",
                )
            if has_baseline:
                st.badge(
                    f"Baseline {_position(selected.get('baseline_position'))}",
                    color="orange",
                )
            st.badge(
                f"Complex {_position(selected.get('predicted_position'))}",
                color="blue",
            )
        with st.container(horizontal=True):
            st.metric("Top 15", _probability(selected["top_15_probability"]), border=True)
            st.metric("Top 10", _probability(selected["top_10_probability"]), border=True)
            st.metric("Top 5", _probability(selected["top_5_probability"]), border=True)

    with st.expander("Technical model details", icon=":material/tune:"):
        detail_columns = [
            "name_acronym",
            "baseline_lap_seconds",
            "projected_lap_seconds",
            "projected_gap_seconds",
            "top_15_probability",
            "top_10_probability",
            "top_5_probability",
            "evidence_count",
            "row_status",
        ]
        details = comparison[
            [column for column in detail_columns if column in comparison.columns]
        ].rename(
            columns={
                "name_acronym": "Driver",
                "baseline_lap_seconds": "Latest-session lap",
                "projected_lap_seconds": "Projected qualifying lap",
                "projected_gap_seconds": "Projected gap",
                "top_15_probability": "Top 15",
                "top_10_probability": "Top 10",
                "top_5_probability": "Top 5",
                "evidence_count": "Sessions",
                "row_status": "Status",
            }
        )
        for column in ("Latest-session lap", "Projected qualifying lap"):
            if column in details.columns:
                details[column] = details[column].map(_lap_time)
        st.dataframe(
            details,
            hide_index=True,
            column_config={
                "Driver": st.column_config.TextColumn(pinned=True),
                "Projected gap": st.column_config.NumberColumn(format="+%.3f s"),
                "Top 15": st.column_config.NumberColumn(format="percent"),
                "Top 10": st.column_config.NumberColumn(format="percent"),
                "Top 5": st.column_config.NumberColumn(format="percent"),
            },
        )
