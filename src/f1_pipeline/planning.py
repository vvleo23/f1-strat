from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from f1_pipeline.sources.open_meteo import utc_timestamp
from f1_pipeline.sources.openf1_weekend import OpenF1WeekendError, normalize_session_type

PURPOSES = (
    "weekend",
    "weekend_complete_v1",
    "replay",
    "qualifying_prediction",
    "race_strategy",
)


class SessionPlanningError(RuntimeError):
    pass


def _timestamp(session: dict[str, Any], field: str) -> pd.Timestamp | None:
    value = pd.to_datetime(session.get(field), utc=True, errors="coerce")
    return value if isinstance(value, pd.Timestamp) and pd.notna(value) else None


def plan_sessions_for_purpose(
    sessions: list[dict[str, Any]],
    *,
    purpose: str,
    decision_time: str | datetime | pd.Timestamp,
    target_session_key: int | None = None,
) -> dict[str, Any]:
    normalized_purpose = purpose.strip().casefold() if isinstance(purpose, str) else ""
    if normalized_purpose not in PURPOSES:
        raise SessionPlanningError(f"Purpose must be one of: {', '.join(PURPOSES)}.")
    cut_time = utc_timestamp(decision_time, "decision_time")
    usable: list[dict[str, Any]] = []
    for session in sessions:
        if str(session.get("status", "")).casefold() == "cancelled":
            continue
        try:
            session_type = normalize_session_type(
                str(session.get("session_type", "")),
                str(session.get("session_name", "")),
            )
        except OpenF1WeekendError as exc:
            raise SessionPlanningError(str(exc)) from exc
        usable.append({**session, "normalized_session_type": session_type})
    if not usable:
        raise SessionPlanningError("The meeting has no usable sessions.")
    ordered = sorted(
        usable,
        key=lambda session: (
            _timestamp(session, "scheduled_start_utc") is None,
            _timestamp(session, "scheduled_start_utc"),
            session["source_session_key"],
        ),
    )

    def completed_by_cut(session: dict[str, Any]) -> bool:
        end = _timestamp(session, "scheduled_end_utc")
        return (
            str(session.get("status", "")).casefold() == "completed"
            and end is not None
            and end <= cut_time
        )

    target = None
    if target_session_key is not None:
        matches = [
            session
            for session in ordered
            if session["source_session_key"] == target_session_key
        ]
        if len(matches) != 1:
            raise SessionPlanningError(
                f"Target session {target_session_key} does not belong to the meeting."
            )
        target = matches[0]
    elif normalized_purpose == "qualifying_prediction":
        qualifying = [
            session
            for session in ordered
            if session["normalized_session_type"] == "qualifying"
        ]
        if not qualifying:
            qualifying = [
                session
                for session in ordered
                if session["normalized_session_type"] == "sprint_qualifying"
            ]
        upcoming = [
            session
            for session in qualifying
            if (_timestamp(session, "scheduled_start_utc") or cut_time) >= cut_time
        ]
        target = upcoming[0] if upcoming else (qualifying[-1] if qualifying else None)
    elif normalized_purpose in {"replay", "race_strategy"}:
        races = [
            session
            for session in ordered
            if session["normalized_session_type"] == "race"
        ]
        target = races[-1] if races else None
    else:
        upcoming = [
            session
            for session in ordered
            if (_timestamp(session, "scheduled_start_utc") or cut_time) >= cut_time
        ]
        races = [
            session
            for session in ordered
            if session["normalized_session_type"] == "race"
        ]
        target = upcoming[0] if upcoming else (races[-1] if races else ordered[-1])
    if target is None:
        raise SessionPlanningError(
            f"No target session is available for purpose '{normalized_purpose}'."
        )
    if (
        normalized_purpose == "qualifying_prediction"
        and target["normalized_session_type"]
        not in {"qualifying", "sprint_qualifying"}
    ):
        raise SessionPlanningError(
            "Qualifying prediction requires a Qualifying or Sprint Qualifying target."
        )

    if normalized_purpose == "replay":
        if not completed_by_cut(target):
            raise SessionPlanningError("Replay requires a completed target session.")
        selected = [target]
    elif normalized_purpose == "qualifying_prediction":
        target_start = _timestamp(target, "scheduled_start_utc")
        selected = [
            session
            for session in ordered
            if completed_by_cut(session)
            and session is not target
            and (
                target_start is None
                or (_timestamp(session, "scheduled_end_utc") or cut_time) <= target_start
            )
        ]
    elif normalized_purpose == "race_strategy":
        selected = [session for session in ordered if completed_by_cut(session)]
        target_start = _timestamp(target, "scheduled_start_utc")
        if (
            target not in selected
            and target_start is not None
            and target_start <= cut_time
            and str(target.get("status", "")).casefold() == "completed"
        ):
            selected.append(target)
    else:
        selected = [session for session in ordered if completed_by_cut(session)]
    return {
        "purpose": normalized_purpose,
        "decision_time": cut_time.isoformat(),
        "selected_sessions": selected,
        "target_session": target,
        "selection_basis": "scheduled_end_and_session_status",
    }
