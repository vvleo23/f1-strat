from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd

from f1_pipeline.job_runner import JOB_MANIFEST_DIR, WeekendJobIntent, job_id, run_weekend_job
from f1_pipeline.master_data import load_master_data, master_table_path
from f1_pipeline.persistence import atomic_json, file_lock
from f1_pipeline.temporal import decision_timestamp

SCHEDULE_DIR = JOB_MANIFEST_DIR.parent / "schedules"
SESSION_ATTEMPT_HOURS = (1, 5)
SESSION_CATCHUP_HOURS = 24
FORECAST_SLOT_HOURS = (-24, -6, -1)
JobRunner = Callable[..., dict[str, Any]]


class SchedulerError(RuntimeError):
    pass


def _source_key(value: Any, kind: str) -> int:
    try:
        return int(str(value).rsplit(":", 1)[-1])
    except (TypeError, ValueError) as exc:
        raise SchedulerError(f"Invalid {kind} identity: {value}") from exc


def _timestamp(value: Any, field: str) -> pd.Timestamp:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if not isinstance(parsed, pd.Timestamp) or pd.isna(parsed):
        raise SchedulerError(f"Session has no valid {field}.")
    return parsed


def _state(path: Path, payload: dict[str, Any]) -> dict[str, Any]:
    if not path.is_file():
        return payload
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SchedulerError(f"Could not read schedule state {path}: {exc}") from exc
    if not isinstance(loaded, dict):
        raise SchedulerError(f"Schedule state {path} is invalid.")
    return loaded


def _sessions(season: int, refresh_calendar: bool, job_dir: Path) -> pd.DataFrame:
    path = master_table_path("session", season)
    if refresh_calendar or not path.is_file():
        calendar_status: dict[str, Any] = {
            "season": season,
            "attempted_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            with file_lock(job_dir / "source_pipeline", timeout_seconds=900.0):
                outputs = load_master_data(season, refresh=refresh_calendar)
            path = outputs["session"]
            calendar_status["status"] = "available"
        except Exception as exc:
            calendar_status.update(status="stale" if path.is_file() else "unavailable", error=str(exc))
            if not path.is_file():
                raise SchedulerError(f"Calendar unavailable: {exc}") from exc
        finally:
            atomic_json(calendar_status, job_dir.parent / "schedules" / f"calendar_{season}.json")
    frame = pd.read_parquet(path)
    if frame.empty:
        raise SchedulerError(f"Season {season} has no sessions.")
    return frame.sort_values("scheduled_start_utc").reset_index(drop=True)


def _eligible(session: pd.Series) -> bool:
    return str(session.get("status", "")).casefold() not in {
        "cancelled",
        "postponed",
    }


def _hours(value: int) -> pd.Timedelta:
    return pd.Timedelta(int(value), unit="h")


def _run_job(intent: WeekendJobIntent, *, job_dir: Path, runner: JobRunner) -> dict[str, Any]:
    try:
        return runner(intent, job_dir=job_dir)
    except Exception as exc:
        return {"job_id": job_id(intent), "status": "unavailable", "error": str(exc)}


def _run_session_attempt(
    session: pd.Series,
    *,
    season: int,
    now: pd.Timestamp,
    schedule_dir: Path,
    job_dir: Path,
    runner: JobRunner,
) -> dict[str, Any] | None:
    if not _eligible(session):
        return None
    end = _timestamp(session.get("scheduled_end_utc"), "scheduled_end_utc")
    session_key = _source_key(session.get("session_id"), "session")
    meeting_key = _source_key(session.get("meeting_id"), "meeting")
    path = schedule_dir / f"session_{season}_{session_key}.json"
    if not path.is_file() and now > end + _hours(SESSION_CATCHUP_HOURS):
        return None
    state = _state(
        path,
        {
            "schema_version": 1,
            "schedule_type": "session_ingestion",
            "season": season,
            "meeting_key": meeting_key,
            "session_key": session_key,
            "attempts": [],
        },
    )
    attempts = state.setdefault("attempts", [])
    if any(attempt.get("status") in {"available", "stale"} for attempt in attempts):
        return None
    completed_numbers = {int(attempt["number"]) for attempt in attempts}
    due_attempt = next(
        (
            (number, end + _hours(offset))
            for number, offset in enumerate(SESSION_ATTEMPT_HOURS, start=1)
            if number not in completed_numbers
            and end + _hours(offset) <= now
        ),
        None,
    )
    if due_attempt is None:
        return None
    number, due_at = due_attempt
    intent = WeekendJobIntent(
        season=season,
        meeting_key=meeting_key,
        purpose="weekend",
        target_session_key=session_key,
        decision_time=now.isoformat(),
        refresh=False,
        session_keys=(session_key,),
    )
    attempt = {
        "number": number,
        "due_at": due_at.isoformat(),
        "attempted_at": now.isoformat(),
        "job_id": job_id(intent),
        "status": "running",
    }
    attempts.append(attempt)
    state["status"] = "running"
    state["next_attempt_at"] = (
        (end + _hours(SESSION_ATTEMPT_HOURS[number])).isoformat()
        if number < len(SESSION_ATTEMPT_HOURS) else None
    )
    atomic_json(state, path)
    result = _run_job(intent, job_dir=job_dir, runner=runner)
    status = str(result.get("status", "unavailable"))
    attempt.update(job_id=result.get("job_id"), status=status, error=result.get("error"))
    next_attempt_at = None
    if status not in {"available", "stale"} and number < len(SESSION_ATTEMPT_HOURS):
        next_attempt_at = (
            end + _hours(SESSION_ATTEMPT_HOURS[number])
        ).isoformat()
    state["status"] = status
    state["next_attempt_at"] = next_attempt_at
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    atomic_json(state, path)
    return state


def _run_forecast_slot(
    session: pd.Series,
    *,
    season: int,
    now: pd.Timestamp,
    schedule_dir: Path,
    job_dir: Path,
    runner: JobRunner,
) -> dict[str, Any] | None:
    if not _eligible(session):
        return None
    start = _timestamp(session.get("scheduled_start_utc"), "scheduled_start_utc")
    if now >= start:
        return None
    active_slots = [
        offset
        for offset in FORECAST_SLOT_HOURS
        if start + _hours(offset) <= now
    ]
    if not active_slots:
        return None
    slot = max(active_slots)
    session_key = _source_key(session.get("session_id"), "session")
    meeting_key = _source_key(session.get("meeting_id"), "meeting")
    path = schedule_dir / f"forecast_{season}_{session_key}.json"
    state = _state(
        path,
        {
            "schema_version": 1,
            "schedule_type": "forecast_capture",
            "season": season,
            "meeting_key": meeting_key,
            "session_key": session_key,
            "snapshots": [],
        },
    )
    snapshots = state.setdefault("snapshots", [])
    if any(int(snapshot["slot_hours"]) == slot for snapshot in snapshots):
        return None
    intent = WeekendJobIntent(
        season=season,
        meeting_key=meeting_key,
        purpose="forecast",
        target_session_key=session_key,
        decision_time=now.isoformat(),
        refresh=False,
    )
    snapshot = {
        "slot_hours": slot,
        "due_at": (start + _hours(slot)).isoformat(),
        "attempted_at": now.isoformat(),
        "job_id": job_id(intent),
        "status": "running",
    }
    snapshots.append(snapshot)
    state["status"] = "running"
    atomic_json(state, path)
    result = _run_job(intent, job_dir=job_dir, runner=runner)
    status = str(result.get("status", "unavailable"))
    snapshot.update(job_id=result.get("job_id"), status=status, error=result.get("error"))
    state["status"] = status
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    atomic_json(state, path)
    return state


def run_scheduler_once(
    season: int,
    *,
    now: str | datetime | pd.Timestamp | None = None,
    refresh_calendar: bool = False,
    include_forecasts: bool = True,
    schedule_dir: Path = SCHEDULE_DIR,
    job_dir: Path = JOB_MANIFEST_DIR,
    runner: JobRunner = run_weekend_job,
) -> dict[str, list[dict[str, Any]]]:
    cut_time = decision_timestamp(now or datetime.now(timezone.utc))
    sessions = _sessions(season, refresh_calendar, job_dir)
    schedule_dir.mkdir(parents=True, exist_ok=True)
    forecast_results: list[dict[str, Any]] = []
    session_results: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    operations = [("session_ingestion", _run_session_attempt, session_results)]
    if include_forecasts:
        operations.insert(0, ("forecast_capture", _run_forecast_slot, forecast_results))
    for _, session in sessions.iterrows():
        for schedule_type, operation, results in operations:
            try:
                result = operation(
                    session,
                    season=season,
                    now=cut_time if now is not None else decision_timestamp(datetime.now(timezone.utc)),
                    schedule_dir=schedule_dir,
                    job_dir=job_dir,
                    runner=runner,
                )
                if result is not None:
                    results.append(result)
            except Exception as exc:
                errors.append({
                    "session_id": str(session.get("session_id")),
                    "schedule_type": schedule_type,
                    "error": str(exc),
                })
    atomic_json(
        {"season": season, "attempted_at": cut_time.isoformat(), "errors": errors},
        schedule_dir / f"scheduler_errors_{season}.json",
    )
    return {"forecasts": forecast_results, "sessions": session_results, "errors": errors}


def _next_due_from_sessions(
    sessions: pd.DataFrame,
    *,
    season: int,
    now: pd.Timestamp,
    include_forecasts: bool,
    schedule_dir: Path,
) -> pd.Timestamp | None:
    due_times: list[pd.Timestamp] = []
    for _, session in sessions.iterrows():
        try:
            if not _eligible(session):
                continue
            session_key = _source_key(session.get("session_id"), "session")
            if include_forecasts:
                start = _timestamp(session.get("scheduled_start_utc"), "scheduled_start_utc")
                forecast_state = _state(
                    schedule_dir / f"forecast_{season}_{session_key}.json",
                    {"snapshots": []},
                )
                completed_slots = {
                    int(snapshot["slot_hours"])
                    for snapshot in forecast_state.get("snapshots", [])
                }
                due_times.extend(
                    start + _hours(offset)
                    for offset in FORECAST_SLOT_HOURS
                    if offset not in completed_slots and start + _hours(offset) > now
                )
            end = _timestamp(session.get("scheduled_end_utc"), "scheduled_end_utc")
            session_path = schedule_dir / f"session_{season}_{session_key}.json"
            if not session_path.is_file() and now > end + _hours(SESSION_CATCHUP_HOURS):
                continue
            session_state = _state(
                session_path,
                {"attempts": []},
            )
            attempts = session_state.get("attempts", [])
            if any(attempt.get("status") in {"available", "stale"} for attempt in attempts):
                continue
            completed_numbers = {int(attempt["number"]) for attempt in attempts}
            first_due = end + _hours(SESSION_ATTEMPT_HOURS[0])
            if 1 not in completed_numbers:
                due_times.append(max(first_due, now))
                continue
            first_attempt = next(
                (attempt for attempt in attempts if int(attempt["number"]) == 1),
                None,
            )
            retry_due = end + _hours(SESSION_ATTEMPT_HOURS[1])
            if (
                first_attempt is not None
                and first_attempt.get("status") not in {"available", "stale"}
                and 2 not in completed_numbers
            ):
                due_times.append(max(retry_due, now))
        except (SchedulerError, OSError, ValueError, KeyError, TypeError, AttributeError):
            continue
    return min(due_times) if due_times else None


def next_scheduled_event(
    season: int,
    *,
    now: str | datetime | pd.Timestamp | None = None,
    include_forecasts: bool = True,
    schedule_dir: Path = SCHEDULE_DIR,
    job_dir: Path = JOB_MANIFEST_DIR,
) -> pd.Timestamp | None:
    cut_time = decision_timestamp(now or datetime.now(timezone.utc))
    sessions = _sessions(season, False, job_dir)
    return _next_due_from_sessions(
        sessions,
        season=season,
        now=cut_time,
        include_forecasts=include_forecasts,
        schedule_dir=schedule_dir,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run due F1 pipeline jobs.")
    parser.add_argument("--season", type=int, default=datetime.now(timezone.utc).year)
    parser.add_argument("--refresh-calendar", action="store_true")
    parser.add_argument("--without-forecasts", action="store_true")
    parser.add_argument("--watch", action="store_true")
    args = parser.parse_args(argv)
    refresh_calendar = args.refresh_calendar or args.watch
    while True:
        result = run_scheduler_once(
            args.season,
            refresh_calendar=refresh_calendar,
            include_forecasts=not args.without_forecasts,
        )
        print(json.dumps(result, indent=2, ensure_ascii=False))
        if not args.watch:
            return 0
        next_due = next_scheduled_event(
            args.season,
            include_forecasts=not args.without_forecasts,
        )
        if next_due is None:
            print("No future scheduled events.")
            return 0
        print(f"Next pipeline event: {next_due.isoformat()}")
        try:
            delay_seconds = max(
                0.0,
                (next_due - pd.Timestamp.now(tz="UTC")).total_seconds(),
            )
            time.sleep(delay_seconds)
        except KeyboardInterrupt:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
