from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from f1_pipeline.scheduler import (
    _next_due_from_sessions,
    _run_forecast_slot,
    _run_session_attempt,
)


class SchedulerTest(unittest.TestCase):
    def session(self) -> pd.Series:
        return pd.Series(
            {
                "session_id": "openf1:session:42",
                "meeting_id": "openf1:meeting:7",
                "scheduled_start_utc": "2026-07-26T08:00:00Z",
                "scheduled_end_utc": "2026-07-26T10:00:00Z",
                "status": "scheduled",
            }
        )

    def test_session_attempts_run_at_plus_one_and_plus_five_hours(self) -> None:
        calls = []

        def runner(intent, *, job_dir):
            calls.append(intent)
            status = "partial" if len(calls) == 1 else "available"
            return {"job_id": f"job-{len(calls)}", "status": status}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = _run_session_attempt(
                self.session(),
                season=2026,
                now=pd.Timestamp("2026-07-26T11:00:00Z"),
                schedule_dir=root / "schedules",
                job_dir=root / "jobs",
                runner=runner,
            )
            early = _run_session_attempt(
                self.session(),
                season=2026,
                now=pd.Timestamp("2026-07-26T14:59:00Z"),
                schedule_dir=root / "schedules",
                job_dir=root / "jobs",
                runner=runner,
            )
            retry = _run_session_attempt(
                self.session(),
                season=2026,
                now=pd.Timestamp("2026-07-26T15:00:00Z"),
                schedule_dir=root / "schedules",
                job_dir=root / "jobs",
                runner=runner,
            )

        self.assertEqual(first["next_attempt_at"], "2026-07-26T15:00:00+00:00")
        self.assertIsNone(early)
        self.assertEqual(retry["status"], "available")
        self.assertIsNone(retry["next_attempt_at"])
        self.assertEqual([call.purpose for call in calls], ["weekend", "weekend"])
        self.assertEqual(calls[0].session_keys, (42,))
        self.assertFalse(calls[0].refresh)

    def test_forecast_runs_once_per_active_slot(self) -> None:
        calls = []

        def runner(intent, *, job_dir):
            calls.append(intent)
            return {"job_id": f"forecast-{len(calls)}", "status": "available"}

        session = self.session().copy()
        session["scheduled_start_utc"] = "2026-07-27T12:00:00Z"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            first = _run_forecast_slot(
                session,
                season=2026,
                now=pd.Timestamp("2026-07-27T06:00:00Z"),
                schedule_dir=root / "schedules",
                job_dir=root / "jobs",
                runner=runner,
            )
            duplicate = _run_forecast_slot(
                session,
                season=2026,
                now=pd.Timestamp("2026-07-27T07:00:00Z"),
                schedule_dir=root / "schedules",
                job_dir=root / "jobs",
                runner=runner,
            )
            latest = _run_forecast_slot(
                session,
                season=2026,
                now=pd.Timestamp("2026-07-27T11:00:00Z"),
                schedule_dir=root / "schedules",
                job_dir=root / "jobs",
                runner=runner,
            )

        self.assertEqual(first["snapshots"][0]["slot_hours"], -6)
        self.assertIsNone(duplicate)
        self.assertEqual(latest["snapshots"][-1]["slot_hours"], -1)
        self.assertEqual([call.purpose for call in calls], ["forecast", "forecast"])
        self.assertEqual(calls[0].target_session_key, 42)

    def test_old_untracked_session_is_not_backfilled(self) -> None:
        def runner(intent, *, job_dir):
            raise AssertionError("Old session must not run")

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = _run_session_attempt(
                self.session(),
                season=2026,
                now=pd.Timestamp("2026-07-27T11:00:01Z"),
                schedule_dir=root / "schedules",
                job_dir=root / "jobs",
                runner=runner,
            )

        self.assertIsNone(result)

    def test_next_event_schedules_retry_only_after_incomplete_first_attempt(self) -> None:
        sessions = pd.DataFrame([self.session()])
        with tempfile.TemporaryDirectory() as temporary:
            schedule_dir = Path(temporary)
            first_due = _next_due_from_sessions(
                sessions,
                season=2026,
                now=pd.Timestamp("2026-07-26T09:00:00Z"),
                include_forecasts=False,
                schedule_dir=schedule_dir,
            )
            state = {
                "attempts": [{"number": 1, "status": "partial"}],
            }
            (schedule_dir / "session_2026_42.json").write_text(
                json.dumps(state),
                encoding="utf-8",
            )
            retry_due = _next_due_from_sessions(
                sessions,
                season=2026,
                now=pd.Timestamp("2026-07-26T12:00:00Z"),
                include_forecasts=False,
                schedule_dir=schedule_dir,
            )
            overdue_retry = _next_due_from_sessions(
                sessions,
                season=2026,
                now=pd.Timestamp("2026-07-26T15:30:00Z"),
                include_forecasts=False,
                schedule_dir=schedule_dir,
            )

        self.assertEqual(first_due, pd.Timestamp("2026-07-26T11:00:00Z"))
        self.assertEqual(retry_due, pd.Timestamp("2026-07-26T15:00:00Z"))
        self.assertEqual(overdue_retry, pd.Timestamp("2026-07-26T15:30:00Z"))


if __name__ == "__main__":
    unittest.main()
