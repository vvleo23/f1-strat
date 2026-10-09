from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from f1_pipeline.scheduler import (
    _next_due_from_sessions,
    _run_forecast_slot,
    _run_session_attempt,
    run_scheduler_once,
)


class SchedulerTest(unittest.TestCase):
    def test_runner_failure_is_persisted_and_other_sessions_continue(self) -> None:
        first = self.session()
        second = first.copy()
        second["session_id"] = "openf1:session:43"
        calls = []

        def runner(intent, *, job_dir):
            calls.append(intent.target_session_key)
            if intent.target_session_key == 42:
                raise RuntimeError("controlled provider failure")
            return {"job_id": "second-job", "status": "available"}

        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            with patch("f1_pipeline.scheduler._sessions", return_value=pd.DataFrame([first, second])):
                result = run_scheduler_once(
                    2026, now="2026-07-26T11:00:00Z", include_forecasts=False,
                    schedule_dir=root / "schedules", job_dir=root / "jobs", runner=runner,
                )
            state = json.loads((root / "schedules/session_2026_42.json").read_text())
            self.assertEqual(calls, [42, 43])
            self.assertEqual([item["status"] for item in result["sessions"]], ["unavailable", "available"])
            self.assertEqual(state["attempts"][0]["error"], "controlled provider failure")
            self.assertEqual(state["next_attempt_at"], "2026-07-26T15:00:00+00:00")

    def test_calendar_refresh_postpones_session_before_ingestion(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calendar = root / "sessions.parquet"
            pd.DataFrame([self.session()]).to_parquet(calendar)

            def refresh(season, *, refresh):
                self.assertTrue(refresh)
                postponed = self.session()
                postponed["scheduled_end_utc"] = "2026-07-26T13:00:00Z"
                pd.DataFrame([postponed]).to_parquet(calendar)
                return {"session": calendar}

            with (
                patch("f1_pipeline.scheduler.master_table_path", return_value=calendar),
                patch("f1_pipeline.scheduler.load_master_data", side_effect=refresh),
            ):
                result = run_scheduler_once(
                    2026, now="2026-07-26T11:00:00Z", refresh_calendar=True,
                    include_forecasts=False, schedule_dir=root / "schedules", job_dir=root / "jobs",
                    runner=lambda *args, **kwargs: self.fail("Postponed session must not load yet"),
                )
            self.assertEqual(result["sessions"], [])
            self.assertEqual(result["errors"], [])

    def test_calendar_failure_preserves_local_calendar_and_records_fallback(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            calendar = root / "sessions.parquet"
            pd.DataFrame([self.session()]).to_parquet(calendar)
            original = calendar.read_bytes()
            with (
                patch("f1_pipeline.scheduler.master_table_path", return_value=calendar),
                patch("f1_pipeline.scheduler.load_master_data", side_effect=RuntimeError("API blocked")),
            ):
                result = run_scheduler_once(
                    2026, now="2026-07-26T11:00:00Z", refresh_calendar=True,
                    include_forecasts=False, schedule_dir=root / "schedules", job_dir=root / "jobs",
                    runner=lambda *args, **kwargs: {"job_id": "fallback", "status": "available"},
                )
            status = json.loads((root / "schedules/calendar_2026.json").read_text())
            self.assertEqual(status["status"], "stale")
            self.assertEqual(status["error"], "API blocked")
            self.assertEqual(calendar.read_bytes(), original)
            self.assertEqual(result["sessions"][0]["status"], "available")

    def test_corrupt_session_state_does_not_stop_other_sessions_or_next_event(self) -> None:
        first = self.session()
        second = first.copy()
        second["session_id"] = "openf1:session:43"
        sessions = pd.DataFrame([first, second])
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            schedules = root / "schedules"
            schedules.mkdir()
            corrupt = schedules / "session_2026_42.json"
            corrupt.write_text("broken state", encoding="utf-8")
            with patch("f1_pipeline.scheduler._sessions", return_value=sessions):
                result = run_scheduler_once(
                    2026, now="2026-07-26T11:00:00Z", include_forecasts=False,
                    schedule_dir=schedules, job_dir=root / "jobs",
                    runner=lambda *args, **kwargs: {"job_id": "other", "status": "available"},
                )
            self.assertEqual(result["sessions"][0]["session_key"], 43)
            self.assertEqual(result["errors"][0]["session_id"], "openf1:session:42")
            self.assertEqual(corrupt.read_text(), "broken state")
            self.assertIsNone(_next_due_from_sessions(
                sessions, season=2026, now=pd.Timestamp("2026-07-26T11:00:00Z"),
                include_forecasts=False, schedule_dir=schedules,
            ))

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
