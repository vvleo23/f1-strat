from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from f1_pipeline.job_runner import (
    JobAlreadyRunningError,
    JobRunnerError,
    WeekendJobIntent,
    job_id,
    read_job_status,
    read_job_progress,
    run_weekend_job,
)


class JobRunnerTest(unittest.TestCase):
    def intent(self, *, refresh: bool = False) -> WeekendJobIntent:
        return WeekendJobIntent(
            season=2026,
            meeting_key=1291,
            purpose="weekend_complete_v1",
            target_session_key=11342,
            decision_time="2026-07-26T16:00:00Z",
            refresh=refresh,
        )

    def test_job_id_is_deterministic_and_success_is_reused(self) -> None:
        calls: list[dict[str, object]] = []

        def pipeline(**kwargs: object):
            calls.append(kwargs)
            return {"status": "available", "run_id": "run-1"}, Path("manifest.json")

        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            first = run_weekend_job(
                self.intent(), job_dir=job_dir, pipeline_runner=pipeline
            )
            second = run_weekend_job(
                self.intent(), job_dir=job_dir, pipeline_runner=pipeline
            )

            self.assertEqual(job_id(self.intent()), first["job_id"])
            self.assertEqual(first, second)
            self.assertEqual(len(calls), 1)
            self.assertEqual(first["status"], "available")

    def test_existing_lock_prevents_parallel_run(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            identifier = job_id(self.intent())
            (job_dir / f"job_{identifier}.lock").write_text("", encoding="utf-8")

            with self.assertRaises(JobAlreadyRunningError):
                run_weekend_job(self.intent(), job_dir=job_dir)

    def test_pipeline_error_is_persisted_as_unavailable(self) -> None:
        def failing_pipeline(**kwargs: object):
            raise RuntimeError("provider failed")

        with tempfile.TemporaryDirectory() as temporary:
            result = run_weekend_job(
                self.intent(),
                job_dir=Path(temporary),
                pipeline_runner=failing_pipeline,
            )

            self.assertEqual(result["status"], "unavailable")
            self.assertEqual(result["error"], "provider failed")
            self.assertEqual(read_job_status(result["job_id"], Path(temporary)), result)

    def test_calculation_jobs_persist_reference_and_inputs(self) -> None:
        for purpose, session, focus, identifier_key in (
            ("qualifying_prediction", 11338, None, "calculation_id"),
            ("race_strategy", 11342, 4, "strategy_run_id"),
        ):
            with self.subTest(purpose=purpose), tempfile.TemporaryDirectory() as temporary:
                intent = WeekendJobIntent(
                    season=2026, meeting_key=1291, purpose=purpose,
                    target_session_key=session, focus_driver=focus,
                    decision_time="2026-07-26T16:00:00Z",
                )
                manifest = {"status": "partial", "run_id": "run-2"}
                calls = []

                def pipeline(**kwargs: object):
                    return manifest, Path("pipeline.json")

                def calculation(inputs, path, *, focus_driver=None):
                    calls.append((inputs, path, focus_driver))
                    return (
                        {"status": "available", identifier_key: "calculation-1"},
                        Path("calculation.json"),
                    )

                result = run_weekend_job(
                    intent, job_dir=Path(temporary), pipeline_runner=pipeline,
                    calculation_runner=calculation, strategy_runner=calculation,
                )
                self.assertEqual(result["status"], "available")
                self.assertEqual(result["calculation_id"], "calculation-1")
                self.assertEqual(result["calculation_manifest_path"], "calculation.json")
                self.assertEqual(calls, [(manifest, Path("pipeline.json"), focus)])
                self.assertEqual(read_job_status(result["job_id"], Path(temporary)), result)

    def test_strategy_job_requires_focus_driver(self) -> None:
        intent = WeekendJobIntent(
            season=2026,
            meeting_key=1291,
            purpose="race_strategy",
            target_session_key=11342,
            decision_time="2026-07-26T16:00:00Z",
        )

        with self.assertRaisesRegex(JobRunnerError, "focus driver"):
            intent.normalized()

    def test_openf1_jobs_reject_unsupported_seasons(self) -> None:
        intent = WeekendJobIntent(
            season=2022,
            meeting_key=1,
            purpose="weekend",
            target_session_key=None,
            decision_time="2022-01-01T00:00:00Z",
        )

        with self.assertRaisesRegex(JobRunnerError, "2023 or later"):
            intent.normalized()

    def test_pipeline_runner_receives_a_working_on_progress_callback(self) -> None:
        def pipeline(*, on_progress, **kwargs: object):
            on_progress({"stage": "openf1_endpoint", "endpoint": "sessions", "status": "available"})
            on_progress({"stage": "openf1_endpoint", "endpoint": "laps", "status": "available"})
            return {"status": "available", "run_id": "run-3"}, Path("manifest.json")

        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            result = run_weekend_job(
                self.intent(), job_dir=job_dir, pipeline_runner=pipeline
            )

            events = read_job_progress(result["job_id"], job_dir)
            self.assertEqual([event["endpoint"] for event in events], ["sessions", "laps"])
            self.assertIn("recorded_at", events[0])

    def test_progress_file_is_reset_between_runs_of_the_same_job(self) -> None:
        calls = {"count": 0}

        def pipeline(*, on_progress, **kwargs: object):
            calls["count"] += 1
            on_progress({"stage": "openf1_endpoint", "endpoint": f"run-{calls['count']}"})
            return {"status": "partial", "run_id": f"run-{calls['count']}"}, Path("manifest.json")

        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            first = run_weekend_job(
                self.intent(refresh=True), job_dir=job_dir, pipeline_runner=pipeline
            )
            second = run_weekend_job(
                self.intent(refresh=True), job_dir=job_dir, pipeline_runner=pipeline
            )

            events = read_job_progress(second["job_id"], job_dir)
            self.assertEqual([event["endpoint"] for event in events], ["run-2"])

    def test_unwritable_progress_file_does_not_fail_the_job(self) -> None:
        def pipeline(*, on_progress, **kwargs: object):
            on_progress({"stage": "openf1_endpoint", "endpoint": "laps", "status": "available"})
            return {"status": "available", "run_id": "run-4"}, Path("manifest.json")

        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            identifier = job_id(self.intent())
            # A directory in place of the progress file makes every write fail.
            (job_dir / f"job_{identifier}.progress.json").mkdir()

            result = run_weekend_job(
                self.intent(), job_dir=job_dir, pipeline_runner=pipeline
            )

            self.assertEqual(result["status"], "available")
            self.assertEqual(read_job_progress(identifier, job_dir), [])

    def test_read_job_progress_returns_empty_list_when_absent(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            self.assertEqual(read_job_progress("missing", Path(temporary)), [])


if __name__ == "__main__":
    unittest.main()
