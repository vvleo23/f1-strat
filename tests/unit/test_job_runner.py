from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from f1_pipeline.job_runner import (
    JobAlreadyRunningError,
    WeekendJobIntent,
    job_id,
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


if __name__ == "__main__":
    unittest.main()

