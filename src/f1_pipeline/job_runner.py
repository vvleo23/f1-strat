from __future__ import annotations

import argparse
import hashlib
import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Callable

from f1_pipeline.persistence import atomic_json
from f1_pipeline.planning import PURPOSES
from f1_pipeline.settings import CURATED_DATA_DIR
from f1_pipeline.sources.weekend_weather_pipeline import run_weekend_weather_pipeline

JOB_MANIFEST_DIR = CURATED_DATA_DIR / "manifests" / "jobs"
JOB_PURPOSES = frozenset({"weekend", "weekend_complete_v1", "replay"})
PipelineRunner = Callable[..., tuple[dict[str, Any], Path]]


class JobRunnerError(RuntimeError):
    pass


class JobAlreadyRunningError(JobRunnerError):
    pass


@dataclass(frozen=True)
class WeekendJobIntent:
    season: int
    meeting_key: int
    purpose: str
    target_session_key: int | None
    decision_time: str
    refresh: bool = False
    session_keys: tuple[int, ...] = ()

    def normalized(self) -> WeekendJobIntent:
        purpose = self.purpose.strip().casefold()
        if purpose not in JOB_PURPOSES or purpose not in PURPOSES:
            raise JobRunnerError(
                f"Dashboard jobs require one of: {', '.join(sorted(JOB_PURPOSES))}."
            )
        if self.season < 1950:
            raise JobRunnerError("Season must be 1950 or later.")
        if self.meeting_key <= 0:
            raise JobRunnerError("Meeting key must be positive.")
        if purpose == "replay" and self.target_session_key is None:
            raise JobRunnerError("Replay jobs require a target session key.")
        return WeekendJobIntent(
            season=int(self.season),
            meeting_key=int(self.meeting_key),
            purpose=purpose,
            target_session_key=(
                int(self.target_session_key)
                if self.target_session_key is not None
                else None
            ),
            decision_time=str(self.decision_time).strip(),
            refresh=bool(self.refresh),
            session_keys=tuple(sorted({int(value) for value in self.session_keys})),
        )


def job_id(intent: WeekendJobIntent) -> str:
    normalized = intent.normalized()
    payload = json.dumps(asdict(normalized), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def job_status_path(identifier: str, job_dir: Path = JOB_MANIFEST_DIR) -> Path:
    return job_dir / f"job_{identifier}.json"


def read_job_status(identifier: str, job_dir: Path = JOB_MANIFEST_DIR) -> dict[str, Any] | None:
    path = job_status_path(identifier, job_dir)
    if not path.is_file():
        return None
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise JobRunnerError(f"Could not read job status {identifier}: {exc}") from exc
    return payload if isinstance(payload, dict) else None


def run_weekend_job(
        intent: WeekendJobIntent,
        *,
        job_dir: Path = JOB_MANIFEST_DIR,
        pipeline_runner: PipelineRunner = run_weekend_weather_pipeline,
) -> dict[str, Any]:
    normalized = intent.normalized()
    identifier = job_id(normalized)
    status_path = job_status_path(identifier, job_dir)
    existing = read_job_status(identifier, job_dir)
    if existing and existing.get("status") in {"available", "stale"} and not normalized.refresh:
        return existing
    job_dir.mkdir(parents=True, exist_ok=True)
    lock_path = job_dir / f"job_{identifier}.lock"
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise JobAlreadyRunningError(f"Job {identifier} is already running.") from exc
    os.close(descriptor)
    intent_payload = asdict(normalized)
    intent_payload["session_keys"] = list(normalized.session_keys)
    running = {
        "schema_version": 1,
        "job_id": identifier,
        "status": "running",
        "intent": intent_payload,
    }
    atomic_json(running, status_path)
    try:
        manifest, manifest_path = pipeline_runner(
            season=normalized.season,
            meeting_key=normalized.meeting_key,
            purpose=normalized.purpose,
            target_session_key=normalized.target_session_key,
            decision_time=normalized.decision_time,
            refresh=normalized.refresh,
            session_keys=normalized.session_keys or None,
        )
        completed = {
            **running,
            "status": str(manifest.get("status", "unavailable")),
            "pipeline_manifest_path": str(manifest_path),
            "pipeline_run_id": manifest.get("run_id"),
        }
    except Exception as exc:
        completed = {**running, "status": "unavailable", "error": str(exc)}
    finally:
        lock_path.unlink(missing_ok=True)
    atomic_json(completed, status_path)
    return completed


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a controlled Dashboard V1 data job.")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--meeting-key", type=int, required=True)
    parser.add_argument("--purpose", choices=sorted(JOB_PURPOSES), required=True)
    parser.add_argument("--target-session-key", type=int)
    parser.add_argument("--decision-time", required=True)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--include-session-key", type=int, action="append", default=[])
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_argument_parser().parse_args(argv)
    result = run_weekend_job(
        WeekendJobIntent(
            season=args.season,
            meeting_key=args.meeting_key,
            purpose=args.purpose,
            target_session_key=args.target_session_key,
            decision_time=args.decision_time,
            refresh=args.refresh,
            session_keys=tuple(args.include_session_key),
        )
    )
    print(json.dumps(result, indent=2))
    return 0 if result["status"] in {"available", "stale", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())

