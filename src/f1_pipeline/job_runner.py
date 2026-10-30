# AI assistance: Anthropic Claude
# Scope: Incremental job progress recording.
from __future__ import annotations

import argparse
from contextlib import ExitStack
import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from f1_pipeline.analysis.qualifying_prediction import build_qualifying_prediction
from f1_pipeline.analysis.race_strategy import build_race_strategy
from f1_pipeline.persistence import atomic_json, file_lock
from f1_pipeline.planning import PURPOSES
from f1_pipeline.settings import CURATED_DATA_DIR
from f1_pipeline.sources.weekend_weather_pipeline import run_weekend_weather_pipeline

JOB_MANIFEST_DIR = CURATED_DATA_DIR / "manifests" / "jobs"
MIN_OPENF1_SEASON = 2023
JOB_PURPOSES = frozenset(
    {
        "forecast",
        "weekend",
        "weekend_complete_v1",
        "replay",
        "qualifying_prediction",
        "race_strategy",
    }
)
PipelineRunner = Callable[..., tuple[dict[str, Any], Path]]
CalculationRunner = Callable[[dict[str, Any], Path], tuple[dict[str, Any], Path]]
StrategyRunner = Callable[..., tuple[dict[str, Any], Path]]


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
    focus_driver: int | None = None

    def normalized(self) -> WeekendJobIntent:
        purpose = self.purpose.strip().casefold()
        if purpose not in JOB_PURPOSES or purpose not in PURPOSES:
            raise JobRunnerError(
                f"Dashboard jobs require one of: {', '.join(sorted(JOB_PURPOSES))}."
            )
        if self.season < MIN_OPENF1_SEASON:
            raise JobRunnerError(
                f"OpenF1-backed jobs require season {MIN_OPENF1_SEASON} or later."
            )
        if self.meeting_key <= 0:
            raise JobRunnerError("Meeting key must be positive.")
        if (
            purpose in {"forecast", "replay", "qualifying_prediction", "race_strategy"}
            and self.target_session_key is None
        ):
            raise JobRunnerError(f"{purpose} jobs require a target session key.")
        if purpose == "race_strategy" and self.focus_driver is None:
            raise JobRunnerError("race_strategy jobs require a focus driver.")
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
            focus_driver=(int(self.focus_driver) if self.focus_driver is not None else None),
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


def _progress_path(identifier: str, job_dir: Path = JOB_MANIFEST_DIR) -> Path:
    return job_dir / f"job_{identifier}.progress.json"


def read_job_progress(identifier: str, job_dir: Path = JOB_MANIFEST_DIR) -> list[dict[str, Any]]:
    """Read the incremental progress events recorded for one job, if any.

    Absent, unreadable, or malformed files are treated as "no events yet"
    rather than an error -- a job that has not reached its first progress
    callback (or predates this feature) simply has nothing to show.
    """
    path = _progress_path(identifier, job_dir)
    if not path.is_file():
        return []
    try:
        with path.open(encoding="utf-8") as stream:
            payload = json.load(stream)
    except (OSError, json.JSONDecodeError):
        return []
    events = payload.get("events") if isinstance(payload, dict) else None
    return events if isinstance(events, list) else []


def _write_progress(path: Path, payload: dict[str, Any]) -> None:
    # Progress is display-only; a locked or unwritable file must not fail the job.
    try:
        atomic_json(payload, path)
    except OSError:
        pass


def _make_progress_recorder(
        identifier: str, job_dir: Path
) -> Callable[[dict[str, Any]], None]:
    path = _progress_path(identifier, job_dir)

    def record(event: dict[str, Any]) -> None:
        events = read_job_progress(identifier, job_dir)
        events.append({"recorded_at": datetime.now(timezone.utc).isoformat(), **event})
        _write_progress(path, {"schema_version": 1, "job_id": identifier, "events": events})

    return record


def run_weekend_job(
        intent: WeekendJobIntent,
        *,
        job_dir: Path = JOB_MANIFEST_DIR,
        pipeline_runner: PipelineRunner = run_weekend_weather_pipeline,
        calculation_runner: CalculationRunner = build_qualifying_prediction,
        strategy_runner: StrategyRunner = build_race_strategy,
) -> dict[str, Any]:
    normalized = intent.normalized()
    identifier = job_id(normalized)
    status_path = job_status_path(identifier, job_dir)
    existing = read_job_status(identifier, job_dir)
    if existing and existing.get("status") in {"available", "stale"} and not normalized.refresh:
        return existing
    job_dir.mkdir(parents=True, exist_ok=True)
    lock_context = ExitStack()
    try:
        lock_context.enter_context(file_lock(job_dir / f"job_{identifier}", timeout_seconds=0.0))
    except TimeoutError as exc:
        raise JobAlreadyRunningError(f"Job {identifier} is already running.") from exc
    intent_payload = asdict(normalized)
    intent_payload["session_keys"] = list(normalized.session_keys)
    running = {
        "schema_version": 1,
        "job_id": identifier,
        "status": "running",
        "intent": intent_payload,
    }
    progress = _make_progress_recorder(identifier, job_dir)
    manifest = None
    manifest_path = None
    completed = running
    try:
        atomic_json(running, status_path)
        _write_progress(
            _progress_path(identifier, job_dir),
            {"schema_version": 1, "job_id": identifier, "events": []},
        )
        with file_lock(job_dir / "source_pipeline", timeout_seconds=900.0):
            manifest, manifest_path = pipeline_runner(
                season=normalized.season,
                meeting_key=normalized.meeting_key,
                purpose=normalized.purpose,
                target_session_key=normalized.target_session_key,
                decision_time=normalized.decision_time,
                refresh=normalized.refresh,
                session_keys=normalized.session_keys or None,
                on_progress=progress,
            )
        calculation = None
        calculation_path = None
        if normalized.purpose == "qualifying_prediction":
            calculation, calculation_path = calculation_runner(
                manifest, manifest_path
            )
        elif normalized.purpose == "race_strategy":
            calculation, calculation_path = strategy_runner(
                manifest,
                manifest_path,
                focus_driver=int(normalized.focus_driver),
            )
        completed = {
            **running,
            "status": str(
                calculation.get("status", "unavailable")
                if calculation is not None
                else manifest.get("status", "unavailable")
            ),
            "pipeline_manifest_path": str(manifest_path),
            "pipeline_run_id": manifest.get("run_id"),
        }
        if calculation is not None and calculation_path is not None:
            completed["calculation_manifest_path"] = str(calculation_path)
            completed["calculation_id"] = calculation.get(
                "calculation_id", calculation.get("strategy_run_id")
            )
            if calculation.get("status") == "unavailable":
                diagnostics = calculation.get("diagnostics", {})
                errors = (
                    diagnostics.get("errors", [])
                    if isinstance(diagnostics, dict)
                    else []
                )
                if errors:
                    completed["error"] = str(errors[0])
    except Exception as exc:
        completed = {**running, "status": "unavailable", "error": str(exc)}
        if manifest is not None and manifest_path is not None:
            completed["pipeline_manifest_path"] = str(manifest_path)
            completed["pipeline_run_id"] = manifest.get("run_id")
    finally:
        try:
            atomic_json(completed, status_path)
        finally:
            lock_context.close()
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
    parser.add_argument("--focus-driver", type=int)
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
            focus_driver=args.focus_driver,
        )
    )
    print(json.dumps(result, indent=2))
    return 0 if result["status"] in {"available", "stale", "partial"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
