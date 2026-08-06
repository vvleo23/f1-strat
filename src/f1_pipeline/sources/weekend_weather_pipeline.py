from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

import pandas as pd

from f1_pipeline.master_data import MasterDataError, load_master_data
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.settings import CURATED_DATA_DIR, PROJECT_ROOT
from f1_pipeline.sources.open_meteo import OpenMeteoError, load_forecast
from f1_pipeline.sources.openf1_weekend import (
    OpenF1WeekendError,
    ingest_weekend,
    normalize_session_type,
)
from f1_pipeline.sources.wikidata import (
    CircuitReference,
    MissingCircuitMappingError,
    WikidataError,
    discover_circuit_candidates,
    load_circuit_reference,
)

DEFAULT_SEASON = 2026
DEFAULT_MEETING_KEY = 1291
DEFAULT_SESSION_KEY = 11342
DEFAULT_RUN_INITIALIZED_AT = "2026-07-26T00:00:00Z"
DEFAULT_AVAILABLE_AT = "2026-07-26T06:00:00Z"
DEFAULT_DECISION_TIME = "2026-07-26T12:00:00Z"
MANIFEST_SCHEMA_VERSION = 3


class WeekendWeatherPipelineError(RuntimeError):
    pass


MasterLoader = Callable[[int, bool], dict[str, Path]]
ReferenceLoader = Callable[..., tuple[CircuitReference, dict[str, Any]]]
ForecastLoader = Callable[..., tuple[pd.DataFrame, dict[str, Any]]]
WeekendLoader = Callable[..., tuple[dict[str, Any], Path]]


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _load_master_outputs(season: int, refresh: bool) -> dict[str, Path]:
    dimensions = CURATED_DATA_DIR / "dimensions"
    outputs = {
        "meeting": dimensions / "meeting.parquet",
        "session": dimensions / "session.parquet",
        "circuit": dimensions / "circuit.parquet",
        "manifest": CURATED_DATA_DIR / "manifests" / f"master_data_{season}.json",
    }
    if refresh or not all(path.exists() for path in outputs.values()):
        return load_master_data(season, refresh=refresh)
    return outputs


def _selected_context(
    outputs: dict[str, Path], meeting_key: int, session_key: int
) -> dict[str, Any]:
    meeting_id = f"openf1:meeting:{meeting_key}"
    session_id = f"openf1:session:{session_key}"
    meetings = pd.read_parquet(outputs["meeting"])
    sessions = pd.read_parquet(outputs["session"])
    circuits = pd.read_parquet(outputs["circuit"])
    meeting_rows = meetings[meetings["meeting_id"].eq(meeting_id)]
    session_rows = sessions[
        sessions["session_id"].eq(session_id)
        & sessions["meeting_id"].eq(meeting_id)
    ]
    if len(meeting_rows) != 1:
        raise WeekendWeatherPipelineError(f"Meeting {meeting_key} was not found uniquely.")
    if len(session_rows) != 1:
        raise WeekendWeatherPipelineError(
            f"Session {session_key} does not belong uniquely to meeting {meeting_key}."
        )
    meeting = meeting_rows.iloc[0]
    circuit_id = str(meeting["circuit_id"])
    circuit_rows = circuits[circuits["circuit_id"].eq(circuit_id)]
    if len(circuit_rows) != 1:
        raise WeekendWeatherPipelineError(f"Circuit {circuit_id} was not found uniquely.")
    circuit = circuit_rows.iloc[0]
    source_circuit_key = circuit["source_circuit_key"]
    if pd.isna(source_circuit_key):
        raise WeekendWeatherPipelineError(f"Circuit {circuit_id} has no OpenF1 circuit key.")
    weekend_sessions = sessions[sessions["meeting_id"].eq(meeting_id)].sort_values(
        "scheduled_start_utc"
    )
    if weekend_sessions.empty:
        raise WeekendWeatherPipelineError(f"Meeting {meeting_key} has no discovered sessions.")
    session_records: list[dict[str, Any]] = []
    for row in weekend_sessions.itertuples(index=False):
        scheduled_start = getattr(row, "scheduled_start_utc")
        session_records.append(
            {
                "session_id": str(getattr(row, "session_id")),
                "source_session_key": int(
                    str(getattr(row, "session_id")).rsplit(":", 1)[-1]
                ),
                "session_type": str(getattr(row, "session_type")),
                "session_name": str(getattr(row, "session_name")),
                "scheduled_start_utc": (
                    scheduled_start.isoformat() if pd.notna(scheduled_start) else None
                ),
                "status": str(getattr(row, "status")),
            }
        )
    return {
        "meeting_id": meeting_id,
        "session_id": session_id,
        "circuit_id": circuit_id,
        "circuit_name": (
            str(circuit.get("circuit_name"))
            if pd.notna(circuit.get("circuit_name"))
            else circuit_id
        ),
        "circuit_location": (
            str(circuit.get("location")) if pd.notna(circuit.get("location")) else None
        ),
        "source_circuit_key": int(source_circuit_key),
        "session_count": len(weekend_sessions),
        "sessions": session_records,
    }


def _normalized_session_keys(session_keys: Sequence[int] | None) -> list[int] | None:
    if session_keys is None:
        return None
    if isinstance(session_keys, (str, bytes)) or not session_keys:
        raise WeekendWeatherPipelineError("Session key filter must not be empty.")
    normalized: set[int] = set()
    for session_key in session_keys:
        if isinstance(session_key, bool) or not isinstance(session_key, int) or session_key <= 0:
            raise WeekendWeatherPipelineError("Session keys must be positive integers.")
        normalized.add(session_key)
    return sorted(normalized)


def _normalized_session_types(session_types: Sequence[str] | None) -> list[str] | None:
    if session_types is None:
        return None
    if isinstance(session_types, (str, bytes)) or not session_types:
        raise WeekendWeatherPipelineError("Session type filter must not be empty.")
    normalized: set[str] = set()
    for session_type in session_types:
        if not isinstance(session_type, str) or not session_type.strip():
            raise WeekendWeatherPipelineError("Session types must be non-empty strings.")
        try:
            normalized.add(normalize_session_type(session_type.strip()))
        except OpenF1WeekendError as exc:
            raise WeekendWeatherPipelineError(str(exc)) from exc
    return sorted(normalized)


def select_weekend_sessions(
    sessions: list[dict[str, Any]],
    *,
    session_keys: Sequence[int] | None = None,
    session_types: Sequence[str] | None = None,
) -> list[dict[str, Any]]:
    normalized_keys = _normalized_session_keys(session_keys)
    normalized_types = _normalized_session_types(session_types)
    by_key: dict[int, dict[str, Any]] = {}
    for session in sessions:
        session_key = session.get("source_session_key")
        if isinstance(session_key, bool) or not isinstance(session_key, int):
            raise WeekendWeatherPipelineError("Discovered session has an invalid source key.")
        if session_key in by_key:
            raise WeekendWeatherPipelineError(f"Session key {session_key} was discovered more than once.")
        by_key[session_key] = session
    if normalized_keys is not None:
        missing = sorted(set(normalized_keys).difference(by_key))
        if missing:
            raise WeekendWeatherPipelineError(
                f"Requested session keys do not belong to the selected meeting: {missing}."
            )
    selected: list[dict[str, Any]] = []
    for session_key, session in by_key.items():
        if normalized_keys is not None and session_key not in normalized_keys:
            continue
        if str(session.get("status", "")).casefold() == "cancelled":
            if normalized_keys is not None and session_key in normalized_keys:
                raise WeekendWeatherPipelineError(
                    f"Requested session key {session_key} is cancelled."
                )
            continue
        if normalized_types is not None:
            try:
                current_type = normalize_session_type(
                    str(session.get("session_type", "")),
                    str(session.get("session_name", "")),
                )
            except OpenF1WeekendError as exc:
                raise WeekendWeatherPipelineError(str(exc)) from exc
            if current_type not in normalized_types:
                continue
        selected.append(session)
    if not selected:
        raise WeekendWeatherPipelineError("Session selection did not match any usable session.")
    return sorted(
        selected,
        key=lambda session: (
            session.get("scheduled_start_utc") is None,
            session.get("scheduled_start_utc") or "",
            session["source_session_key"],
        ),
    )


def _enrich_circuit(path: Path, reference: CircuitReference) -> None:
    circuits = pd.read_parquet(path)
    mask = circuits["source_circuit_key"].eq(reference.source_circuit_key)
    if int(mask.sum()) != 1:
        raise WeekendWeatherPipelineError(
            f"Circuit key {reference.source_circuit_key} was not found uniquely in Silver."
        )
    values = {
        "wikidata_entity_id": reference.wikidata_entity_id,
        "reference_latitude": reference.latitude,
        "reference_longitude": reference.longitude,
        "reference_crs": reference.crs,
        "coordinate_revision": reference.coordinate_revision,
        "coordinate_retrieved_at": reference.coordinate_retrieved_at,
        "coordinate_verification_status": reference.verification_status,
        "coordinate_raw_path": reference.raw_path,
        "coordinate_sha256": reference.raw_sha256,
    }
    enriched = circuits.copy()
    for column, value in values.items():
        if column not in enriched.columns:
            enriched[column] = None
        enriched.loc[mask, column] = value
    if enriched.equals(circuits):
        return
    atomic_parquet(enriched, path)


def _overall_status(jobs: dict[str, dict[str, Any]]) -> str:
    statuses = {job.get("status") for job in jobs.values()}
    if statuses.issubset({"available", "stale"}):
        return "stale" if "stale" in statuses else "available"
    if statuses.intersection({"available", "stale"}):
        return "partial"
    return "unavailable"


def run_weekend_weather_pipeline(
    *,
    season: int = DEFAULT_SEASON,
    meeting_key: int = DEFAULT_MEETING_KEY,
    session_key: int = DEFAULT_SESSION_KEY,
    run_initialized_at: str = DEFAULT_RUN_INITIALIZED_AT,
    available_at: str = DEFAULT_AVAILABLE_AT,
    decision_time: str = DEFAULT_DECISION_TIME,
    session_keys: Sequence[int] | None = None,
    session_types: Sequence[str] | None = None,
    refresh: bool = False,
    output_dir: Path = CURATED_DATA_DIR,
    master_loader: MasterLoader | None = None,
    reference_loader: ReferenceLoader | None = None,
    forecast_loader: ForecastLoader | None = None,
    weekend_loader: WeekendLoader | None = None,
) -> tuple[dict[str, Any], Path]:
    started_at = datetime.now(timezone.utc)
    jobs: dict[str, dict[str, Any]] = {}
    context: dict[str, Any] | None = None
    reference: CircuitReference | None = None
    outputs: dict[str, Path] = {}
    load_master = master_loader or _load_master_outputs
    load_reference = reference_loader or load_circuit_reference
    load_weather = forecast_loader or load_forecast
    load_weekend = weekend_loader or ingest_weekend
    normalized_session_keys = _normalized_session_keys(session_keys)
    normalized_session_types = _normalized_session_types(session_types)
    resolved_session_keys: list[int] = []

    try:
        outputs = load_master(season, refresh)
        selected_context = _selected_context(outputs, meeting_key, session_key)
    except (MasterDataError, WeekendWeatherPipelineError, OSError, ValueError, KeyError) as exc:
        jobs["openf1"] = {"status": "unavailable", "error": str(exc)}
    else:
        selected_sessions = select_weekend_sessions(
            selected_context["sessions"],
            session_keys=normalized_session_keys,
            session_types=normalized_session_types,
        )
        resolved_session_keys = [session["source_session_key"] for session in selected_sessions]
        context = {
            **selected_context,
            "discovered_session_count": selected_context["session_count"],
            "selected_session_count": len(selected_sessions),
            "sessions": selected_sessions,
        }
        jobs["openf1"] = {
            "status": "available" if refresh else "stale",
            "manifest_path": _relative(outputs["manifest"]),
            "manifest_sha256": sha256(outputs["manifest"]),
            "session_count": context["discovered_session_count"],
        }

    if context is None:
        jobs["openf1_weekend_facts"] = {
            "status": "unavailable",
            "error": "OpenF1 session context is unavailable.",
        }
    else:
        try:
            weekend_result, weekend_path = load_weekend(
                context["sessions"],
                meeting_key=meeting_key,
                refresh=refresh,
                curated_dir=output_dir,
            )
            jobs["openf1_weekend_facts"] = {
                "status": weekend_result["status"],
                "manifest_path": _relative(weekend_path),
                "manifest_sha256": sha256(weekend_path),
                "sessions": weekend_result["sessions"],
            }
        except (OpenF1WeekendError, OSError, ValueError, KeyError) as exc:
            jobs["openf1_weekend_facts"] = {
                "status": "unavailable",
                "error": str(exc),
            }

    if context is None:
        jobs["wikidata"] = {
            "status": "unavailable",
            "error": "OpenF1 circuit context is unavailable.",
        }
    else:
        try:
            loaded_reference, result = load_reference(
                context["source_circuit_key"], refresh=refresh
            )
            reference = loaded_reference
            _enrich_circuit(outputs["circuit"], loaded_reference)
            jobs["wikidata"] = {
                **result,
                "reference": loaded_reference.to_dict(),
                "curated_path": _relative(outputs["circuit"]),
                "curated_sha256": sha256(outputs["circuit"]),
            }
        except MissingCircuitMappingError as exc:
            try:
                jobs["wikidata"] = discover_circuit_candidates(
                    context["source_circuit_key"],
                    context["circuit_name"],
                    location=context["circuit_location"],
                )
            except (WikidataError, OSError, ValueError, KeyError) as search_exc:
                jobs["wikidata"] = {
                    "status": "unavailable",
                    "error": str(search_exc),
                    "mapping": exc.mapping,
                }
        except (WikidataError, WeekendWeatherPipelineError, OSError, ValueError, KeyError) as exc:
            jobs["wikidata"] = {"status": "unavailable", "error": str(exc)}

    if context is None or reference is None:
        jobs["open_meteo"] = {
            "status": "unavailable",
            "error": "A verified circuit reference is unavailable.",
        }
    else:
        try:
            forecast, result = load_weather(
                reference,
                session_id=context["session_id"],
                circuit_id=context["circuit_id"],
                run_initialized_at=run_initialized_at,
                available_at=available_at,
                decision_time=decision_time,
                refresh=refresh,
            )
            snapshot_id = str(forecast.iloc[0]["snapshot_id"]).replace(":", "_")
            forecast_path = (
                output_dir
                / "facts"
                / "weather_forecast"
                / f"{snapshot_id}.parquet"
            )
            if not forecast_path.exists():
                atomic_parquet(forecast, forecast_path)
            jobs["open_meteo"] = {
                **result,
                "curated_path": _relative(forecast_path),
                "curated_sha256": sha256(forecast_path),
            }
        except (OpenMeteoError, OSError, ValueError, KeyError) as exc:
            jobs["open_meteo"] = {"status": "unavailable", "error": str(exc)}

    identity = json.dumps(
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            "season": season,
            "meeting_key": meeting_key,
            "session_key": session_key,
            "run_initialized_at": run_initialized_at,
            "available_at": available_at,
            "decision_time": decision_time,
            "session_keys": normalized_session_keys,
            "session_types": normalized_session_types,
            "resolved_session_keys": resolved_session_keys,
            "circuit_mapping": {
                "schema_version": jobs.get("wikidata", {}).get("mapping", {}).get("schema_version"),
                "sha256": jobs.get("wikidata", {}).get("mapping", {}).get("sha256"),
            },
            "jobs": {
                name: {
                    "raw_sha256": job.get("raw_sha256"),
                    "curated_sha256": job.get("curated_sha256"),
                    "manifest_sha256": job.get("manifest_sha256"),
                }
                for name, job in jobs.items()
            },
        },
        sort_keys=True,
    )
    run_id = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "run_id": run_id,
        "pipeline": "f1_wikidata_open_meteo_weekend_weather_pipeline",
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "status": _overall_status(jobs),
        "selection": {
            "season": season,
            "meeting_key": meeting_key,
            "session_key": session_key,
            "run_initialized_at": run_initialized_at,
            "available_at": available_at,
            "decision_time": decision_time,
            "session_keys": normalized_session_keys,
            "session_types": normalized_session_types,
            "resolved_session_keys": resolved_session_keys,
        },
        "context": context,
        "jobs": jobs,
    }
    manifest_path = (
        output_dir
        / "manifests"
        / f"weekend_weather_pipeline_{meeting_key}_{run_id}.json"
    )
    if manifest_path.exists() and not refresh:
        with manifest_path.open(encoding="utf-8") as stream:
            return json.load(stream), manifest_path
    atomic_json(manifest, manifest_path)
    return manifest, manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run the F1-Wikidata-Open-Meteo weekend weather pipeline."
    )
    parser.add_argument("--season", type=int, default=DEFAULT_SEASON)
    parser.add_argument("--meeting-key", type=int, default=DEFAULT_MEETING_KEY)
    parser.add_argument("--session-key", type=int, default=DEFAULT_SESSION_KEY)
    parser.add_argument("--run-initialized-at", default=DEFAULT_RUN_INITIALIZED_AT)
    parser.add_argument("--available-at", default=DEFAULT_AVAILABLE_AT)
    parser.add_argument("--decision-time", default=DEFAULT_DECISION_TIME)
    parser.add_argument(
        "--include-session-key", type=int, action="append", dest="session_keys"
    )
    parser.add_argument(
        "--include-session-type", action="append", dest="session_types"
    )
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--strict", action="store_true")
    args = parser.parse_args(argv)
    manifest, path = run_weekend_weather_pipeline(
        season=args.season,
        meeting_key=args.meeting_key,
        session_key=args.session_key,
        run_initialized_at=args.run_initialized_at,
        available_at=args.available_at,
        decision_time=args.decision_time,
        session_keys=args.session_keys,
        session_types=args.session_types,
        refresh=args.refresh,
    )
    print(f"Weekend-weather manifest: {path}")
    print(f"Status: {manifest['status']}")
    return 1 if args.strict and manifest["status"] not in {"available", "stale"} else 0


if __name__ == "__main__":
    raise SystemExit(main())







