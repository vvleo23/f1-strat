from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence, cast

import pandas as pd

from f1_pipeline.geometry import (
    TrackGeometryError,
    build_manifest_geometry,
)
from f1_pipeline.master_data import MasterDataError, load_master_data, master_table_path
from f1_pipeline.planning import (
    PURPOSES,
    SessionPlanningError,
    plan_sessions_for_purpose,
)
from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.settings import CURATED_DATA_DIR, PROJECT_ROOT
from f1_pipeline.sources.open_meteo import (
    OpenMeteoError,
    load_forecast,
    select_historical_single_run,
    utc_timestamp,
    validate_forecast_horizon,
)
from f1_pipeline.sources.openf1 import OpenF1Client, OpenF1Error
from f1_pipeline.sources.openf1_weekend import (
    OpenF1WeekendError,
    ingest_weekend,
    normalize_session_type,
)
from f1_pipeline.sources.wikidata import (
    CircuitResolutionError,
    CircuitReference,
    MissingCircuitMappingError,
    WikidataError,
    discover_circuit_candidates,
    resolve_circuit_reference,
)

DEFAULT_SEASON = 2026
MANIFEST_SCHEMA_VERSION = 6


class WeekendWeatherPipelineError(RuntimeError):
    pass


MasterLoader = Callable[[int, bool], dict[str, Path]]
ReferenceLoader = Callable[..., tuple[CircuitReference, dict[str, Any]]]
ForecastLoader = Callable[..., tuple[pd.DataFrame, dict[str, Any]]]
WeekendLoader = Callable[..., tuple[dict[str, Any], Path]]
GeometryBuilder = Callable[..., tuple[dict[str, Any], Path]]


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path)


def _load_master_outputs(
        season: int,
        refresh: bool,
        client: OpenF1Client | None = None,
) -> dict[str, Path]:
    outputs = {
        "country": master_table_path("country", season),
        "meeting": master_table_path("meeting", season),
        "session": master_table_path("session", season),
        "circuit": master_table_path("circuit", season),
        "manifest": CURATED_DATA_DIR / "manifests" / f"master_data_{season}.json",
    }
    if not refresh and all(path.exists() for path in outputs.values()):
        return outputs
    legacy_dimensions = CURATED_DATA_DIR / "dimensions"
    legacy = {
        **outputs,
        "country": legacy_dimensions / "country.parquet",
        "meeting": legacy_dimensions / "meeting.parquet",
        "session": legacy_dimensions / "session.parquet",
        "circuit": legacy_dimensions / "circuit.parquet",
    }
    if season == 2026 and not refresh and all(path.exists() for path in legacy.values()):
        return legacy
    return load_master_data(season, refresh=refresh, client=client)


def _selected_meeting_context(outputs: dict[str, Path], meeting_key: int) -> dict[str, Any]:
    meeting_id = f"openf1:meeting:{meeting_key}"
    meetings = pd.read_parquet(outputs["meeting"])
    sessions = pd.read_parquet(outputs["session"])
    circuits = pd.read_parquet(outputs["circuit"])
    meeting_rows = meetings[meetings["meeting_id"].eq(meeting_id)]
    if len(meeting_rows) != 1:
        raise WeekendWeatherPipelineError(f"Meeting {meeting_key} was not found uniquely.")
    meeting = meeting_rows.iloc[0]
    circuit_id = str(meeting["circuit_id"])
    circuit_rows = circuits[circuits["circuit_id"].eq(circuit_id)]
    if len(circuit_rows) != 1:
        raise WeekendWeatherPipelineError(f"Circuit {circuit_id} was not found uniquely.")
    circuit = circuit_rows.iloc[0]
    country_name = None
    country_path = outputs.get("country")
    country_id = circuit.get("country_id")
    if country_path is not None and country_path.exists() and pd.notna(country_id):
        countries = pd.read_parquet(country_path)
        country_rows = countries[countries["country_id"].eq(str(country_id))]
        if len(country_rows) == 1 and pd.notna(country_rows.iloc[0].get("country_name")):
            country_name = str(country_rows.iloc[0]["country_name"])
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
        scheduled_end = getattr(row, "scheduled_end_utc")
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
                "scheduled_end_utc": (
                    scheduled_end.isoformat() if pd.notna(scheduled_end) else None
                ),
                "status": str(getattr(row, "status")),
            }
        )
    return {
        "meeting_id": meeting_id,
        "circuit_id": circuit_id,
        "circuit_name": (
            str(circuit.get("circuit_name"))
            if pd.notna(circuit.get("circuit_name"))
            else circuit_id
        ),
        "circuit_location": (
            str(circuit.get("location")) if pd.notna(circuit.get("location")) else None
        ),
        "circuit_country": country_name,
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


def _enrich_circuit(
        path: Path,
        reference: CircuitReference,
        legacy_path: Path | None = None,
) -> None:
    circuits = pd.read_parquet(path)
    mask = circuits["source_circuit_key"].eq(reference.source_circuit_key)
    if len(circuits.loc[mask]) != 1:
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
    if not enriched.equals(circuits):
        atomic_parquet(enriched, path)
    if legacy_path is not None:
        if legacy_path != path:
            atomic_parquet(enriched, legacy_path)


def _overall_status(jobs: dict[str, dict[str, Any]]) -> str:
    statuses = {job.get("status") for job in jobs.values()}
    if statuses.issubset({"available", "stale"}):
        return "stale" if "stale" in statuses else "available"
    if statuses.intersection({"available", "stale"}):
        return "partial"
    return "unavailable"


def run_weekend_weather_pipeline(
        *,
        meeting_key: int,
        decision_time: str,
        season: int = DEFAULT_SEASON,
        purpose: str = "weekend",
        target_session_key: int | None = None,
        forecast_session_key: int | None = None,
        session_key: int | None = None,
        run_initialized_at: str | None = None,
        available_at: str | None = None,
        session_keys: Sequence[int] | None = None,
        session_types: Sequence[str] | None = None,
        refresh: bool = False,
        output_dir: Path = CURATED_DATA_DIR,
        master_loader: MasterLoader | None = None,
        reference_loader: ReferenceLoader | None = None,
        forecast_loader: ForecastLoader | None = None,
        weekend_loader: WeekendLoader | None = None,
        geometry_builder: GeometryBuilder = build_manifest_geometry,
) -> tuple[dict[str, Any], Path]:
    started_at = datetime.now(timezone.utc)
    cut_time = utc_timestamp(decision_time, "decision_time")
    normalized_decision_time = cut_time.isoformat()
    normalized_purpose = purpose.strip().casefold()
    jobs: dict[str, dict[str, Any]] = {}
    context: dict[str, Any] | None = None
    reference: CircuitReference | None = None
    outputs: dict[str, Path] = {}
    weekend_result: dict[str, Any] | None = None
    shared_openf1 = (
        OpenF1Client()
        if master_loader is None or weekend_loader is None
        else None
    )
    load_master = master_loader or (
        lambda selected_season, force_refresh: _load_master_outputs(
            selected_season,
            force_refresh,
            shared_openf1,
        )
    )
    load_reference = reference_loader or resolve_circuit_reference
    load_weather = forecast_loader or load_forecast
    load_weekend = weekend_loader or (
        lambda sessions, **kwargs: ingest_weekend(
            sessions,
            client=shared_openf1,
            **kwargs,
        )
    )
    normalized_session_keys = _normalized_session_keys(session_keys)
    normalized_session_types = _normalized_session_types(session_types)
    resolved_session_keys: list[int] = []
    requested_targets = {
        value
        for value in (target_session_key, forecast_session_key, session_key)
        if value is not None
    }
    if len(requested_targets) > 1:
        raise WeekendWeatherPipelineError(
            "target_session_key and legacy session aliases must not conflict."
        )
    requested_target_session_key = next(iter(requested_targets), None)
    if (run_initialized_at is None) != (available_at is None):
        raise WeekendWeatherPipelineError(
            "run_initialized_at and available_at must be provided together."
        )
    if run_initialized_at is None:
        selected_run, selected_availability = select_historical_single_run(cut_time)
        resolved_run_initialized_at = selected_run.isoformat()
        resolved_available_at = selected_availability.isoformat()
    else:
        assert run_initialized_at is not None
        assert available_at is not None
        resolved_run_initialized_at = utc_timestamp(
            cast(str, run_initialized_at), "run_initialized_at"
        ).isoformat()
        resolved_available_at = utc_timestamp(
            cast(str, available_at), "available_at"
        ).isoformat()

    try:
        outputs = load_master(season, refresh)
        selected_context = _selected_meeting_context(outputs, meeting_key)
    except (
            MasterDataError,
            OpenF1Error,
            WeekendWeatherPipelineError,
            OSError,
            ValueError,
            KeyError,
    ) as exc:
        jobs["openf1"] = {"status": "unavailable", "error": str(exc)}
    else:
        try:
            purpose_plan = plan_sessions_for_purpose(
                selected_context["sessions"],
                purpose=normalized_purpose,
                decision_time=cut_time,
                target_session_key=requested_target_session_key,
            )
        except SessionPlanningError as exc:
            raise WeekendWeatherPipelineError(str(exc)) from exc
        if purpose_plan["selected_sessions"]:
            selected_sessions = select_weekend_sessions(
                purpose_plan["selected_sessions"],
                session_keys=normalized_session_keys,
                session_types=normalized_session_types,
            )
        elif normalized_session_keys is not None or normalized_session_types is not None:
            raise WeekendWeatherPipelineError(
                "Session filters did not match data available at decision_time."
            )
        else:
            selected_sessions = []
        resolved_session_keys = [
            session["source_session_key"] for session in selected_sessions
        ]
        context = {
            **selected_context,
            "discovered_session_count": selected_context["session_count"],
            "selected_session_count": len(selected_sessions),
            "sessions": selected_sessions,
            "purpose": purpose_plan["purpose"],
            "decision_time": purpose_plan["decision_time"],
            "selection_basis": purpose_plan["selection_basis"],
            "target_session": purpose_plan["target_session"],
        }
        jobs["openf1"] = {
            "status": "available" if refresh else "stale",
            "manifest_path": _relative(outputs["manifest"]),
            "manifest_sha256": sha256(outputs["manifest"]),
            "session_count": context["discovered_session_count"],
        }

    if context is None or not context["sessions"]:
        jobs["openf1_weekend_facts"] = {
            "status": "unavailable",
            "error": (
                "OpenF1 session context is unavailable."
                if context is None
                else "No completed sessions are available at decision_time."
            ),
        }
    else:
        try:
            loaded_weekend, weekend_path = load_weekend(
                context["sessions"],
                meeting_key=meeting_key,
                purpose=context["purpose"],
                refresh=refresh,
                curated_dir=output_dir,
            )
            weekend_result = loaded_weekend
            jobs["openf1_weekend_facts"] = {
                "status": loaded_weekend["status"],
                "manifest_path": _relative(weekend_path),
                "manifest_sha256": sha256(weekend_path),
                "sessions": loaded_weekend["sessions"],
            }
        except (OpenF1WeekendError, OSError, ValueError, KeyError) as exc:
            jobs["openf1_weekend_facts"] = {
                "status": "unavailable",
                "error": str(exc),
            }

    geometry_results: list[dict[str, Any]] = []
    geometry_errors: list[dict[str, Any]] = []
    if context is not None and weekend_result is not None:
        for session_manifest in weekend_result.get("sessions", []):
            if not isinstance(session_manifest, dict):
                continue
            endpoints = session_manifest.get("endpoints")
            if not isinstance(endpoints, dict) or not all(
                    name in endpoints for name in ("sessions", "laps", "location")
            ):
                continue
            try:
                geometry_result, _ = geometry_builder(
                    session_manifest,
                    season=season,
                    meeting_key=meeting_key,
                    circuit_id=context["circuit_id"],
                    curated_dir=output_dir,
                )
                geometry_results.append(geometry_result)
            except (TrackGeometryError, OSError, ValueError, KeyError, TypeError) as exc:
                geometry_errors.append(
                    {
                        "session_key": session_manifest.get("session_key"),
                        "error": str(exc),
                    }
                )
    if geometry_results:
        statuses = {result["status"] for result in geometry_results}
        jobs["track_geometry"] = {
            "status": (
                "partial"
                if geometry_errors or "partial" in statuses
                else "available"
            ),
            "geometries": geometry_results,
            "errors": geometry_errors,
            "manifest_sha256": geometry_results[-1].get("manifest_sha256"),
            "curated_sha256": geometry_results[-1].get("curated_sha256"),
        }
    else:
        jobs["track_geometry"] = {
            "status": "unavailable",
            "error": "No session produced a usable OpenF1 track geometry.",
            "errors": geometry_errors,
        }
    if "openf1" in jobs and outputs.get("manifest", Path()).is_file():
        jobs["openf1"]["manifest_sha256"] = sha256(outputs["manifest"])

    if context is None:
        jobs["wikidata"] = {
            "status": "unavailable",
            "error": "OpenF1 circuit context is unavailable.",
        }
    else:
        try:
            if reference_loader is None:
                loaded_reference, result = load_reference(
                    context["source_circuit_key"],
                    context["circuit_name"],
                    context["circuit_country"] or "",
                    location=context["circuit_location"],
                    refresh=refresh,
                )
            else:
                loaded_reference, result = load_reference(
                    context["source_circuit_key"], refresh=refresh
                )
            reference = loaded_reference
            dimensions_dir = (CURATED_DATA_DIR / "dimensions").resolve()
            legacy_circuit = None
            if season == 2026 and outputs["circuit"].resolve().is_relative_to(
                    dimensions_dir
            ):
                legacy_circuit = dimensions_dir / "circuit.parquet"
            _enrich_circuit(outputs["circuit"], loaded_reference, legacy_circuit)
            jobs["wikidata"] = {
                **result,
                "reference": loaded_reference.to_dict(),
                "curated_path": _relative(outputs["circuit"]),
                "curated_sha256": sha256(outputs["circuit"]),
            }
        except CircuitResolutionError as exc:
            jobs["wikidata"] = exc.result
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
                session_id=context["target_session"]["session_id"],
                circuit_id=context["circuit_id"],
                run_initialized_at=resolved_run_initialized_at,
                available_at=resolved_available_at,
                decision_time=decision_time,
                refresh=refresh,
            )
            target_start = context["target_session"].get("scheduled_start_utc")
            if target_start is None:
                raise OpenMeteoError("The target session has no scheduled start time.")
            validate_forecast_horizon(forecast, target_start)
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

    resolved_target_session_key = (
        context.get("target_session", {}).get("source_session_key")
        if context
        else None
    )
    selection = {
        "season": season,
        "meeting_key": meeting_key,
        "purpose": normalized_purpose,
        "target_session_key": resolved_target_session_key,
        "run_initialized_at": resolved_run_initialized_at,
        "available_at": resolved_available_at,
        "decision_time": normalized_decision_time,
        "session_keys": normalized_session_keys,
        "session_types": normalized_session_types,
        "resolved_session_keys": resolved_session_keys,
    }
    identity = json.dumps(
        {
            "schema_version": MANIFEST_SCHEMA_VERSION,
            **selection,
            "circuit_mapping": {
                "schema_version": jobs.get("wikidata", {})
                .get("mapping", {})
                .get("schema_version"),
                "sha256": jobs.get("wikidata", {}).get("mapping", {}).get("sha256"),
                "auto_schema_version": jobs.get("wikidata", {})
                .get("auto_mapping", {})
                .get("schema_version"),
                "auto_sha256": jobs.get("wikidata", {})
                .get("auto_mapping", {})
                .get("sha256"),
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
        "selection": selection,
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
    parser.add_argument("--meeting-key", type=int, required=True)
    parser.add_argument("--decision-time", required=True)
    parser.add_argument("--purpose", choices=PURPOSES, default="weekend")
    target_group = parser.add_mutually_exclusive_group()
    target_group.add_argument("--target-session-key", type=int)
    target_group.add_argument("--forecast-session-key", type=int, dest="target_session_key")
    target_group.add_argument("--session-key", type=int, dest="target_session_key")
    parser.add_argument("--run-initialized-at")
    parser.add_argument("--available-at")
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
        purpose=args.purpose,
        target_session_key=args.target_session_key,
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
