from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from f1_pipeline.persistence import atomic_json, atomic_parquet, sha256
from f1_pipeline.planning import plan_sessions_for_purpose
from f1_pipeline.sources.open_meteo import (
    HOURLY_VARIABLES,
    OpenMeteoError,
    normalize_forecast,
    select_historical_single_run,
    validate_forecast_horizon,
)
from f1_pipeline.sources.weekend_weather_pipeline import (
    WeekendWeatherPipelineError,
    run_weekend_weather_pipeline,
    select_weekend_sessions,
)
from f1_pipeline.sources.wikidata import (
    CircuitReference,
    WikidataError,
    load_reviewed_circuits,
    normalize_circuit_reference,
)


class WeekendWeatherPipelineTest(unittest.TestCase):
    def test_wikidata_accepts_only_the_reviewed_hungaroring_reference(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            raw_path = Path(temporary) / "Q171356.json"
            mapping_path = Path(temporary) / "reviewed_circuits.json"
            mapping_payload = {
                "schema_version": 1,
                "mappings": [
                    {
                        "source_system": "openf1",
                        "source_circuit_key": 4,
                        "wikidata_entity_id": "Q171356",
                        "expected_label": "Hungaroring",
                        "expected_country": "Hungary",
                        "review_status": "reviewed",
                    }
                ],
            }
            atomic_json(mapping_payload, mapping_path)
            payload = {
                "entities": {
                    "Q171356": {
                        "id": "Q171356",
                        "lastrevid": 2519292350,
                        "labels": {"en": {"value": "Hungaroring"}},
                        "descriptions": {
                            "en": {"value": "race track in Mogyoród, Hungary"}
                        },
                        "claims": {
                            "P625": [
                                {
                                    "rank": "normal",
                                    "mainsnak": {
                                        "snaktype": "value",
                                        "datavalue": {
                                            "value": {
                                                "latitude": 47.582222222222,
                                                "longitude": 19.251111111111,
                                                "globe": "http://www.wikidata.org/entity/Q2",
                                            }
                                        },
                                    },
                                }
                            ]
                        },
                    }
                }
            }
            atomic_json(payload, raw_path)
            mappings, mapping = load_reviewed_circuits(mapping_path)

            reference = normalize_circuit_reference(
                4,
                payload,
                retrieved_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
                raw_path=raw_path,
                reviewed_circuits=mappings,
            )

            self.assertEqual(reference.wikidata_entity_id, "Q171356")
            self.assertEqual(reference.crs, "EPSG:4326")
            self.assertAlmostEqual(reference.latitude, 47.582222222222)
            self.assertEqual(mapping["record_count"], 1)
            self.assertEqual(mapping["schema_version"], 1)
            with self.assertRaises(WikidataError):
                normalize_circuit_reference(
                    999,
                    payload,
                    retrieved_at=datetime(2026, 8, 25, tzinfo=timezone.utc),
                    raw_path=raw_path,
                    reviewed_circuits=mappings,
                )

            mapping_payload["mappings"][0]["review_status"] = "candidate"
            atomic_json(mapping_payload, mapping_path)
            with self.assertRaises(WikidataError):
                load_reviewed_circuits(mapping_path)

    def test_session_selection_filters_keys_and_types_deterministically(self) -> None:
        sessions = [
            {
                "session_id": "openf1:session:12",
                "source_session_key": 12,
                "session_type": "Race",
                "session_name": "Race",
                "scheduled_start_utc": "2026-07-26T13:00:00+00:00",
                "status": "completed",
            },
            {
                "session_id": "openf1:session:10",
                "source_session_key": 10,
                "session_type": "Practice",
                "session_name": "Practice 1",
                "scheduled_start_utc": "2026-07-24T11:30:00+00:00",
                "status": "completed",
            },
            {
                "session_id": "openf1:session:11",
                "source_session_key": 11,
                "session_type": "Sprint Qualifying",
                "session_name": "Sprint Qualifying",
                "scheduled_start_utc": "2026-07-24T15:30:00+00:00",
                "status": "completed",
            },
        ]

        selected = select_weekend_sessions(
            sessions,
            session_keys=[12, 11, 10, 10],
            session_types=["sprint qualifying", "practice"],
        )

        self.assertEqual(
            [session["source_session_key"] for session in selected],
            [10, 11],
        )
        with self.assertRaises(WeekendWeatherPipelineError):
            select_weekend_sessions(sessions, session_keys=[999])
        with self.assertRaises(WeekendWeatherPipelineError):
            select_weekend_sessions(sessions, session_types=["testing"])

    def test_forecast_preserves_time_boundaries_and_missing_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            raw_path = Path(temporary) / "forecast.json"
            hourly: dict[str, list[Any]] = {
                "time": ["2026-07-26T12:00", "2026-07-26T13:00"]
            }
            units = {}
            for variable in HOURLY_VARIABLES:
                hourly[variable] = [None, 1]
                units[variable] = "unit"
            payload = {
                "latitude": 47.557117,
                "longitude": 19.243422,
                "elevation": 220.0,
                "hourly": hourly,
                "hourly_units": units,
            }
            atomic_json(payload, raw_path)
            reference = CircuitReference(
                4,
                "Q171356",
                "Hungaroring",
                47.582222222222,
                19.251111111111,
                "EPSG:4326",
                2519292350,
                "2026-08-25T00:00:00+00:00",
                "verified",
                str(raw_path),
                "raw-hash",
            )
            run_time = pd.Timestamp("2026-07-26T00:00:00Z")
            available_at = pd.Timestamp("2026-07-26T06:00:00Z")
            decision_time = pd.Timestamp("2026-07-26T12:00:00Z")

            frame = normalize_forecast(
                payload,
                snapshot_id="open_meteo:test",
                session_id="openf1:session:11342",
                circuit_id="openf1:circuit:4",
                reference=reference,
                run_initialized_at=run_time,
                available_at=available_at,
                retrieved_at=pd.Timestamp("2026-08-25T00:00:00Z"),
                decision_time=decision_time,
                raw_path=raw_path,
            )

            self.assertTrue(pd.isna(frame.iloc[0]["precipitation"]))
            self.assertEqual(frame.iloc[0]["available_at"], available_at)
            self.assertEqual(
                frame.iloc[0]["availability_basis"],
                "conservative_documented_latency",
            )
            with self.assertRaises(OpenMeteoError):
                normalize_forecast(
                    payload,
                    snapshot_id="open_meteo:test-late",
                    session_id="openf1:session:11342",
                    circuit_id="openf1:circuit:4",
                    reference=reference,
                    run_initialized_at=run_time,
                    available_at=pd.Timestamp("2026-07-26T13:00:00Z"),
                    retrieved_at=pd.Timestamp("2026-08-25T00:00:00Z"),
                    decision_time=decision_time,
                    raw_path=raw_path,
                )

    def test_purpose_planning_selects_historical_inputs_for_consumer_side_cut(self) -> None:
        sessions = [
            {
                "session_id": "openf1:session:10",
                "source_session_key": 10,
                "session_type": "Practice",
                "session_name": "Practice 1",
                "scheduled_start_utc": "2026-07-24T10:00:00Z",
                "scheduled_end_utc": "2026-07-24T11:00:00Z",
                "status": "completed",
            },
            {
                "session_id": "openf1:session:11",
                "source_session_key": 11,
                "session_type": "Qualifying",
                "session_name": "Qualifying",
                "scheduled_start_utc": "2026-07-25T14:00:00Z",
                "scheduled_end_utc": "2026-07-25T15:00:00Z",
                "status": "completed",
            },
            {
                "session_id": "openf1:session:12",
                "source_session_key": 12,
                "session_type": "Race",
                "session_name": "Race",
                "scheduled_start_utc": "2026-07-26T13:00:00Z",
                "scheduled_end_utc": "2026-07-26T15:00:00Z",
                "status": "completed",
            },
        ]

        qualifying = plan_sessions_for_purpose(
            sessions,
            purpose="qualifying_prediction",
            decision_time="2026-07-25T13:00:00Z",
        )
        strategy = plan_sessions_for_purpose(
            sessions,
            purpose="race_strategy",
            decision_time="2026-07-26T13:30:00Z",
        )
        replay = plan_sessions_for_purpose(
            sessions,
            purpose="replay",
            decision_time="2026-07-26T16:00:00Z",
            target_session_key=12,
        )
        practice_replay = plan_sessions_for_purpose(
            sessions,
            purpose="replay",
            decision_time="2026-07-24T12:00:00Z",
            target_session_key=10,
        )

        self.assertEqual(
            [row["source_session_key"] for row in qualifying["selected_sessions"]],
            [10],
        )
        self.assertEqual(
            [row["source_session_key"] for row in strategy["selected_sessions"]],
            [10, 11, 12],
        )
        self.assertEqual(
            [row["source_session_key"] for row in replay["selected_sessions"]],
            [12],
        )
        self.assertEqual(
            [row["source_session_key"] for row in practice_replay["selected_sessions"]],
            [10],
        )

    def test_qualifying_plan_includes_completed_sprint_weekend_sessions(self) -> None:
        sessions = [
            {
                "session_id": f"openf1:session:{key}",
                "source_session_key": key,
                "session_type": session_type,
                "session_name": name,
                "scheduled_start_utc": start,
                "scheduled_end_utc": end,
                "status": "completed",
            }
            for key, session_type, name, start, end in (
                (1, "Practice", "Practice 1", "2026-05-01T10:00:00Z", "2026-05-01T11:00:00Z"),
                (2, "Qualifying", "Sprint Qualifying", "2026-05-01T14:00:00Z", "2026-05-01T15:00:00Z"),
                (3, "Race", "Sprint", "2026-05-02T10:00:00Z", "2026-05-02T11:00:00Z"),
                (4, "Qualifying", "Qualifying", "2026-05-02T14:00:00Z", "2026-05-02T15:00:00Z"),
                (5, "Race", "Race", "2026-05-03T13:00:00Z", "2026-05-03T15:00:00Z"),
            )
        ]

        plan = plan_sessions_for_purpose(
            sessions,
            purpose="qualifying_prediction",
            decision_time="2026-05-02T13:00:00Z",
        )

        self.assertEqual(plan["target_session"]["source_session_key"], 4)
        self.assertEqual(
            [row["source_session_key"] for row in plan["selected_sessions"]],
            [1, 2, 3],
        )

    def test_historical_run_selection_respects_publication_boundary(self) -> None:
        exact_run, exact_available = select_historical_single_run(
            "2026-07-26T12:00:00Z"
        )
        earlier_run, earlier_available = select_historical_single_run(
            "2026-07-26T11:59:59Z"
        )

        self.assertEqual(exact_run, pd.Timestamp("2026-07-26T06:00:00Z"))
        self.assertEqual(exact_available, pd.Timestamp("2026-07-26T12:00:00Z"))
        self.assertEqual(earlier_run, pd.Timestamp("2026-07-26T00:00:00Z"))
        self.assertLessEqual(earlier_available, pd.Timestamp("2026-07-26T11:59:59Z"))

    def test_forecast_horizon_must_cover_target_session(self) -> None:
        forecast = pd.DataFrame(
            {
                "valid_time": pd.to_datetime(
                    ["2026-07-26T12:00:00Z", "2026-07-26T13:00:00Z"],
                    utc=True,
                )
            }
        )

        validate_forecast_horizon(forecast, "2026-07-26T13:00:00Z")
        with self.assertRaises(OpenMeteoError):
            validate_forecast_horizon(forecast, "2026-07-26T14:00:00Z")

    def test_weather_failure_preserves_f1_output_and_idempotent_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            dimensions = root / "dimensions"
            manifest_path = root / "master_data.json"
            meeting_path = dimensions / "meeting.parquet"
            session_path = dimensions / "session.parquet"
            circuit_path = dimensions / "circuit.parquet"
            atomic_parquet(
                pd.DataFrame(
                    [{"meeting_id": "openf1:meeting:1291", "circuit_id": "openf1:circuit:4"}]
                ),
                meeting_path,
            )
            atomic_parquet(
                pd.DataFrame(
                    [
                        {
                            "session_id": "openf1:session:11338",
                            "meeting_id": "openf1:meeting:1291",
                            "session_type": "Qualifying",
                            "session_name": "Qualifying",
                            "scheduled_start_utc": pd.Timestamp("2026-07-25T14:00:00Z"),
                            "scheduled_end_utc": pd.Timestamp("2026-07-25T15:00:00Z"),
                            "status": "completed",
                        },
                        {
                            "session_id": "openf1:session:11342",
                            "meeting_id": "openf1:meeting:1291",
                            "session_type": "Race",
                            "session_name": "Race",
                            "scheduled_start_utc": pd.Timestamp("2026-07-26T13:00:00Z"),
                            "scheduled_end_utc": pd.Timestamp("2026-07-26T15:00:00Z"),
                            "status": "completed",
                        },
                    ]
                ),
                session_path,
            )
            atomic_parquet(
                pd.DataFrame(
                    [
                        {
                            "circuit_id": "openf1:circuit:4",
                            "source_circuit_key": 4,
                        }
                    ]
                ),
                circuit_path,
            )
            atomic_json(
                {
                    "status": "valid",
                    "tables": {
                        "circuit": {
                            "path": str(circuit_path),
                            "row_count": 1,
                            "sha256": "before-enrichment",
                        }
                    },
                },
                manifest_path,
            )
            outputs = {
                "meeting": meeting_path,
                "session": session_path,
                "circuit": circuit_path,
                "manifest": manifest_path,
            }
            reference = CircuitReference(
                4,
                "Q171356",
                "Hungaroring",
                47.582222222222,
                19.251111111111,
                "EPSG:4326",
                2519292350,
                "2026-08-25T00:00:00+00:00",
                "verified",
                "data/raw/Q171356.json",
                "raw-hash",
            )

            def master_loader(season: int, refresh: bool) -> dict[str, Path]:
                return outputs

            def reference_loader(*args: object, **kwargs: object) -> tuple[CircuitReference, dict[str, object]]:
                return reference, {"status": "available", "fetched": False}

            def forecast_loader(*args: object, **kwargs: object) -> tuple[pd.DataFrame, dict[str, object]]:
                raise OpenMeteoError("forecast unavailable")

            weekend_calls: list[dict[str, object]] = []

            def weekend_loader(
                    sessions: list[dict[str, Any]], **kwargs: object
            ) -> tuple[dict[str, object], Path]:
                weekend_calls.append(
                    {
                        "purpose": kwargs["purpose"],
                        "session_keys": [
                            session["source_session_key"] for session in sessions
                        ],
                    }
                )
                path = root / "curated" / "manifests" / "openf1_weekend_test.json"
                result: dict[str, object] = {
                    "status": "stale",
                    "sessions": [
                        {"session_id": session["session_id"], "status": "stale"}
                        for session in sessions
                    ],
                }
                atomic_json(result, path)
                return result, path

            first, first_path = run_weekend_weather_pipeline(
                meeting_key=1291,
                decision_time="2026-07-26T16:00:00Z",
                purpose="weekend_complete_v1",
                output_dir=root / "curated",
                master_loader=master_loader,
                reference_loader=reference_loader,
                forecast_loader=forecast_loader,
                weekend_loader=weekend_loader,
                session_keys=[11342, 11338, 11342],
                session_types=["race", "qualifying"],
            )
            second, second_path = run_weekend_weather_pipeline(
                meeting_key=1291,
                decision_time="2026-07-26T16:00:00Z",
                purpose="weekend_complete_v1",
                output_dir=root / "curated",
                master_loader=master_loader,
                reference_loader=reference_loader,
                forecast_loader=forecast_loader,
                weekend_loader=weekend_loader,
                session_keys=[11338, 11342],
                session_types=["qualifying", "race"],
            )
            race_only, race_only_path = run_weekend_weather_pipeline(
                meeting_key=1291,
                decision_time="2026-07-26T16:00:00Z",
                purpose="weekend_complete_v1",
                output_dir=root / "curated",
                master_loader=master_loader,
                reference_loader=reference_loader,
                forecast_loader=forecast_loader,
                weekend_loader=weekend_loader,
                session_keys=[11342],
            )

            self.assertEqual(first["status"], "partial")
            self.assertEqual(first["schema_version"], 6)
            self.assertEqual(first["jobs"]["openf1"]["status"], "stale")
            self.assertEqual(first["jobs"]["openf1_weekend_facts"]["status"], "stale")
            self.assertEqual(first["jobs"]["track_geometry"]["status"], "unavailable")
            self.assertEqual(first["jobs"]["open_meteo"]["status"], "unavailable")
            self.assertEqual(first_path, second_path)
            self.assertEqual(first["run_id"], second["run_id"])
            self.assertEqual(first["selection"]["resolved_session_keys"], [11338, 11342])
            self.assertEqual(first["context"]["selected_session_count"], 2)
            self.assertEqual(first["selection"]["purpose"], "weekend_complete_v1")
            self.assertEqual(
                weekend_calls,
                [
                    {
                        "purpose": "weekend_complete_v1",
                        "session_keys": [11338, 11342],
                    },
                    {
                        "purpose": "weekend_complete_v1",
                        "session_keys": [11338, 11342],
                    },
                    {
                        "purpose": "weekend_complete_v1",
                        "session_keys": [11342],
                    },
                ],
            )
            self.assertEqual(race_only["selection"]["resolved_session_keys"], [11342])
            self.assertNotEqual(first_path, race_only_path)
            persisted = json.loads(first_path.read_text(encoding="utf-8"))
            self.assertEqual(persisted["run_id"], second["run_id"])
            master_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(
                master_manifest["tables"]["circuit"]["sha256"],
                sha256(circuit_path),
            )


if __name__ == "__main__":
    unittest.main()
