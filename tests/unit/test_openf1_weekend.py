from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from f1_pipeline.persistence import sha256
from f1_pipeline.sources.openf1_weekend import (
    OpenF1WeekendError,
    ingest_session,
    plan_weekend_sessions,
)


class OpenF1WeekendTest(unittest.TestCase):
    def test_plans_regular_and_sprint_sessions_without_location(self) -> None:
        plans = plan_weekend_sessions(
            [
                {
                    "session_id": "openf1:session:1",
                    "source_session_key": 1,
                    "session_type": "Practice",
                    "session_name": "Practice 1",
                    "status": "completed",
                },
                {
                    "session_id": "openf1:session:2",
                    "source_session_key": 2,
                    "session_type": "Qualifying",
                    "session_name": "Sprint Qualifying",
                    "status": "completed",
                },
                {
                    "session_id": "openf1:session:3",
                    "source_session_key": 3,
                    "session_type": "Race",
                    "session_name": "Sprint",
                    "status": "completed",
                },
                {
                    "session_id": "openf1:session:4",
                    "source_session_key": 4,
                    "session_type": "Race",
                    "session_name": "Race",
                    "status": "completed",
                },
            ]
        )

        self.assertEqual(
            [plan["normalized_session_type"] for plan in plans],
            ["practice", "sprint_qualifying", "sprint", "race"],
        )
        self.assertNotIn("location", plans[0]["required_endpoints"])
        self.assertIn("location", plans[0]["skipped_endpoints"])
        self.assertIn("intervals", plans[0]["skipped_endpoints"])
        self.assertIn("session_result", plans[1]["optional_endpoints"])
        self.assertIn("pit", plans[-1]["required_endpoints"])
        self.assertIn("starting_grid", plans[-1]["optional_endpoints"])
        self.assertIn("session_result", plans[-1]["optional_endpoints"])
        self.assertIn("championship_drivers", plans[-1]["optional_endpoints"])
        self.assertIn("championship_teams", plans[-1]["optional_endpoints"])

        replay = plan_weekend_sessions(
            [
                {
                    "session_id": "openf1:session:4",
                    "source_session_key": 4,
                    "session_type": "Race",
                    "session_name": "Race",
                    "status": "completed",
                }
            ],
            purpose="replay",
        )[0]
        self.assertIn("sessions", replay["required_endpoints"])
        self.assertIn("location", replay["required_endpoints"])
        self.assertNotIn("location", replay["skipped_endpoints"])

        complete = plan_weekend_sessions(
            [
                {
                    "session_id": "openf1:session:1",
                    "source_session_key": 1,
                    "session_type": "Practice",
                    "session_name": "Practice 1",
                    "status": "completed",
                },
                {
                    "session_id": "openf1:session:4",
                    "source_session_key": 4,
                    "session_type": "Race",
                    "session_name": "Race",
                    "status": "completed",
                },
            ],
            purpose="weekend_complete_v1",
        )
        self.assertIn("sessions", complete[0]["required_endpoints"])
        self.assertNotIn("location", complete[0]["required_endpoints"])
        self.assertIn("sessions", complete[1]["required_endpoints"])
        self.assertIn("location", complete[1]["required_endpoints"])

    def test_ingestion_persists_results_and_isolates_optional_failure(self) -> None:
        payloads = {
            "sessions": [
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "session_name": "Race",
                    "date_start": "2026-07-26T13:00:00Z",
                }
            ],
            "session_result": [
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "driver_number": 1,
                    "position": 1,
                    "number_of_laps": 70,
                    "points": 25.0,
                    "gap_to_leader": 0,
                },
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "driver_number": 2,
                    "position": None,
                    "number_of_laps": 69,
                    "points": 18.0,
                    "duration": [80.1, 79.8],
                    "gap_to_leader": "+1 LAP",
                },
            ],
            "championship_drivers": [
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "driver_number": 1,
                    "position_current": 1,
                    "points_current": 115.0,
                }
            ],
            "championship_teams": [
                {
                    "session_key": 42,
                    "meeting_key": 7,
                    "team_name": "Example Racing",
                    "position_current": 1,
                    "points_current": 165.0,
                }
            ],
        }

        class FakeClient:
            def __init__(self, failed_endpoint: str | None = None) -> None:
                self.failed_endpoint = failed_endpoint

            def get_json(self, endpoint: str, params: dict[str, object]) -> list[dict[str, object]]:
                self.assert_session_key(params)
                if endpoint == self.failed_endpoint:
                    raise OpenF1WeekendError(f"{endpoint} unavailable")
                return payloads[endpoint]

            @staticmethod
            def assert_session_key(params: dict[str, object]) -> None:
                if params != {"session_key": 42}:
                    raise AssertionError(f"Unexpected params: {params}")

        class NoNetworkClient:
            def get_json(
                self, endpoint: str, params: dict[str, object]
            ) -> list[dict[str, object]]:
                raise AssertionError(f"Unexpected request: {endpoint} {params}")

        plan = {
            "session_id": "openf1:session:42",
            "source_session_key": 42,
            "normalized_session_type": "race",
            "required_endpoints": ["sessions"],
            "optional_endpoints": [
                "session_result",
                "championship_drivers",
                "championship_teams",
            ],
            "skipped_endpoints": [],
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = ingest_session(
                plan,
                client=FakeClient(),
                refresh=True,
                raw_dir=root / "raw",
                curated_dir=root / "curated",
            )

            self.assertEqual(result["status"], "available")
            for source_name in (
                    "session_result",
                    "championship_drivers",
                    "championship_teams",
            ):
                endpoint_result = result["endpoints"][source_name]
                raw_path = Path(endpoint_result["raw_path"])
                silver_path = Path(endpoint_result["silver_path"])
                self.assertEqual(endpoint_result["raw_sha256"], sha256(raw_path))
                self.assertEqual(endpoint_result["silver_sha256"], sha256(silver_path))

            result_facts = pd.read_parquet(
                result["endpoints"]["session_result"]["silver_path"]
            )
            self.assertEqual(result_facts.iloc[1]["gap_to_leader_raw"], "+1 LAP")
            self.assertEqual(result_facts.iloc[1]["laps_behind"], 1)
            self.assertEqual(result_facts.iloc[1]["duration_raw"], "[80.1,79.8]")
            self.assertTrue(pd.isna(result_facts.iloc[1]["position"]))
            manifest_path = Path(result["manifest_path"])
            self.assertEqual(json.loads(manifest_path.read_text())["status"], "available")
            self.assertEqual(result["manifest_sha256"], sha256(manifest_path))

            cached = ingest_session(
                plan,
                client=NoNetworkClient(),
                refresh=False,
                raw_dir=root / "raw",
                curated_dir=root / "curated",
            )

            self.assertEqual(cached["status"], "stale")
            self.assertIn("silver_path", cached["endpoints"]["session_result"])

            partial = ingest_session(
                plan,
                client=FakeClient("championship_teams"),
                refresh=True,
                raw_dir=root / "raw",
                curated_dir=root / "curated",
            )

            self.assertEqual(partial["status"], "partial")
            self.assertEqual(
                partial["endpoints"]["championship_teams"]["status"], "unavailable"
            )
            self.assertIn("silver_path", partial["endpoints"]["session_result"])
            self.assertIn("silver_path", partial["endpoints"]["championship_drivers"])

    def test_location_is_loaded_per_driver(self) -> None:
        calls: list[tuple[str, dict[str, object]]] = []

        class FakeClient:
            def get_json(
                    self, endpoint: str, params: dict[str, object]
            ) -> list[dict[str, object]]:
                calls.append((endpoint, params))
                if endpoint == "drivers":
                    return [
                        {"session_key": 42, "driver_number": 1, "name_acronym": "ONE"},
                        {"session_key": 42, "driver_number": 2, "name_acronym": "TWO"},
                    ]
                driver_number = int(params["driver_number"])
                return [
                    {
                        "session_key": 42,
                        "driver_number": driver_number,
                        "date": "2026-07-26T13:00:00Z",
                        "x": driver_number,
                        "y": driver_number + 1,
                        "z": 0,
                    }
                ]

        plan = {
            "session_id": "openf1:session:42",
            "source_session_key": 42,
            "normalized_session_type": "race",
            "required_endpoints": ["drivers", "location"],
            "optional_endpoints": [],
            "skipped_endpoints": [],
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = ingest_session(
                plan,
                client=FakeClient(),
                refresh=True,
                raw_dir=root / "raw",
                curated_dir=root / "curated",
            )

            self.assertEqual(result["status"], "available")
            self.assertEqual(result["endpoints"]["location"]["row_count"], 2)
            self.assertEqual(
                calls,
                [
                    ("drivers", {"session_key": 42}),
                    ("location", {"session_key": 42, "driver_number": 1}),
                    ("location", {"session_key": 42, "driver_number": 2}),
                ],
            )


if __name__ == "__main__":
    unittest.main()
