from __future__ import annotations

import json
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from f1_pipeline.dashboard.admin_panel import (
    ActivityEntry,
    _geometry_entries,
    _job_entries,
    _master_data_entries,
    _recent_event_count,
    _running_job_entries,
    _session_entries,
    _weekend_pipeline_entries,
    collect_pipeline_activity,
    is_stalled,
    meeting_names,
    season_coverage,
    status_tone,
)


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


class JobEntriesTest(unittest.TestCase):
    def test_reads_status_and_intent_from_a_job_file(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            _write(
                job_dir / "job_abc123.json",
                {
                    "status": "running",
                    "intent": {"purpose": "weekend_complete_v1", "meeting_key": 1291},
                },
            )
            entries = _job_entries(job_dir)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].kind, "Dashboard job")
            self.assertEqual(entries[0].status, "running")
            self.assertIn("1291", entries[0].label)

    def test_ignores_unreadable_or_malformed_files(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            (job_dir).mkdir(exist_ok=True)
            (job_dir / "job_broken.json").write_text("not json", encoding="utf-8")
            self.assertEqual(_job_entries(job_dir), [])

    def test_missing_directory_returns_no_entries(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            missing = Path(temporary) / "does_not_exist"
            self.assertEqual(_job_entries(missing), [])

    def test_a_jobs_progress_file_is_not_read_as_a_second_job(self) -> None:
        # job_<id>.progress.json also matches the "job_*.json" glob used to
        # find job status files; without an explicit exclusion it shows up
        # as a bogus "unknown · meeting ?" row alongside the real job.
        with tempfile.TemporaryDirectory() as temporary:
            job_dir = Path(temporary)
            _write(
                job_dir / "job_abc123.json",
                {
                    "status": "running",
                    "intent": {"purpose": "weekend_complete_v1", "meeting_key": 1291},
                },
            )
            _write(
                job_dir / "job_abc123.progress.json",
                {"schema_version": 1, "job_id": "abc123", "events": []},
            )
            entries = _job_entries(job_dir)
            self.assertEqual(len(entries), 1)
            self.assertIn("1291", entries[0].label)


class WeekendPipelineEntriesTest(unittest.TestCase):
    def test_uses_completed_at_and_selection_for_the_label(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest_dir = Path(temporary)
            _write(
                manifest_dir / "weekend_weather_pipeline_1291_abc.json",
                {
                    "status": "partial",
                    "started_at": "2026-07-26T15:00:00Z",
                    "completed_at": "2026-07-26T15:05:00Z",
                    "selection": {"meeting_key": 1291, "purpose": "weekend"},
                },
            )
            entries = _weekend_pipeline_entries(manifest_dir)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].status, "partial")
            self.assertEqual(entries[0].timestamp.isoformat(), "2026-07-26T15:05:00+00:00")


class MasterDataEntriesTest(unittest.TestCase):
    def test_uses_validation_status_and_ingested_at(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest_dir = Path(temporary)
            _write(
                manifest_dir / "master_data_2026.json",
                {
                    "season": 2026,
                    "ingested_at": "2026-01-01T00:00:00Z",
                    "validation": {"status": "valid"},
                },
            )
            entries = _master_data_entries(manifest_dir)
            self.assertEqual(entries[0].status, "valid")
            self.assertIn("2026", entries[0].label)


class SessionEntriesTest(unittest.TestCase):
    def test_timestamp_is_the_latest_endpoint_retrieval(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest_dir = Path(temporary)
            _write(
                manifest_dir / "openf1_sessions" / "session_11342_abc.json",
                {
                    "session_key": 11342,
                    "session_type": "race",
                    "status": "available",
                    "endpoints": {
                        "laps": {"retrieved_at": "2026-07-26T15:00:00Z"},
                        "weather": {"retrieved_at": "2026-07-26T16:00:00Z"},
                    },
                },
            )
            entries = _session_entries(manifest_dir)
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0].timestamp.isoformat(), "2026-07-26T16:00:00+00:00")
            self.assertIn("11342", entries[0].label)

    def test_falls_back_to_file_mtime_without_any_retrieved_at(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest_dir = Path(temporary)
            path = manifest_dir / "openf1_sessions" / "session_1_abc.json"
            _write(path, {"session_key": 1, "status": "available", "endpoints": {}})
            entries = _session_entries(manifest_dir)
            self.assertEqual(len(entries), 1)
            self.assertIsNotNone(entries[0].timestamp)


class GeometryEntriesTest(unittest.TestCase):
    def test_reads_status_and_session_key(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            manifest_dir = Path(temporary)
            _write(
                manifest_dir / "geometries" / "geometry_11342_abc.json",
                {"session_key": 11342, "status": "available"},
            )
            entries = _geometry_entries(manifest_dir)
            self.assertEqual(entries[0].status, "available")
            self.assertIn("11342", entries[0].label)


class CollectPipelineActivityTest(unittest.TestCase):
    def test_merges_all_kinds_sorted_newest_first_and_respects_limit(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            job_dir = root / "jobs"
            manifest_dir = root / "manifests"

            _write(
                job_dir / "job_1.json",
                {"status": "running", "intent": {"purpose": "weekend", "meeting_key": 1}},
            )
            _write(
                manifest_dir / "master_data_2026.json",
                {
                    "season": 2026,
                    "ingested_at": "2020-01-01T00:00:00Z",
                    "validation": {"status": "valid"},
                },
            )
            _write(
                manifest_dir / "master_data_2030.json",
                {
                    "season": 2030,
                    "ingested_at": "2030-01-01T00:00:00Z",
                    "validation": {"status": "valid"},
                },
            )
            # The job file has no timestamp field, so it falls back to file
            # mtime (now); force it newer than the others explicitly ordered
            # by their own recorded timestamps for a deterministic sort check.
            time.sleep(0.01)

            entries = collect_pipeline_activity(
                limit=2, job_dir=job_dir, manifest_dir=manifest_dir
            )
            self.assertEqual(len(entries), 2)
            # Newest (2030 master-data entry) first.
            self.assertEqual(entries[0].kind, "Season master data")
            self.assertIn("2030", entries[0].label)


class RunningJobEntriesTest(unittest.TestCase):
    def test_selects_only_dashboard_jobs_with_running_status(self) -> None:
        entries = [
            ActivityEntry(
                kind="Dashboard job",
                label="weekend · meeting 1",
                status="running",
                timestamp=pd.Timestamp.now(tz="UTC"),
                detail={"job_id": "abc"},
                path=Path("job_abc.json"),
            ),
            ActivityEntry(
                kind="Dashboard job",
                label="weekend · meeting 2",
                status="available",
                timestamp=pd.Timestamp.now(tz="UTC"),
                detail={"job_id": "def"},
                path=Path("job_def.json"),
            ),
            ActivityEntry(
                kind="Weekend pipeline",
                label="meeting 3",
                status="running",
                timestamp=pd.Timestamp.now(tz="UTC"),
                detail={},
                path=Path("weekend.json"),
            ),
        ]
        running = _running_job_entries(entries)
        self.assertEqual([entry.detail["job_id"] for entry in running], ["abc"])


class RecentEventCountTest(unittest.TestCase):
    def test_counts_only_events_within_the_window(self) -> None:
        now = datetime.now(timezone.utc)
        events = [
            {"recorded_at": now.isoformat()},
            {"recorded_at": (now - timedelta(seconds=30)).isoformat()},
            {"recorded_at": (now - timedelta(seconds=90)).isoformat()},
        ]
        self.assertEqual(_recent_event_count(events, window_seconds=60.0), 2)

    def test_empty_events_count_as_zero(self) -> None:
        self.assertEqual(_recent_event_count([]), 0)


class StatusToneTest(unittest.TestCase):
    def test_maps_reused_results_to_ok_and_unknown_to_none(self) -> None:
        self.assertEqual(status_tone("stale"), "ok")
        self.assertEqual(status_tone("PARTIAL"), "warn")
        self.assertEqual(status_tone("unavailable"), "bad")
        self.assertEqual(status_tone("something"), "none")


class StalledJobTest(unittest.TestCase):
    def entry(self, started: pd.Timestamp) -> ActivityEntry:
        return ActivityEntry(
            kind="Dashboard job",
            label="weekend · meeting 1",
            status="running",
            timestamp=started,
            detail={"job_id": "abc"},
            path=Path("job_abc.json"),
        )

    def test_running_job_without_progress_for_long_is_stalled(self) -> None:
        now = pd.Timestamp("2026-10-18T18:00:00Z")
        old = self.entry(now - pd.Timedelta(hours=2))

        self.assertTrue(is_stalled(old, [], now))
        recent_event = [{"recorded_at": (now - pd.Timedelta(minutes=1)).isoformat()}]
        self.assertFalse(is_stalled(old, recent_event, now))
        self.assertFalse(is_stalled(self.entry(now - pd.Timedelta(minutes=5)), [], now))


class SeasonCoverageTest(unittest.TestCase):
    def manifest(self, meeting: int, completed: str, weather: str) -> dict:
        return {
            "status": "partial",
            "completed_at": completed,
            "selection": {"season": 2026, "meeting_key": meeting},
            "context": {
                "circuit_country": "Austria",
                "circuit_location": "Spielberg",
                "target_session": {"scheduled_start_utc": "2026-06-28T13:00:00+00:00"},
            },
            "jobs": {
                "openf1_weekend_facts": {
                    "status": "partial",
                    "sessions": [{"status": "stale"}, {"status": "partial"}],
                },
                "wikidata": {"status": "available"},
                "open_meteo": {"status": weather},
                "track_geometry": {"status": "available"},
            },
        }

    def test_uses_latest_run_per_meeting_and_counts_sessions(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            _write(
                root / "weekend_weather_pipeline_1288_a.json",
                self.manifest(1288, "2026-10-17T14:00:00Z", "unavailable"),
            )
            _write(
                root / "weekend_weather_pipeline_1288_b.json",
                self.manifest(1288, "2026-10-18T14:00:00Z", "available"),
            )
            other_season = self.manifest(9, "2026-10-18T14:00:00Z", "available")
            other_season["selection"]["season"] = 2024
            _write(root / "weekend_weather_pipeline_9_a.json", other_season)

            coverage = season_coverage(2026, root)

            self.assertEqual(coverage["meeting_key"].tolist(), [1288])
            row = coverage.iloc[0]
            self.assertEqual(row["open_meteo"], "available")
            self.assertEqual((row["sessions_ok"], row["sessions_total"]), (1, 2))
            self.assertEqual(row["weekend"], "Austria · Spielberg")
            self.assertEqual(meeting_names(root)[1288], "Austria · Spielberg")

    def test_empty_directory_gives_empty_frame(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            self.assertTrue(season_coverage(2026, Path(temporary)).empty)


if __name__ == "__main__":
    unittest.main()
