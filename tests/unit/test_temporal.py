from __future__ import annotations

import unittest

import pandas as pd

from f1_pipeline.temporal import TemporalCutError, cut_facts, decision_timestamp


class TemporalCutTest(unittest.TestCase):
    def test_requires_event_and_availability_at_or_before_decision_time(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "fact_id": "visible",
                    "event_time": "2026-07-26T09:59:00Z",
                    "available_at": "2026-07-26T10:00:00Z",
                },
                {
                    "fact_id": "late",
                    "event_time": "2026-07-26T09:58:00Z",
                    "available_at": "2026-07-26T10:01:00Z",
                },
                {
                    "fact_id": "future",
                    "event_time": "2026-07-26T10:01:00Z",
                    "available_at": "2026-07-26T09:59:00Z",
                },
                {
                    "fact_id": "unknown",
                    "event_time": None,
                    "available_at": None,
                },
            ]
        )

        result = cut_facts(frame, decision_time="2026-07-26T10:00:00Z")

        self.assertEqual(result["fact_id"].tolist(), ["visible"])
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["event_time"]))
        self.assertEqual(str(result["event_time"].dt.tz), "UTC")
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(result["available_at"]))
        self.assertEqual(str(result["available_at"].dt.tz), "UTC")

    def test_rejects_missing_temporal_columns(self) -> None:
        with self.assertRaisesRegex(TemporalCutError, "available_at"):
            cut_facts(
                pd.DataFrame([{"event_time": "2026-07-26T10:00:00Z"}]),
                decision_time="2026-07-26T10:00:00Z",
            )

    def test_naive_decision_time_is_interpreted_as_utc(self) -> None:
        self.assertEqual(
            decision_timestamp("2026-07-26T10:00:00"),
            pd.Timestamp("2026-07-26T10:00:00Z"),
        )


if __name__ == "__main__":
    unittest.main()

