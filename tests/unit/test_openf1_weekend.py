from __future__ import annotations

import unittest

from f1_pipeline.sources.openf1_weekend import plan_weekend_sessions


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
        self.assertIn("pit", plans[-1]["required_endpoints"])

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


if __name__ == "__main__":
    unittest.main()

