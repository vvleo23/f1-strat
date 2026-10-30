from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from f1_pipeline.demo_data import _build_session


class DemoDataTest(TestCase):
    def test_lap_gaps_are_rejected_before_demo_files_are_written(self) -> None:
        source = SimpleNamespace(
            frames={"laps": pd.DataFrame({"driver_number": [12, 12], "lap_number": [39, 41]})}
        )
        with patch("f1_pipeline.demo_data.load_session_bundle", return_value=source), patch(
            "f1_pipeline.demo_data._write_frame"
        ) as write_frame:
            with self.assertRaisesRegex(ValueError, "internal lap gaps"):
                _build_session()
            write_frame.assert_not_called()
