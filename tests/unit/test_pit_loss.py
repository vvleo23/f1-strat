from __future__ import annotations

import unittest

from f1_pipeline.dashboard.pit_loss import PitLossConfigError, load_pit_loss


class PitLossConfigTest(unittest.TestCase):
    def test_hungary_uses_average_value_from_source_asset(self) -> None:
        result = load_pit_loss("Hungarian Grand Prix", "Hungary", "Hungaroring")

        self.assertIsNotNone(result)
        self.assertAlmostEqual(result.seconds, 21.6355)
        self.assertEqual(result.source_asset, "docs/assets/pitstop.png")

    def test_unknown_circuit_has_no_global_fallback(self) -> None:
        result = load_pit_loss("Las Vegas Grand Prix", "Las Vegas Strip Circuit")

        self.assertIsNone(result)

    def test_ambiguous_identifiers_fail_closed(self) -> None:
        with self.assertRaises(PitLossConfigError):
            load_pit_loss("Hungarian Grand Prix", "Belgian Grand Prix")


if __name__ == "__main__":
    unittest.main()
