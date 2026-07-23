from __future__ import annotations

from pathlib import Path
import unittest

from simulation.silicon import capability_probe


class SiliconEvidenceTests(unittest.TestCase):
    def test_saved_tool_passes_match_current_sources(self) -> None:
        project_root = Path(__file__).resolve().parents[2]
        capabilities = capability_probe(project_root)
        gates = {gate["id"]: gate for gate in capabilities["gates"]}
        self.assertEqual(gates["C2"]["status"], "PASS", gates["C2"])
        self.assertEqual(gates["C3"]["status"], "PASS", gates["C3"])
        self.assertEqual(gates["C4"]["status"], "BLOCKED")
        self.assertFalse(capabilities["manufacturable_gpu"])


if __name__ == "__main__":
    unittest.main()
