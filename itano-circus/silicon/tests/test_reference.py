from __future__ import annotations

import json
from pathlib import Path
import sys
import unittest


SILICON_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SILICON_ROOT))

from reference_model import dot8, verify_vectors


class Dot8ReferenceTests(unittest.TestCase):
    def test_frozen_vectors(self) -> None:
        result = verify_vectors(SILICON_ROOT / "vectors.json")
        self.assertTrue(result["passed"], result)
        self.assertEqual(result["tests"], 6)

    def test_signed_extremes_fit_int32(self) -> None:
        self.assertEqual(dot8([127] * 8, [127] * 8), 129032)
        self.assertEqual(dot8([-128] * 8, [127] * 8), -130048)

    def test_rejects_wrong_width(self) -> None:
        with self.assertRaises(ValueError):
            dot8([1, 2], [3, 4])

    def test_rejects_out_of_range_lane(self) -> None:
        with self.assertRaises(ValueError):
            dot8([128] + [0] * 7, [0] * 8)

    def test_contract_is_explicit(self) -> None:
        document = json.loads((SILICON_ROOT / "vectors.json").read_text(encoding="utf-8"))
        self.assertEqual(document["contract"]["latency_cycles"], 1)
        self.assertEqual(document["contract"]["accumulator"], "signed int32, no saturation required for eight products")


if __name__ == "__main__":
    unittest.main()
