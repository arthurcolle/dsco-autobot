from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Sequence


LANES = 8
INT8_MIN = -128
INT8_MAX = 127


def _validated_vector(values: Iterable[int], name: str) -> tuple[int, ...]:
    vector = tuple(values)
    if len(vector) != LANES:
        raise ValueError(f"{name} must contain exactly {LANES} lanes")
    if any(not isinstance(value, int) or isinstance(value, bool) for value in vector):
        raise TypeError(f"{name} lanes must be integers")
    if any(value < INT8_MIN or value > INT8_MAX for value in vector):
        raise ValueError(f"{name} lane is outside signed INT8 range")
    return vector


def dot8(a: Sequence[int], b: Sequence[int]) -> int:
    """Exact signed INT8 dot product accumulated into a Python/INT32 result."""
    av = _validated_vector(a, "a")
    bv = _validated_vector(b, "b")
    result = sum(left * right for left, right in zip(av, bv, strict=True))
    if not -(2**31) <= result < 2**31:
        raise OverflowError("ACX-DOT8 result escaped signed INT32")
    return result


def verify_vectors(path: Path) -> dict[str, object]:
    document = json.loads(path.read_text(encoding="utf-8"))
    records = []
    for vector in document["vectors"]:
        actual = dot8(vector["a"], vector["b"])
        records.append({"id": vector["id"], "expected": vector["expected"], "actual": actual, "pass": actual == vector["expected"]})
    return {
        "contract": document["contract"]["name"],
        "passed": all(record["pass"] for record in records),
        "tests": len(records),
        "records": records,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute the ACX-DOT8 golden model")
    parser.add_argument("vectors", type=Path)
    args = parser.parse_args()
    result = verify_vectors(args.vectors)
    print(json.dumps(result, indent=2))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
