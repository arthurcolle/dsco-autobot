"""
Calculated field utilities for memory overlay.

Provides common calculated fields like token counts, salience scores,
and temporal decay functions.
"""

import math
import time
from typing import Any, Callable


def calc_tokens(field: str) -> Callable[[Any], int]:
    """
    Calculate token count from a text field.

    Args:
        field: Name of field containing text

    Returns:
        Function that computes token count

    Example:
        CalculatedFieldSpec(name="tokens", fn=calc_tokens("text"))
    """
    def _fn(obj: Any) -> int:
        txt = getattr(obj, field, "") or ""
        # Naive token count (swap with tokenizer later)
        return len(txt.strip().split())
    return _fn


def calc_char_count(field: str) -> Callable[[Any], int]:
    """Calculate character count from a text field."""
    def _fn(obj: Any) -> int:
        txt = getattr(obj, field, "") or ""
        return len(txt)
    return _fn


def calc_salience(
    score_field: str = "score",
    created_field: str = "created_ts",
    updated_field: str = "updated_ts",
    alpha: float = 0.6,
    beta: float = 0.3,
    gamma: float = 0.1,
    half_life_seconds: float = 7 * 24 * 3600.0  # 7 days
) -> Callable[[Any], float]:
    """
    Calculate salience score with recency/frequency/explicit components.

    Formula:
        salience = α * recency + β * frequency + γ * explicit_score

    Where:
        recency = exp(-ln(2) * age / half_life)
        frequency = tanh(updates_per_day / 10)
        explicit_score = tanh(score / 10)

    Args:
        score_field: Field containing explicit score
        created_field: Field containing creation timestamp
        updated_field: Field containing last update timestamp
        alpha: Recency weight (default: 0.6)
        beta: Frequency weight (default: 0.3)
        gamma: Explicit score weight (default: 0.1)
        half_life_seconds: Decay half-life in seconds (default: 7 days)

    Returns:
        Function that computes salience

    Example:
        CalculatedFieldSpec(
            name="salience",
            fn=calc_salience(
                score_field="score",
                created_field="created_ts",
                alpha=0.65,
                beta=0.25,
                gamma=0.10
            )
        )
    """
    def _fn(obj: Any) -> float:
        now = time.time()
        created = getattr(obj, created_field, None) or now
        updated = getattr(obj, updated_field, None) or created
        score = float(getattr(obj, score_field, 0.0) or 0.0)

        # Recency: exponential decay
        age = max(0.0, now - created)
        recency = math.exp(-math.log(2.0) * age / half_life_seconds)

        # Frequency: proxy based on update activity
        days_since_create = max(0.0, (updated - created)) / (24 * 3600.0)
        freq = 1.0 + days_since_create  # 1 + days of activity
        frequency = math.tanh(freq / 10.0)

        # Explicit score (normalized)
        explicit = math.tanh(score / 10.0)

        # Weighted combination
        s = alpha * recency + beta * frequency + gamma * explicit
        return float(round(s, 6))

    return _fn


def calc_word_count(field: str) -> Callable[[Any], int]:
    """Calculate word count (more accurate than tokens)."""
    def _fn(obj: Any) -> int:
        import re
        txt = getattr(obj, field, "") or ""
        words = re.findall(r'\b\w+\b', txt)
        return len(words)
    return _fn


def calc_age_seconds(created_field: str = "created_ts") -> Callable[[Any], float]:
    """Calculate age in seconds since creation."""
    def _fn(obj: Any) -> float:
        created = getattr(obj, created_field, None)
        if not created:
            return 0.0
        return max(0.0, time.time() - created)
    return _fn


def calc_age_days(created_field: str = "created_ts") -> Callable[[Any], float]:
    """Calculate age in days since creation."""
    def _fn(obj: Any) -> float:
        created = getattr(obj, created_field, None)
        if not created:
            return 0.0
        return max(0.0, (time.time() - created) / (24 * 3600.0))
    return _fn


def calc_hash(field: str, algorithm: str = "sha256") -> Callable[[Any], str]:
    """Calculate hash of a field value."""
    import hashlib

    def _fn(obj: Any) -> str:
        val = str(getattr(obj, field, ""))
        h = getattr(hashlib, algorithm)()
        h.update(val.encode())
        return h.hexdigest()
    return _fn
