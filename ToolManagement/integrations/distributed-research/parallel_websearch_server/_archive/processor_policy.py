"""Helpers for enforcing processor selection policies."""

from typing import Iterable, Optional, Sequence, Union

from fastapi import HTTPException

from parallel_websearch_server.config import settings
from parallel_websearch_server.models import ProcessorType

ProcessorValue = Union[str, ProcessorType]


def _allowed_values() -> Optional[Sequence[str]]:
    """Return the configured allowlist (lowercase) if one is set."""
    allowed = settings.allowed_parallel_processors or None
    if not allowed:
        return None
    return allowed


def _normalize_processor_value(processor: ProcessorValue) -> str:
    """Convert enum/string inputs to the lowercase value we compare."""
    if isinstance(processor, ProcessorType):
        raw_value = processor.value
    else:
        raw_value = str(processor)
    return raw_value.strip().lower()


def ensure_processors_allowed(processors: Iterable[Optional[ProcessorValue]]) -> None:
    """Validate that every processor in the iterable passes the allowlist."""
    allowed = _allowed_values()
    if not allowed:
        return

    allowed_set = set(allowed)
    for processor in processors:
        if processor is None:
            continue
        normalized = _normalize_processor_value(processor)
        if normalized in allowed_set:
            continue
        raise HTTPException(
            status_code=403,
            detail={
                "error": f"Processor '{normalized}' is disabled by server policy.",
                "allowed_processors": list(allowed),
            },
        )


def ensure_processor_allowed(processor: Optional[ProcessorValue]) -> None:
    """Validate a single processor."""
    ensure_processors_allowed([processor])
