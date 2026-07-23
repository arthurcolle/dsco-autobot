"""Real-time dashboard and progress tracking"""

from parallel_websearch_server.dashboard.progress_tracker import (
    ProgressTracker,
    ProgressMetrics,
    ProgressEvent,
    ProgressEventType,
    get_progress_tracker,
)

__all__ = [
    "ProgressTracker",
    "ProgressMetrics",
    "ProgressEvent",
    "ProgressEventType",
    "get_progress_tracker",
]
