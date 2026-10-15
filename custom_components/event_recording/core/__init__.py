"""Pure Python core for Event Recording.

Follows the strict boundary: zero Home Assistant imports, zero AI imports.
"""

from __future__ import annotations

from .media import (
    FFmpegProbe,
    InvalidMediaError,
    MediaImportError,
    MediaProbe,
    MediaProbeResult,
    MediaTypeMismatchError,
    PathTraversalSecurityError,
    ProbeUnavailableError,
    unlink_promoted_media,
    verify_media_path,
)
from .models import (
    MediaItem,
    MediaPinLease,
    ReconcileResult,
    RecordedEvent,
    RecordingIntent,
    ResourceSession,
    RetentionResult,
    SourceEventRecord,
    ensure_utc,
    format_iso,
    sanitize_capture_error,
    to_public_dto,
    utc_now,
)
from .store import JournalStore

__all__ = [
    "FFmpegProbe",
    "InvalidMediaError",
    "JournalStore",
    "MediaImportError",
    "MediaItem",
    "MediaPinLease",
    "MediaProbe",
    "MediaProbeResult",
    "MediaTypeMismatchError",
    "PathTraversalSecurityError",
    "ProbeUnavailableError",
    "ReconcileResult",
    "RecordedEvent",
    "RecordingIntent",
    "ResourceSession",
    "RetentionResult",
    "SourceEventRecord",
    "ensure_utc",
    "format_iso",
    "sanitize_capture_error",
    "to_public_dto",
    "unlink_promoted_media",
    "utc_now",
    "verify_media_path",
]
