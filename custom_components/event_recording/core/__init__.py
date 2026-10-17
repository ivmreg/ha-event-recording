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
    Incident,
    IncidentInputs,
    IncidentMembership,
    MediaItem,
    MediaPinLease,
    ReconcileResult,
    RecordedEvent,
    RecordingIntent,
    ResourceSession,
    RetentionResult,
    SourceEventRecord,
    compute_incident_input_token,
    ensure_utc,
    format_iso,
    sanitize_capture_error,
    to_public_dto,
    to_public_incident_dto,
    utc_now,
    validate_correlation_id,
    validate_timing_metadata,
)
from .storage_ledger import StorageCapacityError, StorageLedger
from .store import JournalStore

__all__ = [
    "FFmpegProbe",
    "Incident",
    "IncidentInputs",
    "IncidentMembership",
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
    "StorageCapacityError",
    "StorageLedger",
    "compute_incident_input_token",
    "ensure_utc",
    "format_iso",
    "sanitize_capture_error",
    "to_public_dto",
    "to_public_incident_dto",
    "unlink_promoted_media",
    "utc_now",
    "validate_correlation_id",
    "validate_timing_metadata",
    "verify_media_path",
]
