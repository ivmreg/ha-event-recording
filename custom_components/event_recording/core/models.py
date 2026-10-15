"""Data models and serializable definitions for neutral Event Recording core.

Follows the pure Python boundary: zero Home Assistant imports, zero AI imports.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


def utc_now() -> datetime:
    """Return current UTC datetime with timezone info."""
    return datetime.now(UTC)


def ensure_utc(dt: datetime | str) -> datetime:
    """Ensure datetime has UTC timezone."""
    if isinstance(dt, str):
        dt_val = datetime.fromisoformat(dt)
    else:
        dt_val = dt
    if dt_val.tzinfo is None:
        return dt_val.replace(tzinfo=UTC)
    return dt_val.astimezone(UTC)


def format_iso(dt: datetime | None) -> str | None:
    """Format datetime as UTC ISO-8601 string."""
    if dt is None:
        return None
    utc_dt = ensure_utc(dt)
    return utc_dt.isoformat()


def sanitize_capture_error(error_code: Any) -> str | None:
    """Produce bounded, non-sensitive error code without leaking local paths or keys."""
    if error_code is None:
        return None
    raw = str(error_code).strip()
    if not raw:
        return None
    if ":" in raw or "\\" in raw or "/" in raw:
        return "INTERNAL_ERROR"
    clean = "".join(c for c in raw if c.isalnum() or c in ("_", "-")).upper()
    return clean[:64] if clean else "INTERNAL_ERROR"


def validate_bounded_string(
    val: Any,
    name: str,
    max_len: int = 255,
    allow_none: bool = False,
) -> str | None:
    """Validate a bounded non-sensitive string without control characters."""
    if val is None:
        if allow_none:
            return None
        raise ValueError(f"{name} cannot be None")
    if not isinstance(val, str):
        raise ValueError(f"{name} must be a string, got {type(val).__name__}")
    clean = val.strip()
    if not clean and not allow_none:
        raise ValueError(f"{name} cannot be empty")
    if len(clean) > max_len:
        raise ValueError(f"{name} exceeds maximum length of {max_len}")
    if any(c in clean for c in "\0\r\n"):
        raise ValueError(f"{name} contains prohibited control characters")
    return clean


def validate_finite_number(
    val: Any,
    name: str,
    min_val: float = 0.0,
    allow_zero: bool = True,
) -> float:
    """Validate a finite, non-NaN positive float or int."""
    if isinstance(val, bool) or not isinstance(val, (int, float)):
        raise ValueError(f"{name} must be a number, got {type(val).__name__}")
    num = float(val)
    if not math.isfinite(num):
        raise ValueError(f"{name} must be finite (got {val})")
    if allow_zero:
        if num < min_val:
            raise ValueError(f"{name} must be >= {min_val}, got {num}")
    else:
        if num <= min_val:
            raise ValueError(f"{name} must be > {min_val}, got {num}")
    return num


def validate_payload_json(
    payload: Any,
    name: str = "payload",
    max_bytes: int = 131072,
) -> dict[str, Any]:
    """Validate JSON payload dictionary and reject NaN, inf, or oversized blobs."""
    if payload is None:
        return {}
    if not isinstance(payload, dict):
        raise ValueError(f"{name} must be a dictionary, got {type(payload).__name__}")
    try:
        dumped = json.dumps(payload, allow_nan=False)
    except (ValueError, TypeError) as err:
        raise ValueError(f"{name} cannot be serialized to valid finite JSON: {err}") from err
    if len(dumped.encode("utf-8")) > max_bytes:
        raise ValueError(f"{name} exceeds maximum JSON size of {max_bytes} bytes")
    return payload


def validate_safe_id(val: Any, name: str = "id") -> str:
    """Validate safe internal/external identifier rejecting path components and control chars."""
    if not isinstance(val, str):
        raise ValueError(f"{name} must be a string, got {type(val).__name__}")
    clean = val.strip()
    if not clean or len(clean) > 64:
        raise ValueError(f"{name} must be 1-64 characters")
    if any(c in clean for c in "/\\..\0\r\n\t") or not re.match(r"^[a-zA-Z0-9_-]+$", clean):
        raise ValueError(f"Invalid characters or path traversal attempt in {name}: '{val}'")
    return clean


@dataclass(frozen=True)
class MediaItem:
    """Represents a finalized or partial owned media asset attached to an event."""

    id: str
    kind: str  # "video" | "image"
    mime_type: str
    file_name: str  # Root-relative path inside owned media root; NEVER in public DTO
    duration: float | None = None
    size_bytes: int = 0
    available: bool = True
    is_pinned: bool = False
    content_hash: str | None = None
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class SourceEventRecord:
    """Journaled triggering or contributing event."""

    source_id: str
    kind: str  # Arbitrary bounded string (e.g. "motion", "contact", "doorbell", "mqtt")
    occurred_at: datetime
    idempotency_key: str
    id: int | None = None
    label: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class RecordedEvent:
    """Top-level recorded event lifecycle record."""

    event_id: str
    source_id: str
    kind: str  # Arbitrary bounded event kind
    start_at: datetime
    desired_capture_end: datetime
    capture_state: str  # requested|recording|ready|partial|failed|interrupted|expired
    capture_error: str | None = None
    label: str | None = None
    end_at: datetime | None = None
    media_revision: int = 1  # Increments ONLY when media-set changes or first finalization
    revision: int = 1  # Global mutation revision
    source_events: tuple[SourceEventRecord, ...] = ()
    media: tuple[MediaItem, ...] = ()
    coverage: dict[str, Any] = field(default_factory=dict)
    annotations: dict[str, Any] = field(default_factory=dict)  # Keyed by namespace
    access_entities: tuple[str, ...] = ()  # Entity IDs controlling access
    owner_user_id: str | None = None  # Fallback admin/owner check if access_entities empty
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class RecordingIntent:
    """Durable recording intent queued for physical or virtual capture adapters."""

    intent_id: str
    event_id: str
    resource_key: str  # Stream serialization key (e.g. camera entity or stream URL)
    operation: str  # "start_or_extend" | "end_after"
    window_seconds: float
    lookback_seconds: float = 0.0
    hold: bool = False
    recorder_config: dict[str, Any] = field(default_factory=dict)  # Opaque private config; NEVER public DTO
    status: str = "pending"  # "pending" | "leased" | "completed" | "failed" | "cancelled"
    worker_id: str | None = None
    leased_until: datetime | None = None
    error: str | None = None
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class ResourceSession:
    """Active event tracking and window coalescing state for a resource_key."""

    resource_key: str
    active_event_id: str | None = None
    last_activity_at: datetime = field(default_factory=utc_now)
    desired_capture_end: datetime = field(default_factory=utc_now)
    hold: bool = False
    tail_window_seconds: float = 0.0
    recorder_config: dict[str, Any] = field(default_factory=dict)
    access_entities: tuple[str, ...] = ()
    owner_user_id: str | None = None
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class MediaPinLease:
    """Opaque time-bounded lease preventing retention deletion."""

    pin_id: str
    event_id: str
    lease_token: str
    expires_at: datetime
    reason: str | None = None
    created_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class RetentionResult:
    """Summary of retention pruning enforcement."""

    deleted_files: int = 0
    freed_bytes: int = 0
    marked_expired_events: int = 0
    marked_missing_media: int = 0
    errors: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ReconcileResult:
    """Summary of store reconciliation and recovery."""

    interrupted_events: int = 0
    expired_sessions: int = 0
    reconciled_intents: int = 0
    orphaned_files_cleaned: int = 0


def to_public_dto(event: RecordedEvent) -> dict[str, Any]:
    """Render public DTO without exposing absolute paths, internal credentials, or auth metadata."""
    source_events_dto: list[dict[str, Any]] = []
    for s in event.source_events:
        item: dict[str, Any] = {
            "type": s.kind,
            "occurred_at": format_iso(s.occurred_at),
            "source_id": s.source_id,
        }
        if s.label is not None:
            item["label"] = s.label
        source_events_dto.append(item)

    media_dto = [
        {
            "id": m.id,
            "kind": m.kind,
            "mime_type": m.mime_type,
            "duration": m.duration,
            "available": m.available,
        }
        for m in event.media
    ]

    # Process annotations: flatten safely, hide obsolete annotations, hide raw reservation owner/payload internals
    public_annotations: dict[str, Any] = {}
    for ns, ann in event.annotations.items():
        if isinstance(ann, dict):
            # Obsolete check: hide if media_revision does not match current event.media_revision
            ann_media_rev = ann.get("media_revision", ann.get("expected_media_revision"))
            if ann_media_rev is not None and ann_media_rev != event.media_revision:
                continue

            # Clean raw reservation owner / payload internals
            clean_ann = {
                k: v
                for k, v in ann.items()
                if k
                not in (
                    "worker_id",
                    "reserved_by",
                    "reserved_until",
                    "leased_until",
                    "lease_token",
                    "payload",
                )
            }
            public_annotations[ns] = clean_ann

    return {
        "event_id": event.event_id,
        "source_id": event.source_id,
        "kind": event.kind,
        "label": event.label,
        "start_at": format_iso(event.start_at),
        "end_at": format_iso(event.end_at),
        "capture_state": event.capture_state,
        "capture_error": event.capture_error,
        "media_revision": event.media_revision,
        "revision": event.revision,
        "source_events": source_events_dto,
        "media": media_dto,
        "coverage": event.coverage,
        "annotations": public_annotations,
    }

