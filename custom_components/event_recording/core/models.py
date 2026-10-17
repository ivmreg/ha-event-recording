"""Data models and serializable definitions for neutral Event Recording core.

Follows the pure Python boundary: zero Home Assistant imports, zero AI imports.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from .validation import (  # noqa: F401
    validate_bounded_string,
    validate_correlation_id,
    validate_finite_number,
    validate_payload_json,
    validate_safe_id,
)


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



def validate_timing_metadata(
    requested_start: datetime | None = None,
    requested_end: datetime | None = None,
    capture_start: datetime | None = None,
    capture_end: datetime | None = None,
    time_quality: str | None = None,
    clock_source: str | None = None,
    time_uncertainty_seconds: float | None = None,
    start_offset_seconds: float | None = None,
) -> None:
    """Validate that timing metadata is finite, UTC timezone-aware, and logically ordered."""
    for dt, label in (
        (requested_start, "requested_start"),
        (requested_end, "requested_end"),
        (capture_start, "capture_start"),
        (capture_end, "capture_end"),
    ):
        if dt is not None:
            if not isinstance(dt, datetime):
                raise ValueError(f"{label} must be a datetime, got {type(dt).__name__}")
            if dt.tzinfo is None or dt.utcoffset() != timedelta(0):
                raise ValueError(f"{label} must be a timezone-aware UTC datetime")

    if requested_start is not None and requested_end is not None:
        if requested_start > requested_end:
            raise ValueError(
                f"requested_start ({requested_start}) must be <= requested_end ({requested_end})"
            )

    if capture_start is not None and capture_end is not None:
        if capture_start > capture_end:
            raise ValueError(
                f"capture_start ({capture_start}) must be <= capture_end ({capture_end})"
            )

    if time_uncertainty_seconds is not None:
        if (
            not isinstance(time_uncertainty_seconds, (int, float))
            or not math.isfinite(time_uncertainty_seconds)
            or time_uncertainty_seconds < 0.0
        ):
            raise ValueError(
                f"time_uncertainty_seconds must be a finite non-negative float, got {time_uncertainty_seconds}"
            )

    if start_offset_seconds is not None:
        if not isinstance(start_offset_seconds, (int, float)) or not math.isfinite(
            start_offset_seconds
        ):
            raise ValueError(
                f"start_offset_seconds must be a finite float, got {start_offset_seconds}"
            )

    if time_quality is not None and time_quality not in (
        "device_clock",
        "host_recorded",
        "estimated",
        "provider_reported",
        "unknown",
    ):
        raise ValueError(f"Invalid time_quality: {time_quality}")

    if clock_source is not None and (not isinstance(clock_source, str) or len(clock_source) > 64):
        raise ValueError(f"clock_source must be a string up to 64 chars, got {clock_source}")

    return True


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
    requested_start: datetime | None = None
    requested_end: datetime | None = None
    capture_start: datetime | None = None
    capture_end: datetime | None = None
    time_quality: str | None = "unknown"
    clock_source: str | None = "unknown"
    time_uncertainty_seconds: float | None = None
    start_offset_seconds: float | None = None


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
    correlation_id: str | None = None
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
    correlation_id: str | None = None
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


def to_public_dto(event: RecordedEvent, *, omit_correlation: bool = False) -> dict[str, Any]:
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

    media_dto = []
    for m in event.media:
        m_dict: dict[str, Any] = {
            "id": m.id,
            "kind": m.kind,
            "mime_type": m.mime_type,
            "duration": m.duration,
            "available": m.available,
            "time_quality": m.time_quality or "unknown",
            "clock_source": m.clock_source or "unknown",
        }
        if m.requested_start is not None:
            m_dict["requested_start"] = format_iso(m.requested_start)
        if m.requested_end is not None:
            m_dict["requested_end"] = format_iso(m.requested_end)
        if m.capture_start is not None:
            m_dict["capture_start"] = format_iso(m.capture_start)
        if m.capture_end is not None:
            m_dict["capture_end"] = format_iso(m.capture_end)
        if m.time_uncertainty_seconds is not None:
            m_dict["time_uncertainty_seconds"] = m.time_uncertainty_seconds
        if m.start_offset_seconds is not None:
            m_dict["start_offset_seconds"] = m.start_offset_seconds
        media_dto.append(m_dict)

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

    res: dict[str, Any] = {
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
    if not omit_correlation and event.correlation_id is not None:
        res["correlation_id"] = event.correlation_id
    return res


@dataclass(frozen=True)
class Incident:
    """Neutral incident model grouping multiple recorded events many-to-many."""

    incident_id: str
    anchor_at: datetime  # Immutable anchor first created for keyset pagination
    span_start: datetime  # Recomputed dynamically across member events
    span_end: datetime  # Recomputed dynamically across member events
    membership_revision: int = 1  # Increments ONLY on actual add or remove of members
    reason: str | None = None
    annotations: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class IncidentMembership:
    """Many-to-many membership record linking an event to an incident."""

    incident_id: str
    event_id: str
    added_at: datetime = field(default_factory=utc_now)
    reason: str | None = None


@dataclass(frozen=True)
class IncidentInputs:
    """Consistent snapshot of an incident, its member events, and input freshness token."""

    incident: Incident
    events: tuple[RecordedEvent, ...]
    input_token: str

    @property
    def membership_revision(self) -> int:
        return self.incident.membership_revision

    @property
    def canonical_token(self) -> str:
        return self.input_token

    @property
    def member_events(self) -> tuple[RecordedEvent, ...]:
        return self.events


def canonical_annotation_payload(ann: dict[str, Any] | None) -> dict[str, Any]:
    """Extract canonical payload dict from either a raw annotation envelope or a hydrated flat annotation dict."""
    if not ann or not isinstance(ann, dict):
        return {}
    if "payload" in ann and isinstance(ann["payload"], dict):
        return ann["payload"]
    reserved_meta = {
        "status",
        "generation",
        "media_revision",
        "updated_at",
        "worker_id",
        "reserved_by",
        "reserved_until",
        "leased_until",
        "lease_token",
        "input_token",
        "profile_digest",
        "input_namespace",
    }
    return {k: v for k, v in ann.items() if k not in reserved_meta}


def canonical_json_dumps(obj: Any) -> str:
    """Canonical JSON serialization with sorted keys and compact separators."""
    try:
        return json.dumps(obj, sort_keys=True, separators=(",", ":"))
    except Exception:
        return str(obj)


def compute_incident_input_token(
    membership_revision: int,
    member_events: Any,
    input_namespace: str,
) -> str:
    """Compute canonical hash covering membership revision, member media revisions, and input namespace annotation state.

    Unrelated annotations do NOT invalidate this token.
    """
    parts: list[str] = [f"mrev:{membership_revision}", f"ns:{input_namespace}"]
    sorted_events = sorted(member_events, key=lambda e: getattr(e, "event_id", ""))
    for ev in sorted_events:
        ann = ev.annotations.get(input_namespace) if hasattr(ev, "annotations") else None
        if isinstance(ann, dict):
            gen = ann.get("generation", 1)
            status = ann.get("status", "ready")
            payload = canonical_annotation_payload(ann)
            p_str = canonical_json_dumps(payload)
            p_hash = hashlib.sha256(p_str.encode("utf-8")).hexdigest()[:16]
            ann_repr = f"g={gen}:s={status}:p={p_hash}"
        else:
            ann_repr = "none"
        m_rev = getattr(ev, "media_revision", 1)
        ev_id = getattr(ev, "event_id", "")
        parts.append(f"{ev_id}:{m_rev}:{ann_repr}")
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def to_public_incident_dto(
    incident: Incident,
    member_events: Any = None,
) -> dict[str, Any]:
    """Convert an Incident to a public JSON-serializable DTO."""
    dto: dict[str, Any] = {
        "incident_id": incident.incident_id,
        "anchor_at": format_iso(incident.anchor_at),
        "span_start": format_iso(incident.span_start),
        "span_end": format_iso(incident.span_end),
        "membership_revision": incident.membership_revision,
        "reason": incident.reason,
        "annotations": incident.annotations,
        "created_at": format_iso(incident.created_at),
        "updated_at": format_iso(incident.updated_at),
    }
    if member_events is not None:
        dto["events"] = [to_public_dto(e) for e in member_events]
    return dto
