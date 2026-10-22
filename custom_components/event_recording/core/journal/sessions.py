"""Recording sessions, intent admission, leasing, and state transitions for JournalStore.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
Preserves positive acceptance ACK, atomic session coalescing, and idempotent state transitions.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

from ..coverage import compute_coverage
from ..models import (
    AbandonedCaptureCandidate,
    CaptureFinishResult,
    RecordedEvent,
    RecordingIntent,
    ResourceSession,
    ensure_utc,
    format_iso,
    sanitize_capture_error,
    utc_now,
    validate_bounded_string,
    validate_correlation_id,
    validate_finite_number,
    validate_payload_json,
)
from .common import JournalStoreBase

_LOGGER = logging.getLogger(__name__)

_TERMINAL_CAPTURE_STATES = frozenset(
    {"ready", "partial", "failed", "interrupted", "expired"}
)


class SessionsMixin(JournalStoreBase):
    """Encapsulates recording intent queueing, resource session leases, and capture states."""

    async def log_event(
        self,
        source_id: str,
        kind: str,
        occurred_at: datetime | str,
        idempotency_key: str,
        label: str | None = None,
        payload: dict[str, Any] | None = None,
        access_entities: Sequence[str] = (),
        owner_user_id: str | None = None,
        correlation_id: str | None = None,
        auto_association_config: dict[str, Any] | None = None,
        **extra_kwargs: Any,
    ) -> tuple[RecordedEvent, bool]:
        """Log an arbitrary neutral event without media capture requirement."""
        validate_bounded_string(source_id, "source_id")
        validate_bounded_string(kind, "kind")
        validate_bounded_string(idempotency_key, "idempotency_key")
        validate_bounded_string(label, "label", max_len=500, allow_none=True)
        validate_payload_json(payload, "payload")
        validate_bounded_string(owner_user_id, "owner_user_id", allow_none=True)
        if correlation_id is not None:
            validate_correlation_id(correlation_id)

        res, inserted, notify_tuple, inc_notify = await asyncio.to_thread(
            self._sync_log_event,
            source_id,
            kind,
            occurred_at,
            idempotency_key,
            label,
            payload,
            access_entities,
            owner_user_id,
            correlation_id,
            auto_association_config,
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        if inc_notify:
            self._notify_incident_subscribers(*inc_notify)
        return res, inserted

    def _sync_log_event(
        self,
        source_id: str,
        kind: str,
        occurred_at: datetime | str,
        idempotency_key: str,
        label: str | None,
        payload: dict[str, Any] | None,
        access_entities: Sequence[str],
        owner_user_id: str | None,
        correlation_id: str | None = None,
        auto_association_config: dict[str, Any] | None = None,
    ) -> tuple[RecordedEvent, bool, tuple[str, int] | None, tuple[str, int] | None]:
        dt = ensure_utc(occurred_at)
        dt_str = format_iso(dt) or ""
        now_str = format_iso(utc_now()) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            # Check duplicate strictly scoped to (source_id, idempotency_key)
            cur.execute(
                """
                SELECT sj.event_id, e.access_entities_json, e.owner_user_id
                FROM source_journal sj
                JOIN events e ON e.event_id = sj.event_id
                WHERE sj.source_id = ? AND sj.idempotency_key = ?;
                """,
                (source_id, idempotency_key),
            )
            existing = cur.fetchone()
            if existing:
                caller_entities = tuple(sorted(set(access_entities))) if access_entities else ()
                existing_entities = (
                    tuple(sorted(set(json.loads(existing["access_entities_json"]))))
                    if existing["access_entities_json"]
                    else ()
                )
                if (
                    caller_entities != existing_entities
                    or owner_user_id != existing["owner_user_id"]
                ):
                    raise PermissionError("Access denied")

                cur.execute("SELECT * FROM events WHERE event_id = ?;", (existing["event_id"],))
                ev_row = cur.fetchone()
                return getattr(self, "_sync_hydrate_event")(cur, ev_row), False, None, None

            cur.execute("BEGIN IMMEDIATE;")
            try:
                event_id = str(uuid.uuid4())
                access_list = sorted(set(access_entities)) if access_entities else []
                access_json = json.dumps(access_list)

                cur.execute(
                    """
                    INSERT INTO events (
                        event_id, source_id, kind, start_at, desired_capture_end,
                        capture_state, capture_error, label, end_at, media_revision, revision,
                        coverage_json, access_entities_json, owner_user_id, correlation_id,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'ready', NULL, ?, ?, 1, 1, '{}', ?, ?, ?, ?, ?);
                    """,
                    (
                        event_id,
                        source_id,
                        kind,
                        dt_str,
                        dt_str,
                        label,
                        dt_str,
                        access_json,
                        owner_user_id,
                        correlation_id,
                        now_str,
                        now_str,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO source_journal (
                        event_id, source_id, kind, occurred_at, idempotency_key,
                        label, payload_json, correlation_id, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        event_id,
                        source_id,
                        kind,
                        dt_str,
                        idempotency_key,
                        label,
                        json.dumps(payload or {}),
                        correlation_id,
                        now_str,
                    ),
                )

                inc_notify = None
                if correlation_id:
                    _, _, inc_notify = getattr(self, "_sync_create_or_join_incident")(
                        cur, correlation_id, event_id, reason="correlation_id"
                    )
                elif auto_association_config and auto_association_config.get("enabled", False):
                    auto_inc = getattr(self, "_sync_evaluate_auto_association")(
                        cur,
                        event_id,
                        correlation_group=auto_association_config.get("correlation_group"),
                        zone=auto_association_config.get("zone"),
                        adjacent_zones=auto_association_config.get("adjacent_zones"),
                        source_clock_offset_seconds=float(
                            auto_association_config.get("source_clock_offset_seconds", 0.0)
                        ),
                        source_time_uncertainty_seconds=float(
                            auto_association_config.get("source_time_uncertainty_seconds", 0.0)
                        ),
                        global_tolerance_seconds=float(
                            auto_association_config.get("global_tolerance_seconds", 10.0)
                        ),
                        max_incident_span_seconds=float(
                            auto_association_config.get("max_incident_span_seconds", 300.0)
                        ),
                    )
                    if auto_inc:
                        inc_notify = (auto_inc.incident_id, auto_inc.membership_revision)

                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            ev = getattr(self, "_sync_hydrate_event")(cur, cur.fetchone())
            return ev, True, (event_id, 1), inc_notify

    async def admit_record_intent(
        self,
        source_id: str,
        kind: str,
        resource_key: str,
        operation: str,
        window_seconds: float,
        occurred_at: datetime | str,
        idempotency_key: str,
        lookback_seconds: float = 0.0,
        hold: bool = False,
        recorder_config: dict[str, Any] | None = None,
        label: str | None = None,
        payload: dict[str, Any] | None = None,
        access_entities: Sequence[str] = (),
        owner_user_id: str | None = None,
        tail_window_seconds: float = 0.0,
        correlation_id: str | None = None,
        auto_association_config: dict[str, Any] | None = None,
        **extra_kwargs: Any,
    ) -> tuple[RecordedEvent, bool, str | None]:
        """Admit a recording intent in the same transaction as the source event."""
        validate_bounded_string(source_id, "source_id")
        validate_bounded_string(kind, "kind")
        validate_bounded_string(resource_key, "resource_key")
        validate_bounded_string(operation, "operation")
        validate_finite_number(window_seconds, "window_seconds", min_val=0.0, allow_zero=False)
        validate_bounded_string(idempotency_key, "idempotency_key")
        validate_finite_number(lookback_seconds, "lookback_seconds", min_val=0.0, allow_zero=True)
        validate_finite_number(
            tail_window_seconds, "tail_window_seconds", min_val=0.0, allow_zero=True
        )
        validate_payload_json(recorder_config, "recorder_config")
        validate_bounded_string(label, "label", max_len=500, allow_none=True)
        validate_payload_json(payload, "payload")
        validate_bounded_string(owner_user_id, "owner_user_id", allow_none=True)
        if correlation_id is not None:
            validate_correlation_id(correlation_id)

        res, inserted, intent_id, notify_tuple, inc_notify = await asyncio.to_thread(
            self._sync_admit_record_intent,
            source_id,
            kind,
            resource_key,
            operation,
            window_seconds,
            occurred_at,
            idempotency_key,
            lookback_seconds,
            hold,
            recorder_config,
            label,
            payload,
            access_entities,
            owner_user_id,
            tail_window_seconds,
            correlation_id,
            auto_association_config,
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        if inc_notify:
            self._notify_incident_subscribers(*inc_notify)
        return res, inserted, intent_id

    def _sync_admit_record_intent(
        self,
        source_id: str,
        kind: str,
        resource_key: str,
        operation: str,
        window_seconds: float,
        occurred_at: datetime | str,
        idempotency_key: str,
        lookback_seconds: float,
        hold: bool,
        recorder_config: dict[str, Any] | None,
        label: str | None,
        payload: dict[str, Any] | None,
        access_entities: Sequence[str],
        owner_user_id: str | None,
        tail_window_seconds: float = 0.0,
        correlation_id: str | None = None,
        auto_association_config: dict[str, Any] | None = None,
    ) -> tuple[RecordedEvent, bool, str | None, tuple[str, int] | None, tuple[str, int] | None]:
        dt = ensure_utc(occurred_at)
        dt_str = format_iso(dt) or ""
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        ref_dt = max(now_dt, dt)
        recorder_config = recorder_config or {}

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            # 1. Idempotency Check scoped strictly to (source_id, idempotency_key)
            cur.execute(
                """
                SELECT sj.event_id, e.access_entities_json, e.owner_user_id
                FROM source_journal sj
                JOIN events e ON e.event_id = sj.event_id
                WHERE sj.source_id = ? AND sj.idempotency_key = ?;
                """,
                (source_id, idempotency_key),
            )
            existing_source = cur.fetchone()
            if existing_source:
                caller_entities = tuple(sorted(set(access_entities))) if access_entities else ()
                existing_entities = (
                    tuple(sorted(set(json.loads(existing_source["access_entities_json"]))))
                    if existing_source["access_entities_json"]
                    else ()
                )
                if (
                    caller_entities != existing_entities
                    or owner_user_id != existing_source["owner_user_id"]
                ):
                    raise PermissionError("Access denied")

                cur.execute(
                    "SELECT * FROM events WHERE event_id = ?;",
                    (existing_source["event_id"],),
                )
                ev_row = cur.fetchone()
                return getattr(self, "_sync_hydrate_event")(cur, ev_row), False, None, None, None

            cur.execute("BEGIN IMMEDIATE;")
            try:
                # 2. Check active resource session
                cur.execute(
                    "SELECT * FROM resource_sessions WHERE resource_key = ?;",
                    (resource_key,),
                )
                session = cur.fetchone()

                active_event_id: str | None = None
                active_ev: dict[str, Any] | None = None
                if session and session["active_event_id"]:
                    cur.execute(
                        "SELECT * FROM events WHERE event_id = ?;",
                        (session["active_event_id"],),
                    )
                    active_ev_row = cur.fetchone()
                    if active_ev_row and active_ev_row["capture_state"] in (
                        "requested",
                        "recording",
                    ):
                        active_event_id = session["active_event_id"]
                        active_ev = active_ev_row

                intent_id = str(uuid.uuid4())
                inc_notify = None

                if active_event_id and active_ev is not None:
                    # Verify metadata compatibility before coalescing
                    active_access = (
                        tuple(sorted(set(json.loads(active_ev["access_entities_json"]))))
                        if active_ev["access_entities_json"]
                        else ()
                    )
                    caller_access = tuple(sorted(set(access_entities))) if access_entities else ()
                    if (
                        caller_access != active_access
                        or owner_user_id != active_ev["owner_user_id"]
                    ):
                        raise PermissionError("Access denied")

                    session_cfg = (
                        json.loads(session["recorder_config_json"])
                        if session.get("recorder_config_json")
                        else {}
                    )
                    if recorder_config and session_cfg and recorder_config != session_cfg:
                        raise ValueError(
                            f"Incompatible coalescing metadata on resource_key '{resource_key}': recorder_config mismatch"
                        )

                    target_event_id = active_event_id
                    current_desired = ensure_utc(active_ev["desired_capture_end"])

                    if operation == "start_or_extend":
                        new_desired = ref_dt + timedelta(seconds=window_seconds)
                        updated_desired = max(current_desired, new_desired)
                        session_hold = 1 if hold else session["hold"]
                    elif operation == "end_after":
                        new_desired = ref_dt + timedelta(seconds=window_seconds)
                        updated_desired = new_desired
                        session_hold = 0
                    else:
                        updated_desired = current_desired
                        session_hold = session["hold"]

                    desired_str = format_iso(updated_desired) or ""
                    new_revision = active_ev["revision"] + 1

                    cur.execute(
                        """
                        UPDATE events
                        SET desired_capture_end = ?, revision = ?, updated_at = ?
                        WHERE event_id = ?;
                        """,
                        (desired_str, new_revision, now_str, target_event_id),
                    )

                    cur.execute(
                        """
                        UPDATE resource_sessions
                        SET desired_capture_end = ?, last_activity_at = ?, hold = ?,
                            tail_window_seconds = ?, updated_at = ?
                        WHERE resource_key = ?;
                        """,
                        (
                            desired_str,
                            dt_str,
                            session_hold,
                            tail_window_seconds,
                            now_str,
                            resource_key,
                        ),
                    )

                    # Persist source event for coalesced intent
                    cur.execute(
                        """
                        INSERT INTO source_journal (
                            event_id, source_id, kind, occurred_at, idempotency_key,
                            label, payload_json, correlation_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            target_event_id,
                            source_id,
                            kind,
                            dt_str,
                            idempotency_key,
                            label,
                            json.dumps(payload or {}),
                            correlation_id,
                            now_str,
                        ),
                    )

                    if correlation_id:
                        _, _, inc_notify = getattr(self, "_sync_create_or_join_incident")(
                            cur, correlation_id, target_event_id, reason="correlation_id"
                        )
                    elif auto_association_config and auto_association_config.get("enabled", False):
                        auto_inc = getattr(self, "_sync_evaluate_auto_association")(
                            cur,
                            target_event_id,
                            correlation_group=auto_association_config.get("correlation_group"),
                            zone=auto_association_config.get("zone"),
                            adjacent_zones=auto_association_config.get("adjacent_zones"),
                            source_clock_offset_seconds=float(
                                auto_association_config.get("source_clock_offset_seconds", 0.0)
                            ),
                            source_time_uncertainty_seconds=float(
                                auto_association_config.get("source_time_uncertainty_seconds", 0.0)
                            ),
                            global_tolerance_seconds=float(
                                auto_association_config.get("global_tolerance_seconds", 10.0)
                            ),
                            max_incident_span_seconds=float(
                                auto_association_config.get("max_incident_span_seconds", 300.0)
                            ),
                        )
                        if auto_inc:
                            inc_notify = (auto_inc.incident_id, auto_inc.membership_revision)
                else:
                    # Create brand new event
                    target_event_id = str(uuid.uuid4())
                    start_dt = dt - timedelta(seconds=lookback_seconds)
                    start_str = format_iso(start_dt) or ""
                    desired_dt = ref_dt + timedelta(seconds=window_seconds)
                    desired_str = format_iso(desired_dt) or ""

                    access_list = sorted(set(access_entities)) if access_entities else []
                    access_json = json.dumps(access_list)

                    cur.execute(
                        """
                        INSERT INTO events (
                            event_id, source_id, kind, start_at, desired_capture_end,
                            capture_state, capture_error, label, media_revision, revision,
                            coverage_json, access_entities_json, owner_user_id, correlation_id,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'requested', NULL, ?, 1, 1, '{}', ?, ?, ?, ?, ?);
                        """,
                        (
                            target_event_id,
                            source_id,
                            kind,
                            start_str,
                            desired_str,
                            label,
                            access_json,
                            owner_user_id,
                            correlation_id,
                            now_str,
                            now_str,
                        ),
                    )

                    cur.execute(
                        """
                        INSERT INTO resource_sessions (
                            resource_key, active_event_id, last_activity_at,
                            desired_capture_end, hold, tail_window_seconds,
                            recorder_config_json, access_entities_json, owner_user_id, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(resource_key) DO UPDATE SET
                            active_event_id = excluded.active_event_id,
                            last_activity_at = excluded.last_activity_at,
                            desired_capture_end = excluded.desired_capture_end,
                            hold = excluded.hold,
                            tail_window_seconds = excluded.tail_window_seconds,
                            recorder_config_json = excluded.recorder_config_json,
                            access_entities_json = excluded.access_entities_json,
                            owner_user_id = excluded.owner_user_id,
                            updated_at = excluded.updated_at;
                        """,
                        (
                            resource_key,
                            target_event_id,
                            dt_str,
                            desired_str,
                            1 if hold else 0,
                            tail_window_seconds,
                            json.dumps(recorder_config),
                            access_json,
                            owner_user_id,
                            now_str,
                        ),
                    )

                    # Persist source event
                    cur.execute(
                        """
                        INSERT INTO source_journal (
                            event_id, source_id, kind, occurred_at, idempotency_key,
                            label, payload_json, correlation_id, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            target_event_id,
                            source_id,
                            kind,
                            dt_str,
                            idempotency_key,
                            label,
                            json.dumps(payload or {}),
                            correlation_id,
                            now_str,
                        ),
                    )

                    if correlation_id:
                        _, _, inc_notify = getattr(self, "_sync_create_or_join_incident")(
                            cur, correlation_id, target_event_id, reason="correlation_id"
                        )
                    elif auto_association_config and auto_association_config.get("enabled", False):
                        auto_inc = getattr(self, "_sync_evaluate_auto_association")(
                            cur,
                            target_event_id,
                            correlation_group=auto_association_config.get("correlation_group"),
                            zone=auto_association_config.get("zone"),
                            adjacent_zones=auto_association_config.get("adjacent_zones"),
                            source_clock_offset_seconds=float(
                                auto_association_config.get("source_clock_offset_seconds", 0.0)
                            ),
                            source_time_uncertainty_seconds=float(
                                auto_association_config.get("source_time_uncertainty_seconds", 0.0)
                            ),
                            global_tolerance_seconds=float(
                                auto_association_config.get("global_tolerance_seconds", 10.0)
                            ),
                            max_incident_span_seconds=float(
                                auto_association_config.get("max_incident_span_seconds", 300.0)
                            ),
                        )
                        if auto_inc:
                            inc_notify = (auto_inc.incident_id, auto_inc.membership_revision)

                # Persist durable recording intent in the SAME transaction
                cur.execute(
                    """
                    INSERT INTO recording_intents (
                        intent_id, event_id, resource_key, operation, window_seconds,
                        lookback_seconds, hold, recorder_config_json, status,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?);
                    """,
                    (
                        intent_id,
                        target_event_id,
                        resource_key,
                        operation,
                        window_seconds,
                        lookback_seconds,
                        1 if hold else 0,
                        json.dumps(recorder_config),
                        now_str,
                        now_str,
                    ),
                )

                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (target_event_id,))
            ev = getattr(self, "_sync_hydrate_event")(cur, cur.fetchone())
            return ev, True, intent_id, (target_event_id, ev.revision), inc_notify

    async def lease_record_intents(
        self,
        worker_id: str,
        limit: int = 10,
        lease_seconds: float = 60.0,
    ) -> list[RecordingIntent]:
        """Lease pending recording intents for worker execution."""
        return await asyncio.to_thread(
            self._sync_lease_record_intents, worker_id, limit, lease_seconds
        )

    def _sync_lease_record_intents(
        self,
        worker_id: str,
        limit: int,
        lease_seconds: float,
    ) -> list[RecordingIntent]:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        leased_until_str = format_iso(now_dt + timedelta(seconds=lease_seconds)) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    SELECT * FROM recording_intents
                    WHERE status = 'pending' OR (status = 'leased' AND leased_until < ?)
                    ORDER BY created_at ASC
                    LIMIT ?;
                    """,
                    (now_str, limit),
                )
                rows = cur.fetchall()
                if not rows:
                    cur.execute("COMMIT;")
                    return []

                intent_ids = [r["intent_id"] for r in rows]
                placeholders = ",".join("?" for _ in intent_ids)
                cur.execute(
                    f"""
                    UPDATE recording_intents
                    SET status = 'leased', worker_id = ?, leased_until = ?, updated_at = ?
                    WHERE intent_id IN ({placeholders});
                    """,
                    [worker_id, leased_until_str, now_str, *intent_ids],
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            return [
                RecordingIntent(
                    intent_id=r["intent_id"],
                    event_id=r["event_id"],
                    resource_key=r["resource_key"],
                    operation=r["operation"],
                    window_seconds=r["window_seconds"],
                    lookback_seconds=r["lookback_seconds"],
                    hold=bool(r["hold"]),
                    recorder_config=json.loads(r["recorder_config_json"])
                    if r["recorder_config_json"]
                    else {},
                    status="leased",
                    worker_id=worker_id,
                    leased_until=now_dt + timedelta(seconds=lease_seconds),
                    error=r["error"],
                    created_at=ensure_utc(r["created_at"]),
                    updated_at=now_dt,
                )
                for r in rows
            ]

    async def get_capture_lookback(self, event_id: str) -> float:
        """Read the initial request's durable lookback without per-event RAM state."""

        def read() -> float:
            with self._lock:
                assert self._conn is not None
                row = self._conn.execute(
                    """SELECT lookback_seconds FROM recording_intents
                    WHERE event_id = ? AND operation != 'stop'
                    ORDER BY created_at, intent_id LIMIT 1;""",
                    (event_id,),
                ).fetchone()
                return float(row["lookback_seconds"]) if row else 0.0

        return await asyncio.to_thread(read)

    async def compact_recording_intents(self, max_age_days: int = 7, limit: int = 500) -> int:
        """Discard terminal dispatcher bookkeeping; source-journal deduplication remains."""

        def compact() -> int:
            cutoff = format_iso(utc_now() - timedelta(days=max_age_days))
            with self._lock:
                assert self._conn is not None
                cur = self._conn.execute(
                    """DELETE FROM recording_intents WHERE intent_id IN (
                    SELECT intent_id FROM recording_intents
                    WHERE status IN ('completed', 'failed', 'cancelled') AND updated_at < ?
                    AND NOT EXISTS (SELECT 1 FROM events e
                        WHERE e.event_id = recording_intents.event_id
                        AND e.capture_state IN ('requested', 'recording'))
                    ORDER BY updated_at LIMIT ?
                );""",
                    (cutoff, limit),
                )
                return cur.rowcount

        return await asyncio.to_thread(compact)

    async def complete_record_intent(
        self,
        intent_id: str,
        worker_id: str,
        error: str | None = None,
    ) -> bool:
        """Mark a recording intent as completed or failed."""
        return await asyncio.to_thread(
            self._sync_complete_record_intent, intent_id, worker_id, error
        )

    def _sync_complete_record_intent(
        self,
        intent_id: str,
        worker_id: str,
        error: str | None,
    ) -> bool:
        now_str = format_iso(utc_now()) or ""
        new_status = "failed" if error else "completed"

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    UPDATE recording_intents
                    SET status = ?, error = ?, updated_at = ?
                    WHERE intent_id = ? AND (worker_id = ? OR worker_id IS NULL);
                    """,
                    (new_status, error, now_str, intent_id, worker_id),
                )
                affected = cur.rowcount > 0
                cur.execute("COMMIT;")
                return affected
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def get_resource_session(self, resource_key: str) -> ResourceSession | None:
        """Get the current session state for a physical resource key."""
        return await asyncio.to_thread(self._sync_get_resource_session, resource_key)

    def _hydrate_resource_session(self, row: Any) -> ResourceSession:
        return ResourceSession(
            resource_key=row["resource_key"],
            active_event_id=row["active_event_id"],
            last_activity_at=ensure_utc(row["last_activity_at"]),
            desired_capture_end=ensure_utc(row["desired_capture_end"]),
            hold=bool(row["hold"]),
            tail_window_seconds=row["tail_window_seconds"],
            recorder_config=json.loads(row["recorder_config_json"])
            if row.get("recorder_config_json")
            else {},
            access_entities=tuple(json.loads(row["access_entities_json"]))
            if row.get("access_entities_json")
            else (),
            owner_user_id=row.get("owner_user_id"),
            updated_at=ensure_utc(row["updated_at"]),
        )

    def _sync_get_resource_session(self, resource_key: str) -> ResourceSession | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM resource_sessions WHERE resource_key = ?;", (resource_key,))
            row = cur.fetchone()
            if not row:
                return None
            return self._hydrate_resource_session(row)

    async def finish_capture_if_current(
        self,
        resource_key: str,
        event_id: str,
        valid_clips_count: int,
        error: str | None,
        reason: str,
        now: datetime,
        *,
        allow_extension: bool = False,
        final_state: str | None = None,
    ) -> CaptureFinishResult:
        """Atomically finalize an event only while it still owns its resource.

        The ownership check, current-window/hold check, and terminal transition
        share one ``BEGIN IMMEDIATE`` transaction.  If a newer admission already
        extended the window, ``extended`` is returned without touching the
        event; the caller can resume in place.  If another event owns the
        resource, ``replaced`` is returned and neither event/session is mutated.
        """

        def finish() -> tuple[CaptureFinishResult, tuple[str, int] | None]:
            return self._sync_finish_capture_if_current(
                resource_key,
                event_id,
                valid_clips_count,
                error,
                reason,
                now,
                allow_extension,
                final_state,
            )

        result, notify_tuple = await asyncio.to_thread(finish)
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        return result

    def _sync_finish_capture_if_current(
        self,
        resource_key: str,
        event_id: str,
        valid_clips_count: int,
        error: str | None,
        reason: str,
        now: datetime,
        allow_extension: bool,
        final_state: str | None,
    ) -> tuple[CaptureFinishResult, tuple[str, int] | None]:
        if valid_clips_count < 0:
            raise ValueError("valid_clips_count must be non-negative")
        if final_state is not None:
            if final_state not in _TERMINAL_CAPTURE_STATES:
                raise ValueError(
                    f"final_state override must be terminal, got {final_state!r}"
                )
            if valid_clips_count == 0 and final_state in ("ready", "partial"):
                raise ValueError(
                    f"zero valid clips cannot be finalized as {final_state!r}"
                )

        decision_now = ensure_utc(now)
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    "SELECT * FROM resource_sessions WHERE resource_key = ?;",
                    (resource_key,),
                )
                session_row = cur.fetchone()
                session = (
                    self._hydrate_resource_session(session_row) if session_row else None
                )
                owns_session = (
                    session is not None and session.active_event_id == event_id
                )
                if (
                    session is not None
                    and session.active_event_id is not None
                    and not owns_session
                ):
                    cur.execute("COMMIT;")
                    return CaptureFinishResult(status="replaced", session=session), None

                cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
                event_row = cur.fetchone()
                if event_row is None:
                    cur.execute("COMMIT;")
                    return CaptureFinishResult(status="replaced", session=session), None

                event = getattr(self, "_sync_hydrate_event")(cur, event_row)
                if event.capture_state in _TERMINAL_CAPTURE_STATES:
                    cur.execute("COMMIT;")
                    return (
                        CaptureFinishResult(
                            status="terminal", event=event, session=session
                        ),
                        None,
                    )

                if not owns_session:
                    # The session was cleared without a replacement, or belongs
                    # to a newer event; do not mutate this stale event.
                    cur.execute("COMMIT;")
                    return CaptureFinishResult(status="replaced", session=session), None

                if allow_extension and (
                    session.hold or session.desired_capture_end > decision_now
                ):
                    _LOGGER.debug(
                        "Capture for %s still owns an open window during %s; "
                        "resuming instead of finalizing event %s",
                        resource_key,
                        reason,
                        event_id,
                    )
                    cur.execute("COMMIT;")
                    return (
                        CaptureFinishResult(
                            status="extended", event=event, session=session
                        ),
                        None,
                    )

                if final_state is not None:
                    state = final_state
                    if (
                        valid_clips_count == 0
                        and final_state == "failed"
                        and error is None
                    ):
                        error_code = "ZERO_CLIPS_RECORDED"
                    else:
                        error_code = error
                elif valid_clips_count == 0:
                    state = "failed"
                    error_code = error or "ZERO_CLIPS_RECORDED"
                elif error:
                    state = "partial"
                    error_code = error
                else:
                    state = "ready"
                    error_code = None

                # The last attach may predate an extension or a failed final
                # chunk. Recompute against the window this event actually asked
                # for now, so stale "verified" coverage cannot survive a
                # finish that reports an error.
                if valid_clips_count > 0:
                    cur.execute(
                        "SELECT * FROM media WHERE event_id = ?;",
                        (event_id,),
                    )
                    media_rows = cur.fetchall()
                    if media_rows:
                        finish_coverage = compute_coverage(
                            media_rows,
                            requested_start=event.start_at,
                            requested_end=event.desired_capture_end,
                        )
                        if error_code is not None:
                            finish_coverage["is_uncertain"] = True
                            finish_coverage["verification"] = "partial"
                            finish_coverage["reason"] = (
                                finish_coverage.get("reason")
                                or sanitize_capture_error(error_code)
                                or "capture_error"
                            )
                        cur.execute(
                            "UPDATE events SET coverage_json = ? WHERE event_id = ?;",
                            (json.dumps(finish_coverage), event_id),
                        )

                updated, notify_tuple = self._sync_apply_capture_state(
                    cur, event_id, state, error_code, now_str
                )
                cur.execute("COMMIT;")
                return (
                    CaptureFinishResult(status="finished", event=updated),
                    notify_tuple,
                )
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def find_abandoned_capture_candidates(
        self,
        *,
        older_than: datetime,
        limit: int = 8,
        after_updated_at: datetime | None = None,
        after_event_id: str | None = None,
    ) -> list[AbandonedCaptureCandidate]:
        """Return a bounded, keyset-rotated page of ownerless inflight events.

        The query starts from ``events.capture_state`` (covered by
        ``idx_events_capture_state``) rather than from ``resource_sessions``:
        an abandoned event may already have had its session binding cleared or
        replaced.  Physical resource identity is resolved from the latest
        durable start/extend intent (covered by ``idx_intents_event``), never
        guessed from ``source_id``.

        ``after_updated_at``/``after_event_id`` form a stable cursor so live or
        unrepairable older rows cannot occupy the first page forever.
        """
        bounded_limit = max(1, min(64, int(limit)))
        return await asyncio.to_thread(
            self._sync_find_abandoned_capture_candidates,
            older_than,
            bounded_limit,
            after_updated_at,
            after_event_id,
        )

    def _sync_find_abandoned_capture_candidates(
        self,
        older_than: datetime,
        limit: int,
        after_updated_at: datetime | None,
        after_event_id: str | None,
    ) -> list[AbandonedCaptureCandidate]:
        threshold = format_iso(ensure_utc(older_than)) or ""
        cursor_clause = ""
        params: list[Any] = [threshold]
        if after_updated_at is not None and after_event_id:
            after_str = format_iso(ensure_utc(after_updated_at)) or ""
            cursor_clause = (
                "AND (e.updated_at > ? OR "
                "(e.updated_at = ? AND e.event_id > ?))"
            )
            params.extend([after_str, after_str, after_event_id])
        params.append(limit)

        candidates: list[AbandonedCaptureCandidate] = []
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                f"""
                SELECT e.event_id, e.capture_state, e.updated_at
                FROM events e
                WHERE e.capture_state IN ('requested', 'recording')
                  AND e.updated_at < ?
                  AND NOT EXISTS (
                      SELECT 1 FROM recording_intents ri
                      WHERE ri.event_id = e.event_id
                        AND ri.status IN ('pending', 'leased')
                  )
                  {cursor_clause}
                ORDER BY e.updated_at ASC, e.event_id ASC
                LIMIT ?;
                """,
                tuple(params),
            )
            rows = cur.fetchall()
            for row in rows:
                cur.execute(
                    """
                    SELECT resource_key, recorder_config_json
                    FROM recording_intents
                    WHERE event_id = ? AND operation = 'start_or_extend'
                    ORDER BY created_at DESC
                    LIMIT 1;
                    """,
                    (row["event_id"],),
                )
                identity = cur.fetchone()
                if identity is None:
                    # Fallback for a still-bound session when older intent
                    # records were compacted.  A cleared session plus missing
                    # intent has no safe physical identity.
                    cur.execute(
                        """
                        SELECT resource_key, recorder_config_json
                        FROM resource_sessions
                        WHERE active_event_id = ?
                        LIMIT 1;
                        """,
                        (row["event_id"],),
                    )
                    identity = cur.fetchone()
                if identity is None:
                    candidates.append(
                        AbandonedCaptureCandidate(
                            event_id=row["event_id"],
                            resource_key=None,
                            capture_state=row["capture_state"],
                            updated_at=ensure_utc(row["updated_at"]),
                            missing_identity=True,
                        )
                    )
                    continue
                try:
                    config = (
                        json.loads(identity["recorder_config_json"])
                        if identity.get("recorder_config_json")
                        else {}
                    )
                except (TypeError, ValueError):
                    config = {}
                config = config if isinstance(config, dict) else {}
                profile = config.get("_event_recording_profile") or {}
                profile = profile if isinstance(profile, dict) else {}
                ownership_group = profile.get("ownership_group") or config.get(
                    "ownership_group"
                )
                candidates.append(
                    AbandonedCaptureCandidate(
                        event_id=row["event_id"],
                        resource_key=identity["resource_key"],
                        ownership_group=ownership_group,
                        capture_state=row["capture_state"],
                        updated_at=ensure_utc(row["updated_at"]),
                    )
                )
        return candidates

    async def settle_abandoned_capture(
        self, *, event_id: str, resource_key: str, error_code: str, now: datetime,
    ) -> CaptureFinishResult:
        """Settle a physically stopped owner, counting durable media atomically.

        The caller holds the runtime dispatch fence and verifies writer settlement.
        This transaction rechecks intents, identity and state, and clears only
        sessions still bound to this exact event, never a successor's session.
        """
        result, notification = await asyncio.to_thread(
            self._sync_settle_abandoned_capture,
            event_id, resource_key, error_code, ensure_utc(now),
        )
        if notification:
            self._notify_subscribers(*notification)
        return result

    def _sync_settle_abandoned_capture(
        self, event_id: str, resource_key: str, error_code: str, now: datetime,
    ) -> tuple[CaptureFinishResult, tuple[str, int] | None]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
                row = cur.fetchone()
                if row is None:
                    cur.execute("COMMIT;")
                    return CaptureFinishResult(status="replaced"), None
                event = getattr(self, "_sync_hydrate_event")(cur, row)
                if event.capture_state in _TERMINAL_CAPTURE_STATES:
                    cur.execute("COMMIT;")
                    return CaptureFinishResult(status="terminal", event=event), None
                cur.execute(
                    "SELECT 1 FROM recording_intents WHERE event_id = ? "
                    "AND status IN ('pending', 'leased') LIMIT 1;", (event_id,),
                )
                if cur.fetchone() is not None:
                    cur.execute("COMMIT;")
                    return CaptureFinishResult(status="extended", event=event), None
                cur.execute(
                    "SELECT 1 FROM resource_sessions WHERE resource_key = ? "
                    "AND active_event_id = ? UNION ALL "
                    "SELECT 1 FROM recording_intents WHERE resource_key = ? "
                    "AND event_id = ? AND operation = 'start_or_extend' LIMIT 1;",
                    (resource_key, event_id, resource_key, event_id),
                )
                if cur.fetchone() is None:
                    cur.execute("COMMIT;")
                    return CaptureFinishResult(status="replaced"), None
                cur.execute(
                    "SELECT COUNT(*) AS n FROM media WHERE event_id = ? AND available = 1;",
                    (event_id,),
                )
                has_media = cur.fetchone()["n"] > 0
                if has_media:
                    cur.execute(
                        "SELECT * FROM media WHERE event_id = ?;",
                        (event_id,),
                    )
                    coverage = compute_coverage(
                        cur.fetchall(),
                        requested_start=event.start_at,
                        requested_end=event.desired_capture_end,
                    )
                    coverage["is_uncertain"] = True
                    coverage["verification"] = "partial"
                    coverage["reason"] = coverage.get("reason") or "capture_abandoned"
                    cur.execute(
                        "UPDATE events SET coverage_json = ? WHERE event_id = ?;",
                        (json.dumps(coverage), event_id),
                    )
                updated, notification = self._sync_apply_capture_state(
                    cur, event_id, "partial" if has_media else "interrupted",
                    error_code, format_iso(now) or "",
                )
                cur.execute("COMMIT;")
                return CaptureFinishResult(status="finished", event=updated), notification
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def clear_resource_session(self, resource_key: str) -> None:
        """Clear active event binding for a resource session off-loop."""
        await asyncio.to_thread(self._sync_clear_resource_session, resource_key)

    def _sync_clear_resource_session(self, resource_key: str) -> None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                "UPDATE resource_sessions SET active_event_id = NULL WHERE resource_key = ?;",
                (resource_key,),
            )
            self._conn.commit()

    async def update_capture_state(
        self,
        event_id: str,
        state: str,
        error_code: str | None = None,
    ) -> RecordedEvent:
        """Update event capture state and bump revisions."""
        ev, notify_tuple = await asyncio.to_thread(
            self._sync_update_capture_state, event_id, state, error_code
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        return ev

    def _sync_apply_capture_state(
        self,
        cur: Any,
        event_id: str,
        state: str,
        error_code: str | None,
        now_str: str,
    ) -> tuple[RecordedEvent, tuple[str, int] | None]:
        sanitized_error = sanitize_capture_error(error_code)

        cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
        row = cur.fetchone()
        if not row:
            raise KeyError(f"Event {event_id} not found")

        current_state = row["capture_state"]
        current_error = row.get("capture_error")

        existing_coverage = json.loads(row["coverage_json"]) if row["coverage_json"] else {}
        if state == "ready":
            verification = existing_coverage.get("verification")
            if existing_coverage.get("is_uncertain") is True:
                state = "partial"
            elif verification is not None and verification not in (
                "verified",
                "instant",
            ):
                # Recompute is allowed to promote partial -> ready, but only
                # when the current evidence explicitly verifies the window.
                state = "partial"

        state_changed = state != current_state
        error_changed = sanitized_error != current_error

        if not state_changed and not error_changed:
            return getattr(self, "_sync_hydrate_event")(cur, row), None

        was_final = current_state in ("ready", "partial")
        is_final = state in ("ready", "partial")
        is_first_finalization = is_final and not was_final

        new_media_rev = (
            row["media_revision"] + 1 if is_first_finalization else row["media_revision"]
        )
        new_rev = row["revision"] + 1

        end_at_str = row["end_at"] or (
            now_str if state in ("ready", "partial", "failed", "interrupted", "expired") else None
        )

        cur.execute(
            """
            UPDATE events
            SET capture_state = ?, capture_error = ?, end_at = ?, media_revision = ?, revision = ?, updated_at = ?
            WHERE event_id = ?;
            """,
            (
                state,
                sanitized_error,
                end_at_str,
                new_media_rev,
                new_rev,
                now_str,
                event_id,
            ),
        )

        if state in ("ready", "partial", "failed", "interrupted", "expired"):
            cur.execute(
                "UPDATE resource_sessions SET active_event_id = NULL WHERE active_event_id = ?;",
                (event_id,),
            )

        cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
        ev = getattr(self, "_sync_hydrate_event")(cur, cur.fetchone())
        return ev, (event_id, ev.revision)

    def _sync_update_capture_state(
        self,
        event_id: str,
        state: str,
        error_code: str | None,
    ) -> tuple[RecordedEvent, tuple[str, int] | None]:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                ev, notify_tuple = self._sync_apply_capture_state(
                    cur, event_id, state, error_code, now_str
                )
                cur.execute("COMMIT;")
                return ev, notify_tuple
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def finalize_event(self, event_id: str) -> RecordedEvent:
        """Mark event capture ready idempotently."""
        return await self.update_capture_state(event_id, "ready")
