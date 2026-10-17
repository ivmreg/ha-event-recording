"""Incident tracking, CAS annotations, input token hashing, and auto-association for JournalStore.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
Preserves CAS concurrency guards, dynamic span recomputation, and transitive-merge prevention.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import uuid
from collections.abc import Sequence
from datetime import timedelta
from typing import Any

from ..models import (
    Incident,
    IncidentInputs,
    RecordedEvent,
    canonical_annotation_payload,
    canonical_json_dumps,
    ensure_utc,
    format_iso,
    utc_now,
    validate_bounded_string,
    validate_finite_number,
    validate_payload_json,
    validate_safe_id,
)
from .common import JournalStoreBase

_LOGGER = logging.getLogger(__name__)


class IncidentsMixin(JournalStoreBase):
    """Encapsulates incident lifecycle, CAS annotations, input token caching, and auto-association."""

    # -------------------------------------------------------------------------
    # Generic CAS Annotations on Events
    # -------------------------------------------------------------------------

    async def reserve_annotation(
        self,
        event_id: str,
        namespace: str,
        expected_media_revision: int,
        worker_id: str,
        ttl_seconds: float = 180.0,
    ) -> int | None:
        """Reserve an annotation slot for a namespace with generation tracking and status=pending."""
        validate_safe_id(event_id, "event_id")
        validate_bounded_string(namespace, "namespace")
        validate_bounded_string(worker_id, "worker_id")
        validate_finite_number(ttl_seconds, "ttl_seconds", min_val=0.0, allow_zero=False)

        gen, notify_tuple = await asyncio.to_thread(
            self._sync_reserve_annotation,
            event_id,
            namespace,
            expected_media_revision,
            worker_id,
            ttl_seconds,
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        return gen

    def _sync_reserve_annotation(
        self,
        event_id: str,
        namespace: str,
        expected_media_revision: int,
        worker_id: str,
        ttl_seconds: float,
    ) -> tuple[int | None, tuple[str, int] | None]:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        reserved_until = format_iso(now_dt + timedelta(seconds=ttl_seconds)) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT media_revision, revision FROM events WHERE event_id = ?;", (event_id,))
            ev_row = cur.fetchone()
            if not ev_row or ev_row["media_revision"] != expected_media_revision:
                return None, None

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    "SELECT generation, status, reserved_until FROM annotations WHERE event_id = ? AND namespace = ?;",
                    (event_id, namespace),
                )
                ann = cur.fetchone()

                new_generation = 1
                if ann:
                    new_generation = ann["generation"] + 1
                    cur.execute(
                        """
                        UPDATE annotations
                        SET generation = ?, expected_media_revision = ?, status = 'pending',
                            payload_json = '{}',
                            reserved_by = ?, reserved_until = ?, updated_at = ?
                        WHERE event_id = ? AND namespace = ?;
                        """,
                        (
                            new_generation,
                            expected_media_revision,
                            worker_id,
                            reserved_until,
                            now_str,
                            event_id,
                            namespace,
                        ),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO annotations (
                            event_id, namespace, generation, expected_media_revision,
                            payload_json, status, reserved_by, reserved_until,
                            created_at, updated_at
                        ) VALUES (?, ?, 1, ?, '{}', 'pending', ?, ?, ?, ?);
                        """,
                        (
                            event_id,
                            namespace,
                            expected_media_revision,
                            worker_id,
                            reserved_until,
                            now_str,
                            now_str,
                        ),
                    )
                    new_generation = 1

                new_rev = ev_row["revision"] + 1
                cur.execute(
                    "UPDATE events SET revision = ?, updated_at = ? WHERE event_id = ?;",
                    (new_rev, now_str, event_id),
                )
                cur.execute("COMMIT;")
                return new_generation, (event_id, new_rev)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def set_annotation_cas(
        self,
        event_id: str,
        expected_media_revision: int,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int | None = None,
        status: str | None = None,
    ) -> bool:
        """Atomically commit an annotation payload guarded by media_revision, generation, and lease expiry.

        Increments global revision on commit, but NOT media_revision.
        """
        validate_safe_id(event_id, "event_id")
        validate_bounded_string(namespace, "namespace")
        validate_payload_json(payload, "payload")
        if expected_generation is None:
            return False
        if isinstance(expected_generation, bool) or not isinstance(expected_generation, int) or expected_generation < 1:
            return False

        res, notify_tuple = await asyncio.to_thread(
            self._sync_set_annotation_cas,
            event_id,
            expected_media_revision,
            namespace,
            payload,
            expected_generation,
            status,
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        return res

    def _sync_set_annotation_cas(
        self,
        event_id: str,
        expected_media_revision: int,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int | None,
        status: str | None = None,
    ) -> tuple[bool, tuple[str, int] | None]:
        if expected_generation is None:
            return False, None

        raw_status = status or payload.get("status", "complete")
        clean_status = str(raw_status).strip().lower()
        terminal_statuses = {"complete", "failed", "skipped"}
        nonterminal_statuses = {"pending", "processing"}
        valid_statuses = terminal_statuses | nonterminal_statuses
        if clean_status not in valid_statuses:
            return False, None

        now_str = format_iso(utc_now()) or ""

        # Clean payload internal keys
        clean_payload = {
            k: v
            for k, v in payload.items()
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
        clean_payload["status"] = clean_status
        payload_str = json.dumps(clean_payload)

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                "SELECT media_revision, revision FROM events WHERE event_id = ?;",
                (event_id,),
            )
            ev = cur.fetchone()
            if not ev or ev["media_revision"] != expected_media_revision:
                return False, None

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    SELECT generation, status, reserved_by, reserved_until, expected_media_revision
                    FROM annotations
                    WHERE event_id = ? AND namespace = ?;
                    """,
                    (event_id, namespace),
                )
                existing = cur.fetchone()

                # Require existing reserved row (no speculative generation 1 insert to missing row)
                if not existing:
                    cur.execute("ROLLBACK;")
                    return False, None

                # Late old generation or prior media revision rejected
                if existing["generation"] != expected_generation:
                    cur.execute("ROLLBACK;")
                    return False, None
                if existing["expected_media_revision"] != expected_media_revision:
                    cur.execute("ROLLBACK;")
                    return False, None

                # Terminal rows cannot be rewritten
                if existing["status"] in terminal_statuses:
                    cur.execute("ROLLBACK;")
                    return False, None

                # Check lease expiry: expired reservation cannot write
                if existing.get("reserved_until") and existing["reserved_until"] < now_str:
                    cur.execute("ROLLBACK;")
                    return False, None

                if clean_status in terminal_statuses:
                    new_reserved_by = None
                    new_reserved_until = None
                else:
                    new_reserved_by = existing["reserved_by"]
                    new_reserved_until = existing["reserved_until"]

                cur.execute(
                    """
                    UPDATE annotations
                    SET expected_media_revision = ?, payload_json = ?, status = ?,
                        reserved_by = ?, reserved_until = ?, updated_at = ?
                    WHERE event_id = ? AND namespace = ?;
                    """,
                    (
                        expected_media_revision,
                        payload_str,
                        clean_status,
                        new_reserved_by,
                        new_reserved_until,
                        now_str,
                        event_id,
                        namespace,
                    ),
                )

                new_rev = ev["revision"] + 1
                cur.execute(
                    "UPDATE events SET revision = ?, updated_at = ? WHERE event_id = ?;",
                    (new_rev, now_str, event_id),
                )
                cur.execute("COMMIT;")
                return True, (event_id, new_rev)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    # -------------------------------------------------------------------------
    # Incident Token & Span Computation
    # -------------------------------------------------------------------------

    def _sync_compute_incident_input_token(
        self, cur: sqlite3.Cursor, incident_id: str, input_namespace: str
    ) -> str:
        """Compute canonical hash covering membership revision, member media revisions, and input namespace annotation state.

        Unrelated annotations do NOT invalidate this token.
        """
        cur.execute(
            "SELECT membership_revision FROM incidents WHERE incident_id = ?;", (incident_id,)
        )
        inc_row = cur.fetchone()
        membership_rev = inc_row["membership_revision"] if inc_row else 1

        cur.execute(
            """
            SELECT e.event_id, e.media_revision
            FROM incident_memberships m
            JOIN events e ON m.event_id = e.event_id
            WHERE m.incident_id = ?
            ORDER BY e.event_id ASC;
            """,
            (incident_id,),
        )
        member_rows = cur.fetchall()

        parts: list[str] = [f"mrev:{membership_rev}", f"ns:{input_namespace}"]
        for m in member_rows:
            eid = m["event_id"]
            m_rev = m["media_revision"]
            cur.execute(
                """
                SELECT generation, status, payload_json
                FROM annotations
                WHERE event_id = ? AND namespace = ?;
                """,
                (eid, input_namespace),
            )
            ann_row = cur.fetchone()
            if ann_row:
                gen = ann_row["generation"]
                st = ann_row["status"]
                p_raw = ann_row["payload_json"]
                try:
                    p_obj = json.loads(p_raw) if p_raw else {}
                except Exception:
                    p_obj = {}
                p_dict = canonical_annotation_payload(p_obj) if isinstance(p_obj, dict) else {}
                p_str = canonical_json_dumps(p_dict)
                p_hash = hashlib.sha256(p_str.encode("utf-8")).hexdigest()[:16]
                ann_repr = f"g={gen}:s={st}:p={p_hash}"
            else:
                ann_repr = "none"
            parts.append(f"{eid}:{m_rev}:{ann_repr}")
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()

    def _sync_recompute_incident_span(self, cur: sqlite3.Cursor, incident_id: str) -> None:
        """Recompute dynamic span_start and span_end without changing immutable anchor_at."""
        cur.execute(
            """
            SELECT MIN(e.start_at) as min_start,
                   MAX(COALESCE(e.end_at, e.desired_capture_end, e.start_at)) as max_end
            FROM incident_memberships m
            JOIN events e ON m.event_id = e.event_id
            WHERE m.incident_id = ?;
            """,
            (incident_id,),
        )
        row = cur.fetchone()
        if row and row["min_start"] and row["max_end"]:
            now_str = format_iso(utc_now()) or ""
            cur.execute(
                """
                UPDATE incidents
                SET span_start = ?, span_end = ?, updated_at = ?
                WHERE incident_id = ?;
                """,
                (row["min_start"], row["max_end"], now_str, incident_id),
            )

    # -------------------------------------------------------------------------
    # Hydration and Lookups
    # -------------------------------------------------------------------------

    def _sync_hydrate_incident(self, cur: sqlite3.Cursor, incident_id: str) -> Incident | None:
        cur.execute("SELECT * FROM incidents WHERE incident_id = ?;", (incident_id,))
        inc_row = cur.fetchone()
        if not inc_row:
            return None

        # Fetch incident annotations and filter out obsolete aggregates whose input_token disagrees
        cur.execute(
            """
            SELECT namespace, generation, input_token, input_namespace, profile_digest, payload_json, status, updated_at
            FROM incident_annotations
            WHERE incident_id = ?;
            """,
            (incident_id,),
        )
        ann_rows = cur.fetchall()
        valid_annotations: dict[str, Any] = {}
        for ann in ann_rows:
            input_ns = ann["input_namespace"]
            fresh_token = self._sync_compute_incident_input_token(cur, incident_id, input_ns)
            if ann["input_token"] != fresh_token:
                # Omit obsolete aggregate when fresh inputs disagree!
                continue
            if ann["status"] != "ready":
                # Omit incomplete/in-flight reservation payload!
                continue
            payload = json.loads(ann["payload_json"]) if ann["payload_json"] else {}
            valid_annotations[ann["namespace"]] = {
                "generation": ann["generation"],
                "status": ann["status"],
                "input_token": ann["input_token"],
                "profile_digest": ann["profile_digest"],
                "payload": payload,
                "updated_at": ann["updated_at"],
            }

        return Incident(
            incident_id=inc_row["incident_id"],
            anchor_at=ensure_utc(inc_row["anchor_at"]),
            span_start=ensure_utc(inc_row["span_start"]),
            span_end=ensure_utc(inc_row["span_end"]),
            membership_revision=inc_row["membership_revision"],
            reason=inc_row.get("reason"),
            annotations=valid_annotations,
            created_at=ensure_utc(inc_row["created_at"]),
            updated_at=ensure_utc(inc_row["updated_at"]),
        )

    async def get_incident(self, incident_id: str) -> Incident | None:
        """Fetch incident by ID off event loop, omitting obsolete annotations."""
        validate_safe_id(incident_id, "incident_id")
        return await asyncio.to_thread(self._sync_get_incident, incident_id)

    def _sync_get_incident(self, incident_id: str) -> Incident | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            return self._sync_hydrate_incident(cur, incident_id)

    async def get_incident_inputs(
        self, incident_id: str, input_namespace: str
    ) -> IncidentInputs | None:
        """Return consistent snapshot of incident, member events, and input freshness token."""
        validate_safe_id(incident_id, "incident_id")
        validate_bounded_string(input_namespace, "input_namespace")
        return await asyncio.to_thread(self._sync_get_incident_inputs, incident_id, input_namespace)

    def _sync_get_incident_inputs(
        self, incident_id: str, input_namespace: str
    ) -> IncidentInputs | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            incident = self._sync_hydrate_incident(cur, incident_id)
            if not incident:
                return None

            cur.execute(
                """
                SELECT e.*
                FROM incident_memberships m
                JOIN events e ON m.event_id = e.event_id
                WHERE m.incident_id = ?
                ORDER BY e.start_at ASC, e.event_id ASC;
                """,
                (incident_id,),
            )
            member_rows = cur.fetchall()
            events = tuple(self._sync_hydrate_event(cur, r) for r in member_rows)
            token = self._sync_compute_incident_input_token(cur, incident_id, input_namespace)
            return IncidentInputs(incident=incident, events=events, input_token=token)

    async def list_event_incidents(self, event_id: str) -> list[Incident]:
        """List all incidents that contain this event as a member."""
        validate_safe_id(event_id, "event_id")
        return await asyncio.to_thread(self._sync_list_event_incidents, event_id)

    def _sync_list_event_incidents(self, event_id: str) -> list[Incident]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT incident_id
                FROM incident_memberships
                WHERE event_id = ?
                ORDER BY added_at ASC;
                """,
                (event_id,),
            )
            rows = cur.fetchall()
            incidents: list[Incident] = []
            for r in rows:
                inc = self._sync_hydrate_incident(cur, r["incident_id"])
                if inc:
                    incidents.append(inc)
            return incidents

    async def get_incident_events(self, incident_id: str) -> list[RecordedEvent]:
        """Trusted internal API returning all member RecordedEvents for an incident."""
        validate_safe_id(incident_id, "incident_id")
        return await asyncio.to_thread(self._sync_get_incident_events, incident_id)

    def _sync_get_incident_events(self, incident_id: str) -> list[RecordedEvent]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT e.*
                FROM incident_memberships m
                JOIN events e ON m.event_id = e.event_id
                WHERE m.incident_id = ?
                ORDER BY e.start_at ASC, e.event_id ASC;
                """,
                (incident_id,),
            )
            return [self._sync_hydrate_event(cur, r) for r in cur.fetchall()]

    # -------------------------------------------------------------------------
    # Incident Membership & Mutations
    # -------------------------------------------------------------------------

    async def create_or_join_incident(
        self,
        incident_id: str | None,
        event_id: str,
        reason: str | None = None,
    ) -> Incident:
        """Add event to an incident, creating it with immutable anchor_at if nonexistent."""
        if incident_id is not None:
            validate_safe_id(incident_id, "incident_id")
        validate_safe_id(event_id, "event_id")
        validate_bounded_string(reason, "reason", max_len=255, allow_none=True)

        inc, _inserted, notify_tuple = await asyncio.to_thread(
            self._sync_create_or_join_incident_wrapper, incident_id, event_id, reason
        )
        if notify_tuple:
            self._notify_incident_subscribers(*notify_tuple)
        return inc

    def _sync_create_or_join_incident_wrapper(
        self, incident_id: str | None, event_id: str, reason: str | None
    ) -> tuple[Incident, bool, tuple[str, int] | None]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                eff_id = incident_id or f"inc_{uuid.uuid4().hex[:12]}"
                inc, inserted, notify_tuple = self._sync_create_or_join_incident(
                    cur, eff_id, event_id, reason
                )
                cur.execute("COMMIT;")
                return inc, inserted, notify_tuple
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    def _sync_create_or_join_incident(
        self,
        cur: sqlite3.Cursor,
        incident_id: str,
        event_id: str,
        reason: str | None = None,
    ) -> tuple[Incident, bool, tuple[str, int] | None]:
        now_str = format_iso(utc_now()) or ""
        cur.execute("SELECT * FROM incidents WHERE incident_id = ?;", (incident_id,))
        inc_row = cur.fetchone()

        if not inc_row:
            cur.execute(
                "SELECT start_at, end_at, desired_capture_end FROM events WHERE event_id = ?;",
                (event_id,),
            )
            ev_row = cur.fetchone()
            if not ev_row:
                raise ValueError(f"Event '{event_id}' not found")
            anchor_at = ev_row["start_at"]
            span_start = ev_row["start_at"]
            span_end = ev_row["end_at"] or ev_row["desired_capture_end"] or ev_row["start_at"]

            cur.execute(
                """
                INSERT INTO incidents (
                    incident_id, anchor_at, span_start, span_end, membership_revision, reason, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 1, ?, ?, ?);
                """,
                (incident_id, anchor_at, span_start, span_end, reason, now_str, now_str),
            )
            cur.execute(
                """
                INSERT INTO incident_memberships (
                    incident_id, event_id, added_at, reason
                ) VALUES (?, ?, ?, ?);
                """,
                (incident_id, event_id, now_str, reason),
            )
            hydrated = self._sync_hydrate_incident(cur, incident_id)
            assert hydrated is not None
            return hydrated, True, (incident_id, 1)

        # Check existing membership
        cur.execute(
            "SELECT 1 FROM incident_memberships WHERE incident_id = ? AND event_id = ?;",
            (incident_id, event_id),
        )
        if cur.fetchone():
            # Already member: idempotent, no revision bump
            hydrated = self._sync_hydrate_incident(cur, incident_id)
            assert hydrated is not None
            return hydrated, False, None

        # Insert new membership and bump membership_revision
        cur.execute(
            """
            INSERT INTO incident_memberships (
                incident_id, event_id, added_at, reason
            ) VALUES (?, ?, ?, ?);
            """,
            (incident_id, event_id, now_str, reason),
        )
        new_rev = inc_row["membership_revision"] + 1
        cur.execute(
            """
            UPDATE incidents
            SET membership_revision = ?, updated_at = ?
            WHERE incident_id = ?;
            """,
            (new_rev, now_str, incident_id),
        )
        self._sync_recompute_incident_span(cur, incident_id)
        hydrated = self._sync_hydrate_incident(cur, incident_id)
        assert hydrated is not None
        return hydrated, True, (incident_id, new_rev)

    async def remove_event_from_incident(self, incident_id: str, event_id: str) -> bool:
        """Remove an event from an incident, bumping membership_revision and recomputing span."""
        validate_safe_id(incident_id, "incident_id")
        validate_safe_id(event_id, "event_id")

        removed, notify_tuple = await asyncio.to_thread(
            self._sync_remove_event_from_incident, incident_id, event_id
        )
        if notify_tuple:
            self._notify_incident_subscribers(*notify_tuple)
        return removed

    def _sync_remove_event_from_incident(
        self, incident_id: str, event_id: str
    ) -> tuple[bool, tuple[str, int] | None]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                "SELECT 1 FROM incident_memberships WHERE incident_id = ? AND event_id = ?;",
                (incident_id, event_id),
            )
            if not cur.fetchone():
                return False, None

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    "DELETE FROM incident_memberships WHERE incident_id = ? AND event_id = ?;",
                    (incident_id, event_id),
                )
                cur.execute(
                    "SELECT membership_revision FROM incidents WHERE incident_id = ?;",
                    (incident_id,),
                )
                inc_row = cur.fetchone()
                if not inc_row:
                    cur.execute("COMMIT;")
                    return True, None

                now_str = format_iso(utc_now()) or ""
                new_rev = inc_row["membership_revision"] + 1
                cur.execute(
                    "UPDATE incidents SET membership_revision = ?, updated_at = ? WHERE incident_id = ?;",
                    (new_rev, now_str, incident_id),
                )
                self._sync_recompute_incident_span(cur, incident_id)
                cur.execute("COMMIT;")
                return True, (incident_id, new_rev)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    # -------------------------------------------------------------------------
    # Incident CAS Annotations
    # -------------------------------------------------------------------------

    async def reserve_incident_annotation(
        self,
        incident_id: str,
        namespace: str,
        expected_input_token: str,
        input_namespace: str,
        profile_digest: str,
        worker_id: str | None = None,
        ttl_seconds: float = 60.0,
    ) -> int | None:
        """Reserve an incident annotation slot with transactional CAS comparing fresh input token."""
        validate_safe_id(incident_id, "incident_id")
        validate_bounded_string(namespace, "namespace")
        validate_bounded_string(input_namespace, "input_namespace")
        validate_bounded_string(expected_input_token, "expected_input_token")
        validate_bounded_string(profile_digest, "profile_digest", max_len=256, allow_none=False)
        validate_finite_number(ttl_seconds, "ttl_seconds", min_val=0.1, allow_zero=False)

        return await asyncio.to_thread(
            self._sync_reserve_incident_annotation,
            incident_id,
            namespace,
            expected_input_token,
            input_namespace,
            profile_digest,
            worker_id,
            ttl_seconds,
        )

    def _sync_reserve_incident_annotation(
        self,
        incident_id: str,
        namespace: str,
        expected_input_token: str,
        input_namespace: str,
        profile_digest: str,
        worker_id: str | None,
        ttl_seconds: float,
    ) -> int | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute("SELECT 1 FROM incidents WHERE incident_id = ?;", (incident_id,))
                if not cur.fetchone():
                    cur.execute("ROLLBACK;")
                    return None

                fresh_token = self._sync_compute_incident_input_token(
                    cur, incident_id, input_namespace
                )
                if fresh_token != expected_input_token:
                    cur.execute("ROLLBACK;")
                    return None

                cur.execute(
                    """
                    SELECT generation, status, worker_id, reserved_until, input_token, profile_digest
                    FROM incident_annotations
                    WHERE incident_id = ? AND namespace = ?;
                    """,
                    (incident_id, namespace),
                )
                row = cur.fetchone()
                now_dt = utc_now()
                if row and row["status"] == "reserved":
                    if row["reserved_until"]:
                        res_until = ensure_utc(row["reserved_until"])
                        if res_until > now_dt:
                            if row["worker_id"] != worker_id:
                                cur.execute("ROLLBACK;")
                                return None

                next_gen = (row["generation"] + 1) if row else 1
                lease_until = format_iso(now_dt + timedelta(seconds=ttl_seconds)) or ""
                now_str = format_iso(now_dt) or ""

                cur.execute(
                    """
                    INSERT INTO incident_annotations (
                        incident_id, namespace, generation, input_token, input_namespace,
                        profile_digest, payload_json, status, worker_id, reserved_until,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, '{}', 'reserved', ?, ?, ?, ?)
                    ON CONFLICT(incident_id, namespace) DO UPDATE SET
                        generation = excluded.generation,
                        input_token = excluded.input_token,
                        input_namespace = excluded.input_namespace,
                        profile_digest = excluded.profile_digest,
                        payload_json = '{}',
                        status = excluded.status,
                        worker_id = excluded.worker_id,
                        reserved_until = excluded.reserved_until,
                        updated_at = excluded.updated_at;
                    """,
                    (
                        incident_id,
                        namespace,
                        next_gen,
                        expected_input_token,
                        input_namespace,
                        profile_digest,
                        worker_id,
                        lease_until,
                        now_str,
                        now_str,
                    ),
                )
                cur.execute("COMMIT;")
                return next_gen
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def set_incident_annotation_cas(
        self,
        incident_id: str,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int,
        expected_input_token: str,
        profile_digest: str,
    ) -> bool:
        """Commit incident annotation result with transactional CAS comparing fresh input token."""
        validate_safe_id(incident_id, "incident_id")
        validate_bounded_string(namespace, "namespace")
        validate_payload_json(payload, "payload")
        validate_bounded_string(expected_input_token, "expected_input_token")
        validate_bounded_string(profile_digest, "profile_digest", max_len=256, allow_none=False)

        success, notify_tuple = await asyncio.to_thread(
            self._sync_set_incident_annotation_cas,
            incident_id,
            namespace,
            payload,
            expected_generation,
            expected_input_token,
            profile_digest,
        )
        if notify_tuple:
            self._notify_incident_subscribers(*notify_tuple)
        return success

    def _sync_set_incident_annotation_cas(
        self,
        incident_id: str,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int,
        expected_input_token: str,
        profile_digest: str,
    ) -> tuple[bool, tuple[str, int] | None]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    SELECT generation, input_token, input_namespace, profile_digest, status, reserved_until
                    FROM incident_annotations
                    WHERE incident_id = ? AND namespace = ?;
                    """,
                    (incident_id, namespace),
                )
                row = cur.fetchone()
                if not row or row["generation"] != expected_generation:
                    cur.execute("ROLLBACK;")
                    return False, None

                if row["status"] != "reserved":
                    cur.execute("ROLLBACK;")
                    return False, None

                now_dt = utc_now()
                if row["reserved_until"]:
                    res_until = ensure_utc(row["reserved_until"])
                    if res_until <= now_dt:
                        cur.execute("ROLLBACK;")
                        return False, None

                if row["profile_digest"] != profile_digest:
                    cur.execute("ROLLBACK;")
                    return False, None

                input_ns = row["input_namespace"]
                fresh_token = self._sync_compute_incident_input_token(cur, incident_id, input_ns)
                if fresh_token != expected_input_token:
                    cur.execute("ROLLBACK;")
                    return False, None

                now_str = format_iso(utc_now()) or ""
                cur.execute(
                    """
                    UPDATE incident_annotations
                    SET payload_json = ?, status = 'ready', worker_id = NULL, reserved_until = NULL, updated_at = ?
                    WHERE incident_id = ? AND namespace = ?;
                    """,
                    (json.dumps(payload), now_str, incident_id, namespace),
                )
                cur.execute("COMMIT;")

                cur.execute(
                    "SELECT membership_revision FROM incidents WHERE incident_id = ?;",
                    (incident_id,),
                )
                inc_row = cur.fetchone()
                m_rev = inc_row["membership_revision"] if inc_row else 1
                return True, (incident_id, m_rev)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    # -------------------------------------------------------------------------
    # Incident Queries
    # -------------------------------------------------------------------------

    async def list_incidents(
        self,
        limit: int = 50,
        before_cursor: str | None = None,
    ) -> tuple[list[Incident], str | None]:
        """List incidents with keyset pagination by immutable anchor_at."""
        return await asyncio.to_thread(self._sync_list_incidents, limit, before_cursor)

    def _sync_list_incidents(
        self,
        limit: int = 50,
        before_cursor: str | None = None,
    ) -> tuple[list[Incident], str | None]:
        clamped_limit = max(1, min(100, limit))
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            query = "SELECT incident_id FROM incidents"
            params: list[Any] = []

            if before_cursor and "|" in before_cursor:
                parts = before_cursor.split("|", 1)
                cursor_anchor, cursor_id = parts[0], parts[1]
                query += " WHERE (anchor_at < ?) OR (anchor_at = ? AND incident_id < ?)"
                params.extend([cursor_anchor, cursor_anchor, cursor_id])

            query += " ORDER BY anchor_at DESC, incident_id DESC LIMIT ?;"
            params.append(clamped_limit + 1)

            cur.execute(query, params)
            rows = cur.fetchall()

            has_more = len(rows) > clamped_limit
            page_rows = rows[:clamped_limit]

            incidents: list[Incident] = []
            for r in page_rows:
                inc = self._sync_hydrate_incident(cur, r["incident_id"])
                if inc:
                    incidents.append(inc)

            next_cursor = None
            if has_more and page_rows:
                last_inc = incidents[-1]
                next_cursor = f"{format_iso(last_inc.anchor_at)}|{last_inc.incident_id}"

            return incidents, next_cursor

    # -------------------------------------------------------------------------
    # Dynamic Auto-Association
    # -------------------------------------------------------------------------

    def _sync_evaluate_auto_association(
        self,
        cur: sqlite3.Cursor,
        event_id: str,
        correlation_group: str | None,
        zone: str | None,
        adjacent_zones: Sequence[str] | None,
        source_clock_offset_seconds: float,
        source_time_uncertainty_seconds: float,
        global_tolerance_seconds: float = 10.0,
        max_incident_span_seconds: float = 300.0,
    ) -> Incident | None:
        """Evaluate auto-association against active/recent incidents within max span and group/zone isolation.

        Never sweeps historical archive. Uses clearly labelled declared source-time association.
        """
        cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
        ev_row = cur.fetchone()
        if not ev_row:
            return None

        event_start = ensure_utc(ev_row["start_at"])
        event_end = ensure_utc(
            ev_row["end_at"] or ev_row["desired_capture_end"] or ev_row["start_at"]
        )

        offset_delta = timedelta(seconds=source_clock_offset_seconds)
        adj_event_start = event_start + offset_delta
        adj_event_end = event_end + offset_delta

        total_tolerance = timedelta(
            seconds=global_tolerance_seconds + source_time_uncertainty_seconds
        )
        search_start = format_iso(adj_event_start - total_tolerance) or ""
        search_end = format_iso(adj_event_end + total_tolerance) or ""

        cur.execute(
            """
            SELECT incident_id, anchor_at, span_start, span_end, reason
            FROM incidents
            WHERE span_start <= ? AND span_end >= ?
            ORDER BY span_end DESC;
            """,
            (search_end, search_start),
        )
        candidates = cur.fetchall()

        matched_incident_id: str | None = None
        match_reason: str | None = None
        adj_set = set(adjacent_zones or ())

        for cand in candidates:
            cand_id = cand["incident_id"]
            cand_start = ensure_utc(cand["span_start"])
            cand_end = ensure_utc(cand["span_end"])

            new_span_seconds = (
                max(cand_end, adj_event_end) - min(cand_start, adj_event_start)
            ).total_seconds()
            if new_span_seconds > max_incident_span_seconds:
                continue

            cur.execute(
                """
                SELECT reason FROM incident_memberships WHERE incident_id = ?;
                """,
                (cand_id,),
            )
            member_reasons = [r["reason"] or "" for r in cur.fetchall()]
            member_tag_sets = [set(r.split(":")) for r in member_reasons]

            # 1. Matching correlation_group (exact token match)
            if correlation_group and correlation_group.strip():
                expected_grp_tag = f"group={correlation_group.strip()}"
                if any(expected_grp_tag in tag_set for tag_set in member_tag_sets):
                    matched_incident_id = cand_id
                    match_reason = f"auto_association:{expected_grp_tag}"
                    break

            # 2. Matching zone or adjacent zones (exact token match)
            if zone and zone.strip():
                clean_zone = zone.strip()
                target_zone_tags = {f"zone={clean_zone}"}
                for az in adj_set:
                    if az and az.strip():
                        target_zone_tags.add(f"zone={az.strip()}")

                zone_match = any(bool(tag_set & target_zone_tags) for tag_set in member_tag_sets)
                if zone_match:
                    matched_incident_id = cand_id
                    match_reason = f"auto_association:zone={clean_zone}"
                    break

        if matched_incident_id:
            inc, _, _notify = self._sync_create_or_join_incident(
                cur, matched_incident_id, event_id, match_reason
            )
            return inc
        else:
            new_inc_id = f"inc_{uuid.uuid4().hex[:12]}"
            tags = []
            if correlation_group and correlation_group.strip():
                tags.append(f"group={correlation_group.strip()}")
            if zone and zone.strip():
                tags.append(f"zone={zone.strip()}")
            create_reason = f"auto_association:{':'.join(tags)}" if tags else "auto_association:isolated"
            inc, _, _notify = self._sync_create_or_join_incident(
                cur, new_inc_id, event_id, create_reason
            )
            return inc
