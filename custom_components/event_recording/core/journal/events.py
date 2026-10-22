"""Event queries, hydration, activity feed, and deletion for JournalStore.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
Preserves keyset cursor pagination, json_extract matching, and atomic cascaded deletion.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from typing import Any

from ..media import unlink_promoted_media
from ..models import (
    MediaItem,
    RecordedEvent,
    SourceEventRecord,
    ensure_utc,
    format_iso,
    utc_now,
    validate_safe_id,
)
from .common import CommitEffects, JournalStoreBase

_LOGGER = logging.getLogger(__name__)


class EventsMixin(JournalStoreBase):
    """Encapsulates event hydration, query pagination, activity, and deletion."""

    def _sync_hydrate_event(self, cur: sqlite3.Cursor, event_row: dict[str, Any]) -> RecordedEvent:
        return self._sync_hydrate_events(cur, [event_row])[0]

    def _sync_hydrate_events(
        self, cur: sqlite3.Cursor, rows: list[dict[str, Any]]
    ) -> list[RecordedEvent]:
        """Load each child table once per page, preserving the full event contract."""
        if not rows:
            return []
        ids = [row["event_id"] for row in rows]
        placeholders = ",".join("?" for _ in ids)
        children: dict[str, dict[str, list[dict[str, Any]]]] = {}
        for table, order in (
            ("source_journal", "id"),
            ("media", "created_at"),
            ("annotations", "namespace"),
        ):
            cur.execute(
                f"SELECT * FROM {table} WHERE event_id IN ({placeholders}) ORDER BY {order};", ids
            )
            grouped: dict[str, list[dict[str, Any]]] = {}
            for child in cur.fetchall():
                grouped.setdefault(child["event_id"], []).append(child)
            children[table] = grouped
        cur.execute(
            f"SELECT DISTINCT event_id FROM media_pins WHERE event_id IN ({placeholders}) AND expires_at > ?;",
            [*ids, format_iso(utc_now())],
        )
        pinned = {row["event_id"] for row in cur.fetchall()}
        return [
            self._event_from_rows(
                row,
                children["source_journal"].get(row["event_id"], []),
                children["media"].get(row["event_id"], []),
                children["annotations"].get(row["event_id"], []),
                row["event_id"] in pinned,
            )
            for row in rows
        ]

    @staticmethod
    def _event_from_rows(
        event_row: dict[str, Any],
        source_rows: list[dict[str, Any]],
        media_rows: list[dict[str, Any]],
        annotation_rows: list[dict[str, Any]],
        has_active_pins: bool,
    ) -> RecordedEvent:
        event_id = event_row["event_id"]
        source_records = tuple(
            SourceEventRecord(
                id=r["id"],
                source_id=r["source_id"],
                kind=r["kind"],
                occurred_at=ensure_utc(r["occurred_at"]),
                idempotency_key=r["idempotency_key"],
                label=r["label"],
                payload=json.loads(r["payload_json"]) if r["payload_json"] else {},
                correlation_id=r.get("correlation_id"),
                created_at=ensure_utc(r["created_at"]),
            )
            for r in source_rows
        )

        media_items = tuple(
            MediaItem(
                id=m["id"],
                kind=m["kind"],
                mime_type=m["mime_type"],
                file_name=m["file_name"],
                duration=m["duration"],
                size_bytes=m["size_bytes"],
                available=bool(m["available"]),
                is_pinned=has_active_pins,
                content_hash=m["content_hash"],
                created_at=ensure_utc(m["created_at"]),
                requested_start=ensure_utc(m["requested_start"])
                if m.get("requested_start")
                else None,
                requested_end=ensure_utc(m["requested_end"]) if m.get("requested_end") else None,
                capture_start=ensure_utc(m["capture_start"]) if m.get("capture_start") else None,
                capture_end=ensure_utc(m["capture_end"]) if m.get("capture_end") else None,
                time_quality=m.get("time_quality"),
                clock_source=m.get("clock_source"),
                time_uncertainty_seconds=m.get("time_uncertainty_seconds"),
                start_offset_seconds=m.get("start_offset_seconds"),
                continuity=m.get("continuity"),
            )
            for m in media_rows
        )

        annotations = {}
        for a in annotation_rows:
            raw_payload = json.loads(a["payload_json"]) if a["payload_json"] else {}
            flat = dict(raw_payload) if isinstance(raw_payload, dict) else {}
            for internal_key in (
                "worker_id",
                "reserved_by",
                "reserved_until",
                "leased_until",
                "lease_token",
                "payload",
            ):
                flat.pop(internal_key, None)
            flat["status"] = a["status"]
            flat["generation"] = a["generation"]
            flat["media_revision"] = a["expected_media_revision"]
            if a.get("updated_at"):
                flat["updated_at"] = a["updated_at"]
            annotations[a["namespace"]] = flat

        coverage = json.loads(event_row["coverage_json"]) if event_row["coverage_json"] else {}
        access_entities = (
            tuple(json.loads(event_row["access_entities_json"]))
            if event_row["access_entities_json"]
            else ()
        )

        return RecordedEvent(
            event_id=event_id,
            source_id=event_row["source_id"],
            kind=event_row["kind"],
            start_at=ensure_utc(event_row["start_at"]),
            desired_capture_end=ensure_utc(event_row["desired_capture_end"]),
            capture_state=event_row["capture_state"],
            capture_error=event_row.get("capture_error"),
            label=event_row["label"],
            end_at=ensure_utc(event_row["end_at"]) if event_row["end_at"] else None,
            media_revision=event_row["media_revision"],
            revision=event_row["revision"],
            source_events=source_records,
            media=media_items,
            coverage=coverage,
            annotations=annotations,
            access_entities=access_entities,
            owner_user_id=event_row["owner_user_id"],
            correlation_id=event_row.get("correlation_id"),
            created_at=ensure_utc(event_row["created_at"]),
            updated_at=ensure_utc(event_row["updated_at"]),
        )

    async def get_event(self, event_id: str) -> RecordedEvent | None:
        """Fetch a recorded event by ID off event loop."""
        return await asyncio.to_thread(self._sync_get_event, event_id)

    def _sync_get_event(self, event_id: str) -> RecordedEvent | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            row = cur.fetchone()
            if not row:
                return None
            return self._sync_hydrate_event(cur, row)

    async def get_event_access(self, event_id: str) -> Any:
        """Read only authorization fields for revision subscriptions."""
        from types import SimpleNamespace

        def read() -> Any:
            with self._lock:
                assert self._conn is not None
                row = self._conn.execute(
                    "SELECT owner_user_id, access_entities_json FROM events WHERE event_id = ?;",
                    (event_id,),
                ).fetchone()
                return (
                    SimpleNamespace(
                        owner_user_id=row["owner_user_id"],
                        access_entities=tuple(json.loads(row["access_entities_json"] or "[]")),
                    )
                    if row
                    else None
                )

        return await asyncio.to_thread(read)

    async def list_events(
        self,
        filters: dict[str, Any] | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> tuple[list[RecordedEvent], str | None]:
        """List events with keyset cursor pagination."""
        return await asyncio.to_thread(self._sync_list_events, filters, limit, cursor)

    def _sync_list_events(
        self,
        filters: dict[str, Any] | None,
        limit: int,
        cursor: str | None,
    ) -> tuple[list[RecordedEvent], str | None]:
        filters = filters or {}
        limit = max(1, min(limit, 200))
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            clauses = ["1=1"]
            params: list[Any] = []

            if "source_id" in filters:
                clauses.append("source_id = ?")
                params.append(filters["source_id"])
            if "kind" in filters:
                clauses.append("kind = ?")
                params.append(filters["kind"])
            if "capture_state" in filters:
                clauses.append("capture_state = ?")
                params.append(filters["capture_state"])
            if "start_after" in filters:
                clauses.append("start_at >= ?")
                params.append(format_iso(ensure_utc(filters["start_after"])))
            if "start_before" in filters:
                clauses.append("start_at <= ?")
                params.append(format_iso(ensure_utc(filters["start_before"])))

            annotation_exclusion = filters.get("exclude_current_complete_annotation")
            if annotation_exclusion is not None:
                namespace = annotation_exclusion["namespace"]
                field_path = f"$.{annotation_exclusion['field']}"
                encoded_value = json.dumps(annotation_exclusion["equals"], allow_nan=False)
                observation_guard = ""
                if (
                    namespace == "event_vision"
                    and annotation_exclusion["field"] == "activity"
                    and annotation_exclusion["equals"] is False
                ):
                    observation_guard = """
                        AND negative_observation(
                            events.capture_state, events.capture_error, events.coverage_json,
                            events.media_revision, a.payload_json,
                            (SELECT json_group_array(json_object(
                                'id', m.id, 'kind', m.kind, 'mime_type', m.mime_type,
                                'available', json(CASE WHEN m.available = 1 THEN 'true' ELSE 'false' END)
                            )) FROM media AS m WHERE m.event_id = events.event_id)
                        ) = 1
                    """
                clauses.append(
                    f"""
                    NOT EXISTS (
                        SELECT 1
                        FROM annotations AS a
                        WHERE a.event_id = events.event_id
                          AND a.namespace = ?
                          AND a.status = 'complete'
                          AND a.expected_media_revision = events.media_revision
                          AND (
                              json_type(a.payload_json, ?) = json_type(?, '$')
                              OR (
                                  json_type(a.payload_json, ?) IN ('integer', 'real')
                                  AND json_type(?, '$') IN ('integer', 'real')
                              )
                          )
                          AND json_extract(a.payload_json, ?) IS json_extract(?, '$')
                          {observation_guard}
                    )
                    """
                )
                params.extend(
                    [
                        namespace,
                        field_path,
                        encoded_value,
                        field_path,
                        encoded_value,
                        field_path,
                        encoded_value,
                    ]
                )

            if filters.get("exclude_journal_only") is True:
                clauses.append(
                    """
                    NOT (
                        capture_state IN ('ready', 'expired')
                        AND COALESCE(capture_error, '') = ''
                        AND NOT EXISTS (
                            SELECT 1 FROM media AS m WHERE m.event_id = events.event_id
                        )
                    )
                    """
                )

            if cursor:
                try:
                    c_start, c_id = cursor.split("|", 1)
                    clauses.append("(start_at < ? OR (start_at = ? AND event_id < ?))")
                    params.extend([c_start, c_start, c_id])
                except ValueError:
                    pass

            where_str = " AND ".join(clauses)
            query = f"SELECT * FROM events WHERE {where_str} ORDER BY start_at DESC, event_id DESC LIMIT ?;"
            params.append(limit + 1)

            cur.execute(query, params)
            rows = cur.fetchall()

            has_more = len(rows) > limit
            result_rows = rows[:limit]
            events = self._sync_hydrate_events(cur, result_rows)

            next_cursor = None
            if has_more and result_rows:
                last = result_rows[-1]
                next_cursor = f"{last['start_at']}|{last['event_id']}"

            return events, next_cursor

    async def list_activity(
        self,
        group_by_incident: bool = False,
        limit: int = 50,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Query unified timeline entries (incidents and independent events) with keyset pagination."""
        clamped_limit = max(1, min(100, limit))
        return await asyncio.to_thread(
            self._sync_list_activity, group_by_incident, clamped_limit, cursor
        )

    def _sync_list_activity(
        self,
        group_by_incident: bool,
        limit: int,
        cursor: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            if not group_by_incident:
                # Direct events timeline
                query = "SELECT 'event' AS item_type, event_id AS item_id, start_at AS sort_time FROM events"
                params: list[Any] = []
                if cursor and "|" in cursor:
                    parts = cursor.split("|")
                    c_time = parts[0]
                    c_id = parts[-1]
                    query += " WHERE (start_at < ?) OR (start_at = ? AND event_id < ?)"
                    params.extend([c_time, c_time, c_id])
                query += " ORDER BY start_at DESC, event_id DESC LIMIT ?;"
                params.append(limit + 1)
                cur.execute(query, params)
                rows = cur.fetchall()

                has_more = len(rows) > limit
                page_rows = rows[:limit]
                items: list[dict[str, Any]] = []
                for r in page_rows:
                    cur.execute("SELECT * FROM events WHERE event_id = ?;", (r["item_id"],))
                    ev_row = cur.fetchone()
                    if ev_row:
                        ev = self._sync_hydrate_event(cur, ev_row)
                        items.append(
                            {
                                "type": "event",
                                "event": ev,
                                "sort_time": r["sort_time"],
                                "item_id": r["item_id"],
                            }
                        )
                next_cur = None
                if has_more and page_rows:
                    last_r = page_rows[-1]
                    next_cur = f"{last_r['sort_time']}|event|{last_r['item_id']}"
                return items, next_cur

            # group_by_incident = True: Unified union of incidents and events
            base_sql = """
                SELECT 'incident' AS item_type, incident_id AS item_id, anchor_at AS sort_time
                FROM incidents
                UNION ALL
                SELECT 'event' AS item_type, event_id AS item_id, start_at AS sort_time
                FROM events e
            """
            params = []
            where_clause = ""
            if cursor and "|" in cursor:
                parts = cursor.split("|")
                if len(parts) >= 3:
                    c_time, c_type, c_id = parts[0], parts[1], parts[2]
                    where_clause = """
                        WHERE (sort_time < ?)
                           OR (sort_time = ? AND item_type < ?)
                           OR (sort_time = ? AND item_type = ? AND item_id < ?)
                    """
                    params.extend([c_time, c_time, c_type, c_time, c_type, c_id])
                else:
                    c_time, c_id = parts[0], parts[1]
                    where_clause = "WHERE (sort_time < ?) OR (sort_time = ? AND item_id < ?)"
                    params.extend([c_time, c_time, c_id])

            full_query = f"SELECT * FROM ({base_sql}) AS timeline {where_clause} ORDER BY sort_time DESC, item_type DESC, item_id DESC LIMIT ?;"
            params.append(limit + 1)
            cur.execute(full_query, params)
            rows = cur.fetchall()

            has_more = len(rows) > limit
            page_rows = rows[:limit]

            items = []
            for r in page_rows:
                itype = r["item_type"]
                iid = r["item_id"]
                stime = r["sort_time"]

                if itype == "incident":
                    inc = getattr(self, "_sync_hydrate_incident")(cur, iid)
                    if not inc:
                        continue
                    cur.execute(
                        """
                        SELECT e.* FROM events e
                        JOIN incident_memberships im ON im.event_id = e.event_id
                        WHERE im.incident_id = ?
                        ORDER BY e.start_at ASC, e.event_id ASC;
                        """,
                        (iid,),
                    )
                    members = [self._sync_hydrate_event(cur, m_row) for m_row in cur.fetchall()]
                    items.append(
                        {
                            "type": "incident",
                            "incident": inc,
                            "members": members,
                            "sort_time": stime,
                            "item_id": iid,
                        }
                    )
                else:
                    cur.execute("SELECT * FROM events WHERE event_id = ?;", (iid,))
                    ev_row = cur.fetchone()
                    if ev_row:
                        ev = self._sync_hydrate_event(cur, ev_row)
                        cur.execute(
                            "SELECT incident_id FROM incident_memberships WHERE event_id = ?;",
                            (iid,),
                        )
                        inc_ids = [r["incident_id"] for r in cur.fetchall()]
                        items.append(
                            {
                                "type": "event",
                                "event": ev,
                                "incident_ids": inc_ids,
                                "sort_time": stime,
                                "item_id": iid,
                            }
                        )

            next_cur = None
            if has_more and page_rows:
                last_r = page_rows[-1]
                next_cur = f"{last_r['sort_time']}|{last_r['item_type']}|{last_r['item_id']}"

            return items, next_cur

    async def delete_event(self, event_id: str) -> bool:
        """Explicitly delete an event, unlinking media and cascading to incident memberships."""
        validate_safe_id(event_id, "event_id")
        effects: CommitEffects = await asyncio.to_thread(self._sync_delete_event, event_id)
        if effects.result:
            for inc_id, m_rev in effects.incident_notifications:
                self._notify_incident_subscribers(inc_id, m_rev)
        return bool(effects.result)

    def _sync_delete_event(self, event_id: str) -> CommitEffects:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            now_str = format_iso(utc_now()) or ""

            # 1. Respect active media pins: cannot delete pinned event
            cur.execute(
                "SELECT 1 FROM media_pins WHERE event_id = ? AND expires_at > ? LIMIT 1;",
                (event_id, now_str),
            )
            if cur.fetchone():
                return CommitEffects(result=False)

            cur.execute("SELECT 1 FROM events WHERE event_id = ?;", (event_id,))
            if not cur.fetchone():
                return CommitEffects(result=False)

            cur.execute(
                "SELECT incident_id FROM incident_memberships WHERE event_id = ?;", (event_id,)
            )
            inc_ids = [r["incident_id"] for r in cur.fetchall()]

            cur.execute(
                "SELECT file_name, size_bytes FROM media WHERE event_id = ? AND available = 1;",
                (event_id,),
            )
            media_files = [(r["file_name"], r["size_bytes"] or 0) for r in cur.fetchall()]

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute("DELETE FROM events WHERE event_id = ?;", (event_id,))
                for inc_id in inc_ids:
                    cur.execute(
                        "UPDATE incidents SET membership_revision = membership_revision + 1, updated_at = ? WHERE incident_id = ?;",
                        (now_str, inc_id),
                    )
                    getattr(self, "_sync_recompute_incident_span")(cur, inc_id)
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            freed_bytes = 0
            unlinked_count = 0
            for fn, sz in media_files:
                try:
                    if unlink_promoted_media(self._media_root, fn):
                        freed_bytes += sz
                        unlinked_count += 1
                except Exception:
                    pass

            if unlinked_count > 0:
                self.ledger.record_unlinked(freed_bytes, count=unlinked_count)

            notifications: list[tuple[str, int]] = []
            for inc_id in inc_ids:
                cur.execute(
                    "SELECT membership_revision FROM incidents WHERE incident_id = ?;", (inc_id,)
                )
                row = cur.fetchone()
                m_rev = row["membership_revision"] if row else 1
                notifications.append((inc_id, m_rev))

            return CommitEffects(
                result=True,
                unlinked_bytes=freed_bytes,
                unlinked_count=unlinked_count,
                incident_notifications=notifications,
            )
