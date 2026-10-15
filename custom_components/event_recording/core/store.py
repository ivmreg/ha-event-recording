"""Durable SQLite JournalStore for Event Recording.

Strict boundary: Zero Home Assistant imports, zero AI imports.
Thread-safe single-writer off-loop execution with WAL, synchronous=FULL,
atomic intent admission, media-set revisioning, and CAS annotations.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import sqlite3
import threading
import uuid
from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from .media import (
    MediaProbe,
    MediaTypeMismatchError,
    ProbeUnavailableError,
    stage_and_promote_media,
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
    utc_now,
    validate_bounded_string,
    validate_finite_number,
    validate_payload_json,
    validate_safe_id,
)

_LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = 1


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    fields = [col[0] for col in cursor.description]
    return dict(zip(fields, row, strict=False))


class JournalStore:
    """Neutral event journal and media store backed by SQLite."""

    def __init__(
        self,
        db_path: str | Path,
        media_root: str | Path,
        allowed_media_roots: Sequence[str | Path] | None = None,
        probe: MediaProbe | None = None,
    ) -> None:
        self._db_path_raw = Path(db_path)
        self._media_root_raw = Path(media_root)
        self._allowed_media_roots_raw = [
            Path(p) for p in (allowed_media_roots or [])
        ]
        self._db_path = self._db_path_raw
        self._media_root = self._media_root_raw
        self._allowed_media_roots = [self._media_root, *self._allowed_media_roots_raw]
        self._probe = probe
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._is_open = False
        self._subscribers: set[Callable[[str, int], None]] = set()

    @property
    def is_open(self) -> bool:
        """Return whether SQLite connection is active."""
        return self._is_open

    @property
    def media_root(self) -> Path:
        """Return resolved owned media root."""
        return self._media_root

    def subscribe(self, callback: Callable[[str, int], None]) -> Callable[[], None]:
        """Subscribe to post-commit event revision updates.

        Dispatched outside the SQLite lock on the event loop.
        """
        self._subscribers.add(callback)

        def _unsub() -> None:
            self._subscribers.discard(callback)

        return _unsub

    def _notify_subscribers(self, event_id: str, revision: int) -> None:
        """Dispatch revision notification to all registered subscribers."""
        for cb in list(self._subscribers):
            try:
                cb(event_id, revision)
            except Exception as err:
                _LOGGER.warning("Error notifying store subscriber for %s: %s", event_id, err)

    async def open(self) -> None:
        """Open database connection and execute migrations off event loop."""
        await asyncio.to_thread(self._sync_open)

    async def close(self) -> None:
        """Close database connection off event loop."""
        await asyncio.to_thread(self._sync_close)

    def _sync_open(self) -> None:
        with self._lock:
            if self._is_open:
                return
            self._db_path = self._db_path_raw.resolve()
            self._media_root = self._media_root_raw.resolve()
            self._allowed_media_roots = [self._media_root] + [
                p.resolve() for p in self._allowed_media_roots_raw
            ]
            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._media_root.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(
                str(self._db_path),
                timeout=30.0,
                check_same_thread=False,
                isolation_level=None,  # Autocommit mode; explicit BEGIN for transactions
            )
            conn.row_factory = _dict_factory
            conn.execute("PRAGMA journal_mode = WAL;")
            conn.execute("PRAGMA synchronous = FULL;")
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("PRAGMA busy_timeout = 10000;")
            self._conn = conn
            self._sync_migrate()
            self._is_open = True

    def _sync_close(self) -> None:
        with self._lock:
            if not self._is_open or self._conn is None:
                return
            try:
                self._conn.close()
            except Exception as err:
                _LOGGER.warning("Error closing SQLite connection: %s", err)
            finally:
                self._conn = None
                self._is_open = False

    def _sync_migrate(self) -> None:
        assert self._conn is not None
        cur = self._conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_info (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            );
            """
        )
        cur.execute("SELECT MAX(version) as max_v FROM schema_info;")
        row = cur.fetchone()
        current_version = row["max_v"] if row and row["max_v"] is not None else 0

        if current_version < 1:
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS events (
                        event_id TEXT PRIMARY KEY,
                        source_id TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        start_at TEXT NOT NULL,
                        desired_capture_end TEXT NOT NULL,
                        capture_state TEXT NOT NULL,
                        capture_error TEXT,
                        label TEXT,
                        end_at TEXT,
                        media_revision INTEGER NOT NULL DEFAULT 1,
                        revision INTEGER NOT NULL DEFAULT 1,
                        coverage_json TEXT,
                        access_entities_json TEXT,
                        owner_user_id TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS source_journal (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        event_id TEXT NOT NULL,
                        source_id TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        occurred_at TEXT NOT NULL,
                        idempotency_key TEXT NOT NULL,
                        label TEXT,
                        payload_json TEXT,
                        created_at TEXT NOT NULL,
                        UNIQUE(source_id, idempotency_key),
                        FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media (
                        id TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL,
                        kind TEXT NOT NULL,
                        mime_type TEXT NOT NULL,
                        file_name TEXT NOT NULL,
                        duration REAL,
                        size_bytes INTEGER NOT NULL DEFAULT 0,
                        available INTEGER NOT NULL DEFAULT 1,
                        content_hash TEXT,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS recording_intents (
                        intent_id TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL,
                        resource_key TEXT NOT NULL,
                        operation TEXT NOT NULL,
                        window_seconds REAL NOT NULL,
                        lookback_seconds REAL NOT NULL DEFAULT 0.0,
                        hold INTEGER NOT NULL DEFAULT 0,
                        recorder_config_json TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        worker_id TEXT,
                        leased_until TEXT,
                        error TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS resource_sessions (
                        resource_key TEXT PRIMARY KEY,
                        active_event_id TEXT,
                        last_activity_at TEXT NOT NULL,
                        desired_capture_end TEXT NOT NULL,
                        hold INTEGER NOT NULL DEFAULT 0,
                        tail_window_seconds REAL NOT NULL DEFAULT 0.0,
                        recorder_config_json TEXT,
                        access_entities_json TEXT,
                        owner_user_id TEXT,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY (active_event_id) REFERENCES events (event_id) ON DELETE SET NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS annotations (
                        event_id TEXT NOT NULL,
                        namespace TEXT NOT NULL,
                        generation INTEGER NOT NULL DEFAULT 1,
                        expected_media_revision INTEGER NOT NULL DEFAULT 1,
                        payload_json TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'ready',
                        reserved_by TEXT,
                        reserved_until TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (event_id, namespace),
                        FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS media_pins (
                        pin_id TEXT PRIMARY KEY,
                        event_id TEXT NOT NULL,
                        lease_token TEXT NOT NULL UNIQUE,
                        expires_at TEXT NOT NULL,
                        reason TEXT,
                        created_at TEXT NOT NULL,
                        FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                    );
                    """
                )

                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_start ON events(start_at DESC, event_id DESC);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_capture_state ON events(capture_state);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_journal_event_id ON source_journal(event_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_journal_source_idemp ON source_journal(source_id, idempotency_key);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_media_event ON media(event_id);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_intents_status ON recording_intents(status, leased_until);")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_pins_expiry ON media_pins(event_id, expires_at);")

                cur.execute(
                    "INSERT INTO schema_info (version, applied_at) VALUES (?, ?);",
                    (1, format_iso(utc_now()) or ""),
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    # -------------------------------------------------------------------------
    # Helper Data Hydration
    # -------------------------------------------------------------------------

    def _sync_hydrate_event(self, cur: sqlite3.Cursor, event_row: dict[str, Any]) -> RecordedEvent:
        event_id = event_row["event_id"]

        # Hydrate source events
        cur.execute(
            """
            SELECT id, source_id, kind, occurred_at, idempotency_key, label, payload_json, created_at
            FROM source_journal
            WHERE event_id = ?
            ORDER BY id ASC;
            """,
            (event_id,),
        )
        source_rows = cur.fetchall()
        source_records = tuple(
            SourceEventRecord(
                id=r["id"],
                source_id=r["source_id"],
                kind=r["kind"],
                occurred_at=ensure_utc(r["occurred_at"]),
                idempotency_key=r["idempotency_key"],
                label=r["label"],
                payload=json.loads(r["payload_json"]) if r["payload_json"] else {},
                created_at=ensure_utc(r["created_at"]),
            )
            for r in source_rows
        )

        # Hydrate media items
        cur.execute(
            """
            SELECT id, kind, mime_type, file_name, duration, size_bytes, available, content_hash, created_at
            FROM media
            WHERE event_id = ?
            ORDER BY created_at ASC;
            """,
            (event_id,),
        )
        media_rows = cur.fetchall()

        # Check if there are active pins
        now_str = format_iso(utc_now()) or ""
        cur.execute(
            "SELECT 1 FROM media_pins WHERE event_id = ? AND expires_at > ? LIMIT 1;",
            (event_id, now_str),
        )
        has_active_pins = cur.fetchone() is not None

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
            )
            for m in media_rows
        )

        # Hydrate annotations
        cur.execute(
            """
            SELECT namespace, generation, expected_media_revision, payload_json, status, updated_at
            FROM annotations
            WHERE event_id = ?;
            """,
            (event_id,),
        )
        annotation_rows = cur.fetchall()
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
            created_at=ensure_utc(event_row["created_at"]),
            updated_at=ensure_utc(event_row["updated_at"]),
        )

    # -------------------------------------------------------------------------
    # Public Read APIs
    # -------------------------------------------------------------------------

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

    async def get_media(self, media_id: str) -> MediaItem | None:
        """Fetch a media item by ID off event loop."""
        return await asyncio.to_thread(self._sync_get_media, media_id)

    def _sync_get_media(self, media_id: str) -> MediaItem | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM media WHERE id = ?;", (media_id,))
            row = cur.fetchone()
            if not row:
                return None
            now_str = format_iso(utc_now()) or ""
            cur.execute(
                "SELECT 1 FROM media_pins WHERE event_id = ? AND expires_at > ? LIMIT 1;",
                (row["event_id"], now_str),
            )
            has_pins = cur.fetchone() is not None
            return MediaItem(
                id=row["id"],
                kind=row["kind"],
                mime_type=row["mime_type"],
                file_name=row["file_name"],
                duration=row["duration"],
                size_bytes=row["size_bytes"],
                available=bool(row["available"]),
                is_pinned=has_pins,
                content_hash=row["content_hash"],
                created_at=ensure_utc(row["created_at"]),
            )

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
            events = [self._sync_hydrate_event(cur, r) for r in result_rows]

            next_cursor = None
            if has_more and result_rows:
                last = result_rows[-1]
                next_cursor = f"{last['start_at']}|{last['event_id']}"

            return events, next_cursor

    async def list_finalized_media(
        self,
        cursor: str | None = None,
        limit: int = 50,
        namespace_lacking: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Startup catchup / poll query for retained ready events with available media."""
        return await asyncio.to_thread(self._sync_list_finalized_media, cursor, limit, namespace_lacking)

    def _sync_list_finalized_media(
        self,
        cursor: str | None,
        limit: int,
        namespace_lacking: str | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        limit = max(1, min(limit, 200))
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            clauses = ["e.capture_state IN ('ready', 'partial')", "m.available = 1"]
            params: list[Any] = []

            if namespace_lacking:
                now_str = format_iso(utc_now()) or ""
                clauses.append(
                    """
                    NOT EXISTS (
                        SELECT 1 FROM annotations a
                        WHERE a.event_id = e.event_id
                          AND a.namespace = ?
                          AND a.expected_media_revision = e.media_revision
                          AND (
                              a.status IN ('complete', 'ready', 'failed')
                              OR (a.status IN ('pending', 'reserved') AND a.reserved_until > ?)
                          )
                    )
                    """
                )
                params.extend([namespace_lacking, now_str])

            if cursor:
                try:
                    c_start, c_id = cursor.split("|", 1)
                    clauses.append("(e.start_at < ? OR (e.start_at = ? AND e.event_id < ?))")
                    params.extend([c_start, c_start, c_id])
                except ValueError:
                    pass

            where_str = " AND ".join(clauses)
            query = f"""
                SELECT DISTINCT e.event_id, e.source_id, e.kind, e.start_at, e.end_at,
                                e.media_revision, e.revision, e.capture_state, e.capture_error
                FROM events e
                JOIN media m ON m.event_id = e.event_id
                WHERE {where_str}
                ORDER BY e.start_at DESC, e.event_id DESC
                LIMIT ?;
            """
            params.append(limit + 1)

            cur.execute(query, params)
            rows = cur.fetchall()

            has_more = len(rows) > limit
            result_rows = rows[:limit]

            items = [
                {
                    "event_id": r["event_id"],
                    "source_id": r["source_id"],
                    "kind": r["kind"],
                    "start_at": r["start_at"],
                    "end_at": r["end_at"],
                    "media_revision": r["media_revision"],
                    "revision": r["revision"],
                    "capture_state": r["capture_state"],
                    "capture_error": r["capture_error"],
                }
                for r in result_rows
            ]

            next_cursor = None
            if has_more and result_rows:
                last = result_rows[-1]
                next_cursor = f"{last['start_at']}|{last['event_id']}"

            return items, next_cursor

    # -------------------------------------------------------------------------
    # Ingestion & Recording Intent Admission
    # -------------------------------------------------------------------------

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
    ) -> tuple[RecordedEvent, bool]:
        """Log an arbitrary neutral event without media capture requirement.

        Positive acceptance ACK returned immediately after durable journal write.
        Deduplicates per (source_id, idempotency_key) and verifies ACL/owner consistency.
        """
        validate_bounded_string(source_id, "source_id")
        validate_bounded_string(kind, "kind")
        validate_bounded_string(idempotency_key, "idempotency_key")
        validate_bounded_string(label, "label", max_len=500, allow_none=True)
        validate_payload_json(payload, "payload")
        validate_bounded_string(owner_user_id, "owner_user_id", allow_none=True)

        res, inserted, notify_tuple = await asyncio.to_thread(
            self._sync_log_event,
            source_id,
            kind,
            occurred_at,
            idempotency_key,
            label,
            payload,
            access_entities,
            owner_user_id,
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
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
    ) -> tuple[RecordedEvent, bool, tuple[str, int] | None]:
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
                if caller_entities != existing_entities or owner_user_id != existing["owner_user_id"]:
                    raise PermissionError("Access denied")

                cur.execute("SELECT * FROM events WHERE event_id = ?;", (existing["event_id"],))
                ev_row = cur.fetchone()
                return self._sync_hydrate_event(cur, ev_row), False, None

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
                        coverage_json, access_entities_json, owner_user_id,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 'ready', NULL, ?, ?, 1, 1, '{}', ?, ?, ?, ?);
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
                        now_str,
                        now_str,
                    ),
                )

                cur.execute(
                    """
                    INSERT INTO source_journal (
                        event_id, source_id, kind, occurred_at, idempotency_key,
                        label, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        event_id,
                        source_id,
                        kind,
                        dt_str,
                        idempotency_key,
                        label,
                        json.dumps(payload or {}),
                        now_str,
                    ),
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            ev = self._sync_hydrate_event(cur, cur.fetchone())
            return ev, True, (event_id, 1)

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
    ) -> tuple[RecordedEvent, bool, str | None]:
        """Admit a recording intent in the same transaction as the source event.

        Positive acceptance ACK happens immediately on durable intent write.
        Deduplicates per (source_id, idempotency_key). Rejects incompatible coalescing metadata.
        """
        validate_bounded_string(source_id, "source_id")
        validate_bounded_string(kind, "kind")
        validate_bounded_string(resource_key, "resource_key")
        validate_bounded_string(operation, "operation")
        validate_finite_number(window_seconds, "window_seconds", min_val=0.0, allow_zero=False)
        validate_bounded_string(idempotency_key, "idempotency_key")
        validate_finite_number(lookback_seconds, "lookback_seconds", min_val=0.0, allow_zero=True)
        validate_payload_json(recorder_config, "recorder_config")
        validate_bounded_string(label, "label", max_len=500, allow_none=True)
        validate_payload_json(payload, "payload")
        validate_bounded_string(owner_user_id, "owner_user_id", allow_none=True)

        res, inserted, intent_id, notify_tuple = await asyncio.to_thread(
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
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
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
    ) -> tuple[RecordedEvent, bool, str | None, tuple[str, int] | None]:
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
                if caller_entities != existing_entities or owner_user_id != existing_source["owner_user_id"]:
                    raise PermissionError("Access denied")

                cur.execute(
                    "SELECT * FROM events WHERE event_id = ?;",
                    (existing_source["event_id"],),
                )
                ev_row = cur.fetchone()
                return self._sync_hydrate_event(cur, ev_row), False, None, None

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
                    if active_ev_row and active_ev_row["capture_state"] in ("requested", "recording"):
                        active_event_id = session["active_event_id"]
                        active_ev = active_ev_row

                intent_id = str(uuid.uuid4())

                if active_event_id and active_ev is not None:
                    # Verify metadata compatibility before coalescing
                    active_access = (
                        tuple(sorted(set(json.loads(active_ev["access_entities_json"]))))
                        if active_ev["access_entities_json"]
                        else ()
                    )
                    caller_access = tuple(sorted(set(access_entities))) if access_entities else ()
                    if caller_access != active_access or owner_user_id != active_ev["owner_user_id"]:
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
                        (desired_str, dt_str, session_hold, window_seconds, now_str, resource_key),
                    )
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
                            coverage_json, access_entities_json, owner_user_id,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'requested', NULL, ?, 1, 1, '{}', ?, ?, ?, ?);
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
                            window_seconds,
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
                        label, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        target_event_id,
                        source_id,
                        kind,
                        dt_str,
                        idempotency_key,
                        label,
                        json.dumps(payload or {}),
                        now_str,
                    ),
                )

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
            ev = self._sync_hydrate_event(cur, cur.fetchone())
            return ev, True, intent_id, (target_event_id, ev.revision)

    async def lease_record_intents(
        self,
        worker_id: str,
        limit: int = 10,
        lease_seconds: float = 60.0,
    ) -> list[RecordingIntent]:
        """Lease pending recording intents for worker execution."""
        return await asyncio.to_thread(self._sync_lease_record_intents, worker_id, limit, lease_seconds)

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
                    recorder_config=json.loads(r["recorder_config_json"]) if r["recorder_config_json"] else {},
                    status="leased",
                    worker_id=worker_id,
                    leased_until=now_dt + timedelta(seconds=lease_seconds),
                    error=r["error"],
                    created_at=ensure_utc(r["created_at"]),
                    updated_at=now_dt,
                )
                for r in rows
            ]

    async def complete_record_intent(
        self,
        intent_id: str,
        worker_id: str,
        error: str | None = None,
    ) -> bool:
        """Mark a recording intent as completed or failed."""
        return await asyncio.to_thread(self._sync_complete_record_intent, intent_id, worker_id, error)

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

    def _sync_get_resource_session(self, resource_key: str) -> ResourceSession | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM resource_sessions WHERE resource_key = ?;", (resource_key,))
            row = cur.fetchone()
            if not row:
                return None
            return ResourceSession(
                resource_key=row["resource_key"],
                active_event_id=row["active_event_id"],
                last_activity_at=ensure_utc(row["last_activity_at"]),
                desired_capture_end=ensure_utc(row["desired_capture_end"]),
                hold=bool(row["hold"]),
                tail_window_seconds=row["tail_window_seconds"],
                recorder_config=json.loads(row["recorder_config_json"]) if row.get("recorder_config_json") else {},
                access_entities=tuple(json.loads(row["access_entities_json"])) if row.get("access_entities_json") else (),
                owner_user_id=row.get("owner_user_id"),
                updated_at=ensure_utc(row["updated_at"]),
            )

    # -------------------------------------------------------------------------
    # Lifecycle & Media Management
    # -------------------------------------------------------------------------

    async def update_capture_state(
        self,
        event_id: str,
        state: str,
        error_code: str | None = None,
    ) -> RecordedEvent:
        """Update event capture state and bump revisions.

        Idempotent: repeating the same state without error change results in NO revision bump.
        First finalization ('ready', 'partial') increments media_revision.
        Actual state/error mutation increments global revision.
        """
        ev, notify_tuple = await asyncio.to_thread(self._sync_update_capture_state, event_id, state, error_code)
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        return ev

    def _sync_update_capture_state(
        self,
        event_id: str,
        state: str,
        error_code: str | None,
    ) -> tuple[RecordedEvent, tuple[str, int] | None]:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        sanitized_error = sanitize_capture_error(error_code)

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            row = cur.fetchone()
            if not row:
                raise KeyError(f"Event {event_id} not found")

            current_state = row["capture_state"]
            current_error = row.get("capture_error")

            existing_coverage = json.loads(row["coverage_json"]) if row["coverage_json"] else {}
            if state == "ready" and (existing_coverage.get("is_uncertain") or current_state == "partial"):
                state = "partial"

            state_changed = (state != current_state)
            error_changed = (sanitized_error != current_error)

            if not state_changed and not error_changed:
                # Idempotent: no gratuitous bump or notification
                return self._sync_hydrate_event(cur, row), None

            cur.execute("BEGIN IMMEDIATE;")
            try:
                was_final = current_state in ("ready", "partial")
                is_final = state in ("ready", "partial")
                is_first_finalization = is_final and not was_final

                new_media_rev = row["media_revision"] + 1 if is_first_finalization else row["media_revision"]
                new_rev = row["revision"] + 1

                end_at_str = row["end_at"] or (now_str if state in ("ready", "partial", "failed", "interrupted", "expired") else None)

                cur.execute(
                    """
                    UPDATE events
                    SET capture_state = ?, capture_error = ?, end_at = ?, media_revision = ?, revision = ?, updated_at = ?
                    WHERE event_id = ?;
                    """,
                    (state, sanitized_error, end_at_str, new_media_rev, new_rev, now_str, event_id),
                )

                # Clear resource session if this event completed or stopped
                if state in ("ready", "partial", "failed", "interrupted", "expired"):
                    cur.execute(
                        "UPDATE resource_sessions SET active_event_id = NULL WHERE active_event_id = ?;",
                        (event_id,),
                    )

                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            ev = self._sync_hydrate_event(cur, cur.fetchone())
            return ev, (event_id, ev.revision)

    async def finalize_event(self, event_id: str) -> RecordedEvent:
        """Mark event capture ready idempotently."""
        return await self.update_capture_state(event_id, "ready")

    async def append_owned_media(
        self,
        event_id: str,
        kind: str,
        mime_type: str,
        file_name: str,
        duration: float | None = None,
        size_bytes: int = 0,
        content_hash: str | None = None,
        created_at: datetime | None = None,
    ) -> tuple[RecordedEvent, MediaItem]:
        validate_safe_id(event_id, "event_id")
        validate_bounded_string(kind, "kind")
        validate_bounded_string(mime_type, "mime_type")
        if duration is not None:
            validate_finite_number(duration, "duration", min_val=0.0, allow_zero=False)

        ev, media_item, notify_tuple = await asyncio.to_thread(
            self._sync_append_owned_media,
            event_id,
            kind,
            mime_type,
            file_name,
            duration,
            size_bytes,
            content_hash,
            created_at,
        )
        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        return ev, media_item

    def _sync_append_owned_media(
        self,
        event_id: str,
        kind: str,
        mime_type: str,
        file_name: str,
        duration: float | None,
        size_bytes: int,
        content_hash: str | None,
        created_at: datetime | None,
    ) -> tuple[RecordedEvent, MediaItem, tuple[str, int] | None]:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        media_created_dt = ensure_utc(created_at) if created_at else now_dt
        media_created_str = format_iso(media_created_dt) or now_str
        media_id = str(uuid.uuid4())

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            row = cur.fetchone()
            if not row:
                raise KeyError(f"Event {event_id} not found")

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    INSERT INTO media (
                        id, event_id, kind, mime_type, file_name, duration,
                        size_bytes, available, content_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?);
                    """,
                    (
                        media_id,
                        event_id,
                        kind,
                        mime_type,
                        file_name,
                        duration,
                        size_bytes,
                        content_hash,
                        media_created_str,
                    ),
                )

                # Compute actual_duration sum of validated video assets
                cur.execute(
                    "SELECT kind, duration FROM media WHERE event_id = ?;",
                    (event_id,),
                )
                all_media = cur.fetchall()
                actual_duration = round(
                    sum(
                        float(m["duration"])
                        for m in all_media
                        if m["kind"] == "video" and m["duration"] is not None
                    ),
                    4,
                )

                existing_coverage = (
                    json.loads(row["coverage_json"]) if row["coverage_json"] else {}
                )
                preserved_uncertain = bool(
                    existing_coverage.get("is_uncertain", False)
                    or row["capture_state"] == "partial"
                )

                new_coverage = {
                    "actual_duration": actual_duration,
                    "is_uncertain": preserved_uncertain,
                    "gaps": None if preserved_uncertain else 0,
                }
                new_coverage_json = json.dumps(new_coverage)

                new_media_rev = row["media_revision"] + 1
                new_rev = row["revision"] + 1
                cur.execute(
                    """
                    UPDATE events
                    SET media_revision = ?, revision = ?, coverage_json = ?, updated_at = ?
                    WHERE event_id = ?;
                    """,
                    (new_media_rev, new_rev, new_coverage_json, now_str, event_id),
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            ev = self._sync_hydrate_event(cur, cur.fetchone())

            media_item = MediaItem(
                id=media_id,
                kind=kind,
                mime_type=mime_type,
                file_name=file_name,
                duration=duration,
                size_bytes=size_bytes,
                available=True,
                content_hash=content_hash,
                created_at=now_dt,
            )
            return ev, media_item, (event_id, ev.revision)

    def _sync_verify_event_exists(self, event_id: str) -> None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT 1 FROM events WHERE event_id = ?;", (event_id,))
            if not cur.fetchone():
                raise KeyError(f"Event {event_id} not found")

    async def attach_and_promote_media(
        self,
        event_id: str,
        src_path: Path | str,
        kind: str | None = None,
        mime_type: str | None = None,
        duration: float | None = None,
        is_uncertain: bool = False,
        finalize: bool = True,
    ) -> tuple[RecordedEvent, MediaItem]:
        """Validate, stage, promote, and attach media asset to event.

        Strict path containment verification and media probe execution.
        Verifies event exists before any promotion; cleans orphan if DB append fails.
        Duration and MIME type come strictly from the actual probe.
        """
        validate_safe_id(event_id, "event_id")

        # 0. Verify event exists before any filesystem work
        await asyncio.to_thread(self._sync_verify_event_exists, event_id)

        # 1. Verify path containment off-loop
        resolved_src = await asyncio.to_thread(verify_media_path, src_path, self._allowed_media_roots)

        # 2. Execute probe validation off-loop
        if self._probe is None:
            raise ProbeUnavailableError("No media probe configured for store")

        probe_result = await asyncio.to_thread(self._probe.probe, resolved_src)
        if kind and kind != probe_result.kind:
            raise MediaTypeMismatchError(f"Expected kind '{kind}' but probe reported '{probe_result.kind}'")
        if mime_type and mime_type.lower() != probe_result.mime_type.lower():
            raise MediaTypeMismatchError(
                f"Expected MIME '{mime_type}' but probe reported '{probe_result.mime_type}'"
            )

        # Duration and MIME come strictly from actual probe - caller cannot fabricate override
        final_kind = probe_result.kind
        final_mime = probe_result.mime_type
        final_duration = probe_result.duration

        # 3. Off-loop atomic staging and promotion
        media_id = str(uuid.uuid4())
        ext = resolved_src.suffix or (".mp4" if final_kind == "video" else ".jpg")
        rel_file, size_bytes, content_hash = await asyncio.to_thread(
            stage_and_promote_media,
            resolved_src,
            self._media_root,
            event_id,
            media_id,
            ext,
        )

        # 4. Durable DB commit with safe orphan cleanup
        try:
            ev, media_item, notify_tuple = await asyncio.to_thread(
                self._sync_attach_media_commit,
                event_id,
                media_id,
                final_kind,
                final_mime,
                rel_file,
                final_duration,
                size_bytes,
                content_hash,
                is_uncertain,
                finalize,
            )
        except Exception:
            await asyncio.to_thread(unlink_promoted_media, self._media_root, rel_file)
            raise

        if notify_tuple:
            self._notify_subscribers(*notify_tuple)
        return ev, media_item

    def _sync_attach_media_commit(
        self,
        event_id: str,
        media_id: str,
        kind: str,
        mime_type: str,
        file_name: str,
        duration: float | None,
        size_bytes: int,
        content_hash: str,
        is_uncertain: bool,
        finalize: bool,
    ) -> tuple[RecordedEvent, MediaItem, tuple[str, int] | None]:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            row = cur.fetchone()
            if not row:
                raise KeyError(f"Event {event_id} not found")

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    INSERT INTO media (
                        id, event_id, kind, mime_type, file_name, duration,
                        size_bytes, available, content_hash, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?);
                    """,
                    (
                        media_id,
                        event_id,
                        kind,
                        mime_type,
                        file_name,
                        duration,
                        size_bytes,
                        content_hash,
                        now_str,
                    ),
                )

                # Compute actual_duration sum of validated video assets
                cur.execute(
                    "SELECT kind, duration FROM media WHERE event_id = ?;",
                    (event_id,),
                )
                all_media = cur.fetchall()
                actual_duration = round(
                    sum(
                        float(m["duration"])
                        for m in all_media
                        if m["kind"] == "video" and m["duration"] is not None
                    ),
                    4,
                )

                existing_coverage = (
                    json.loads(row["coverage_json"]) if row["coverage_json"] else {}
                )
                preserved_uncertain = bool(
                    existing_coverage.get("is_uncertain", False)
                    or is_uncertain
                    or row["capture_state"] == "partial"
                )

                new_coverage = {
                    "actual_duration": actual_duration,
                    "is_uncertain": preserved_uncertain,
                    "gaps": None if preserved_uncertain else 0,
                }
                new_coverage_json = json.dumps(new_coverage)

                new_media_rev = row["media_revision"] + 1
                new_rev = row["revision"] + 1

                new_state = row["capture_state"]
                end_at_str = row["end_at"]
                if finalize:
                    new_state = "partial" if preserved_uncertain else "ready"
                    if not end_at_str:
                        end_at_str = now_str
                    cur.execute(
                        "UPDATE resource_sessions SET active_event_id = NULL WHERE active_event_id = ?;",
                        (event_id,),
                    )

                cur.execute(
                    """
                    UPDATE events
                    SET capture_state = ?, end_at = ?, media_revision = ?, revision = ?, coverage_json = ?, updated_at = ?
                    WHERE event_id = ?;
                    """,
                    (new_state, end_at_str, new_media_rev, new_rev, new_coverage_json, now_str, event_id),
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            ev = self._sync_hydrate_event(cur, cur.fetchone())

            media_item = MediaItem(
                id=media_id,
                kind=kind,
                mime_type=mime_type,
                file_name=file_name,
                duration=duration,
                size_bytes=size_bytes,
                available=True,
                content_hash=content_hash,
                created_at=now_dt,
            )
            return ev, media_item, (event_id, ev.revision)

    # -------------------------------------------------------------------------
    # Media Pins & Leases
    # -------------------------------------------------------------------------

    async def pin_media_lease(
        self,
        event_id: str,
        lease_seconds: float = 300.0,
        reason: str | None = None,
    ) -> MediaPinLease | None:
        """Acquire a bounded media pin preventing retention deletion.

        TTL is clamped to a maximum of 300.0 seconds.
        """
        validate_safe_id(event_id, "event_id")
        validate_finite_number(lease_seconds, "lease_seconds", min_val=0.0, allow_zero=False)
        validate_bounded_string(reason, "reason", max_len=500, allow_none=True)
        return await asyncio.to_thread(self._sync_pin_media_lease, event_id, lease_seconds, reason)

    async def pin(
        self,
        event_id: str,
        lease_seconds: float = 300.0,
        reason: str | None = None,
    ) -> MediaPinLease | None:
        """Alias for pin_media_lease."""
        return await self.pin_media_lease(event_id, lease_seconds, reason)

    def _sync_pin_media_lease(
        self,
        event_id: str,
        lease_seconds: float,
        reason: str | None,
    ) -> MediaPinLease | None:
        clamped_seconds = max(1.0, min(float(lease_seconds), 300.0))
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        expires_dt = now_dt + timedelta(seconds=clamped_seconds)
        expires_str = format_iso(expires_dt) or ""

        pin_id = str(uuid.uuid4())
        lease_token = secrets.token_urlsafe(32)

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT 1 FROM events WHERE event_id = ?;", (event_id,))
            if not cur.fetchone():
                return None

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    INSERT INTO media_pins (pin_id, event_id, lease_token, expires_at, reason, created_at)
                    VALUES (?, ?, ?, ?, ?, ?);
                    """,
                    (pin_id, event_id, lease_token, expires_str, reason, now_str),
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            return MediaPinLease(
                pin_id=pin_id,
                event_id=event_id,
                lease_token=lease_token,
                expires_at=expires_dt,
                reason=reason,
                created_at=now_dt,
            )

    async def release_media_pin(self, lease_token: str) -> bool:
        """Release a media pin lease using its opaque token."""
        return await asyncio.to_thread(self._sync_release_media_pin, lease_token)

    async def release(self, lease_token: str) -> bool:
        """Alias for release_media_pin."""
        return await self.release_media_pin(lease_token)

    def _sync_release_media_pin(self, lease_token: str) -> bool:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute("DELETE FROM media_pins WHERE lease_token = ?;", (lease_token,))
                released = cur.rowcount > 0
                cur.execute("COMMIT;")
                return released
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    # -------------------------------------------------------------------------
    # Generic CAS Annotations
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
                    # Terminal updates require unexpired matching generation then clear lease
                    # and become immutable until reserve creates NEW generation
                    new_reserved_by = None
                    new_reserved_until = None
                else:
                    # Nonterminal updates KEEP active reservation/expiry
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
    # Retention & Pruning
    # -------------------------------------------------------------------------

    async def enforce_retention(
        self,
        max_bytes: int | None = None,
        max_age_days: int | None = None,
    ) -> RetentionResult:
        """Enforce retention policies protecting active bounded pins and captures.

        Expired media files are unlinked; source journals remain strictly preserved.
        """
        res, notify_list = await asyncio.to_thread(self._sync_enforce_retention, max_bytes, max_age_days)
        for ev_id, rev in notify_list:
            self._notify_subscribers(ev_id, rev)
        return res

    async def retention(
        self,
        max_bytes: int | None = None,
        max_age_days: int | None = None,
    ) -> RetentionResult:
        """Alias for enforce_retention."""
        return await self.enforce_retention(max_bytes, max_age_days)

    def _sync_enforce_retention(
        self,
        max_bytes: int | None,
        max_age_days: int | None,
    ) -> tuple[RetentionResult, list[tuple[str, int]]]:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        deleted_files = 0
        freed_bytes = 0
        marked_expired = 0
        marked_missing = 0
        errors: list[str] = []
        notifications: list[tuple[str, int]] = []

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            # Active pins and active captures are protected
            cur.execute("SELECT DISTINCT event_id FROM media_pins WHERE expires_at > ?;", (now_str,))
            pinned_ids = {r["event_id"] for r in cur.fetchall()}

            cur.execute(
                "SELECT event_id FROM events WHERE capture_state IN ('requested', 'recording');"
            )
            recording_ids = {r["event_id"] for r in cur.fetchall()}

            protected_ids = pinned_ids | recording_ids

            cur.execute("SELECT * FROM media WHERE available = 1 ORDER BY created_at ASC;")
            all_media = cur.fetchall()

            total_size = sum(m["size_bytes"] for m in all_media)
            cutoff_dt = now_dt - timedelta(days=max_age_days) if max_age_days is not None else None

            candidates_to_prune: list[dict[str, Any]] = []

            for m in all_media:
                if m["event_id"] in protected_ids:
                    continue

                m_created = ensure_utc(m["created_at"])
                age_expired = cutoff_dt is not None and m_created < cutoff_dt
                size_expired = max_bytes is not None and total_size > max_bytes

                if age_expired or size_expired:
                    candidates_to_prune.append(m)
                    total_size -= m["size_bytes"]

            if not candidates_to_prune:
                return RetentionResult(), []

            cur.execute("BEGIN IMMEDIATE;")
            try:
                for m in candidates_to_prune:
                    file_path = (self._media_root / m["file_name"]).resolve()

                    # Symlink directory escape guard
                    try:
                        if not file_path.is_relative_to(self._media_root):
                            errors.append(f"Security error: path escape detected for {file_path}")
                            continue
                    except Exception as err:
                        errors.append(f"Path verification error for {file_path}: {err}")
                        continue

                    # Unlink file if present
                    unlink_success = False
                    if file_path.exists():
                        try:
                            file_path.unlink()
                            unlink_success = True
                            deleted_files += 1
                            freed_bytes += m["size_bytes"]
                        except Exception as err:
                            errors.append(f"Failed to unlink {file_path}: {err}")
                    else:
                        unlink_success = True
                        marked_missing += 1

                    if unlink_success:
                        cur.execute("UPDATE media SET available = 0 WHERE id = ?;", (m["id"],))

                        # Check if event has any available media left
                        cur.execute(
                            "SELECT 1 FROM media WHERE event_id = ? AND available = 1 LIMIT 1;",
                            (m["event_id"],),
                        )
                        if not cur.fetchone():
                            cur.execute(
                                """
                                UPDATE events
                                SET capture_state = 'expired', media_revision = media_revision + 1,
                                    revision = revision + 1, updated_at = ?
                                WHERE event_id = ?;
                                """,
                                (now_str, m["event_id"]),
                            )
                            marked_expired += 1
                        else:
                            cur.execute(
                                """
                                UPDATE events
                                SET media_revision = media_revision + 1, revision = revision + 1, updated_at = ?
                                WHERE event_id = ?;
                                """,
                                (now_str, m["event_id"]),
                            )

                        cur.execute(
                            "SELECT revision FROM events WHERE event_id = ?;",
                            (m["event_id"],),
                        )
                        rev_row = cur.fetchone()
                        if rev_row:
                            notifications.append((m["event_id"], rev_row["revision"]))

                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

        return (
            RetentionResult(
                deleted_files=deleted_files,
                freed_bytes=freed_bytes,
                marked_expired_events=marked_expired,
                marked_missing_media=marked_missing,
                errors=errors,
            ),
            notifications,
        )

    # -------------------------------------------------------------------------
    # Reconciliation & Crash Recovery
    # -------------------------------------------------------------------------

    async def reconcile(self, worker_id: str = "reconciler") -> ReconcileResult:
        """Recover cold-start ownership and clean orphans before capture workers start.

        Call only when this store has no active capture writers. The HA coordinator
        also defers this operation while its shared registry has quarantined writers.
        """
        return await asyncio.to_thread(self._sync_reconcile, worker_id)

    def _sync_reconcile(self, worker_id: str) -> ReconcileResult:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        stale_threshold_str = format_iso(now_dt - timedelta(seconds=30)) or ""

        interrupted_events = 0
        expired_sessions = 0
        reconciled_intents = 0
        orphaned_files = 0

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # A dispatched capture has no surviving owner after a process restart,
                # even when its requested end is still in the future. Undispatched
                # pending/leased intents remain available for the new worker.
                cur.execute(
                    """
                    SELECT e.* FROM events e
                    WHERE e.capture_state IN ('requested', 'recording')
                      AND (e.desired_capture_end < ? OR NOT EXISTS (
                          SELECT 1 FROM recording_intents ri
                          WHERE ri.event_id=e.event_id AND ri.status IN ('pending', 'leased')
                      ));
                    """,
                    (stale_threshold_str,),
                )
                stale_evs = cur.fetchall()
                for ev in stale_evs:
                    cur.execute(
                        "SELECT COUNT(*) AS clips FROM media WHERE event_id=? AND available=1;",
                        (ev["event_id"],),
                    )
                    has_media = bool(cur.fetchone()["clips"])
                    coverage = json.loads(ev["coverage_json"] or "{}")
                    if has_media:
                        coverage.update(is_uncertain=True, gaps=None)
                    cur.execute(
                        """
                        UPDATE events
                        SET capture_state = ?, capture_error = 'RECORDING_INTERRUPTED',
                            end_at = ?, media_revision = media_revision + ?,
                            coverage_json = ?, revision = revision + 1, updated_at = ?
                        WHERE event_id = ?;
                        """,
                        (
                            "partial" if has_media else "interrupted",
                            min(now_str, ev["desired_capture_end"]), int(has_media),
                            json.dumps(coverage), now_str, ev["event_id"],
                        ),
                    )
                    interrupted_events += 1

                # 2. Clear expired sessions
                cur.execute(
                    """
                    UPDATE resource_sessions
                    SET active_event_id = NULL, updated_at = ?
                    WHERE active_event_id IS NOT NULL AND (
                        desired_capture_end < ? OR active_event_id IN (
                            SELECT event_id FROM events
                            WHERE capture_state NOT IN ('requested', 'recording')
                        )
                    );
                    """,
                    (now_str, stale_threshold_str),
                )
                expired_sessions = cur.rowcount

                # Cold startup has no surviving dispatcher lease owner.
                cur.execute(
                    """
                    UPDATE recording_intents
                    SET status = 'pending', worker_id = NULL, leased_until = NULL, updated_at = ?
                    WHERE status = 'leased';
                    """,
                    (now_str,),
                )
                reconciled_intents = cur.rowcount

                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            # 4. Clean orphaned files in owned media root
            cur.execute("SELECT file_name FROM media WHERE available = 1;")
            indexed_rel_files = {Path(r["file_name"]).as_posix() for r in cur.fetchall()}

            staging_dir = (self._media_root / ".staging").resolve()

            for path in self._media_root.rglob("*"):
                if path.is_file():
                    try:
                        resolved = path.resolve()
                        if not resolved.is_relative_to(self._media_root):
                            continue
                        if staging_dir in resolved.parents or resolved == staging_dir:
                            # Clean temporary staging files older than 1 hour
                            mtime = path.stat().st_mtime
                            if (now_dt.timestamp() - mtime) > 3600:
                                path.unlink()
                            continue

                        rel_posix = path.relative_to(self._media_root).as_posix()
                        if rel_posix not in indexed_rel_files:
                            path.unlink()
                            orphaned_files += 1
                    except Exception as err:
                        _LOGGER.warning("Error inspecting/cleaning file %s: %s", path, err)

        return ReconcileResult(
            interrupted_events=interrupted_events,
            expired_sessions=expired_sessions,
            reconciled_intents=reconciled_intents,
            orphaned_files_cleaned=orphaned_files,
        )
