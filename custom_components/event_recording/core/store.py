"""Durable SQLite JournalStore for Event Recording.

Strict boundary: Zero Home Assistant imports, zero AI imports.
Thread-safe single-writer off-loop execution with WAL, synchronous=FULL,
atomic intent admission, media-set revisioning, and CAS annotations.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import secrets
import sqlite3
import threading
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
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
    Incident,
    IncidentInputs,
    MediaItem,
    MediaPinLease,
    ReconcileResult,
    RecordedEvent,
    RecordingIntent,
    ResourceSession,
    RetentionResult,
    SourceEventRecord,
    canonical_annotation_payload,
    canonical_json_dumps,
    ensure_utc,
    format_iso,
    sanitize_capture_error,
    utc_now,
    validate_bounded_string,
    validate_correlation_id,
    validate_finite_number,
    validate_payload_json,
    validate_safe_id,
)
from .storage_ledger import (
    InFlightReservation,
    StorageCapacityError,
    StorageLedger,
    StoragePolicy,
)

_LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = 3
MAX_PIN_LEASE_SECONDS = 900.0
DEFAULT_PIN_LEASE_SECONDS = 300.0


@dataclass(slots=True)
class CommitEffects:
    """Encapsulates post-commit side effects executed safely on the event loop."""

    result: Any
    unlinked_bytes: int = 0
    unlinked_count: int = 0
    incident_notifications: list[tuple[str, int]] = field(default_factory=list)


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
        self._incident_subscribers: set[Callable[[str, int], None]] = set()
        self._active_promotion_futures: set[asyncio.Future[Any]] = set()
        self.ledger = StorageLedger()
        self._storage_policy: StoragePolicy = StoragePolicy(min_disk_free_mb=50, retention_size_mb=None)

    @property
    def storage_policy(self) -> StoragePolicy:
        with self._lock:
            return self._storage_policy

    def set_storage_policy(self, policy: StoragePolicy) -> None:
        with self._lock:
            self._storage_policy = policy

    async def reserve_in_flight(
        self,
        reservation_id: str,
        target_path: Path,
        estimated_bytes: int,
        sidecar_paths: tuple[Path, ...] = (),
    ) -> InFlightReservation:
        """Atomically check admission and reserve storage for an in-flight capture off-loop."""
        return await asyncio.to_thread(
            self._sync_reserve_in_flight,
            reservation_id,
            target_path,
            estimated_bytes,
            sidecar_paths,
        )

    def _sync_reserve_in_flight(
        self,
        reservation_id: str,
        target_path: Path,
        estimated_bytes: int,
        sidecar_paths: tuple[Path, ...] = (),
    ) -> InFlightReservation:
        with self._lock:
            return self.ledger.check_and_reserve(
                reservation_id=reservation_id,
                target_path=target_path,
                estimated_bytes=estimated_bytes,
                media_root=self._media_root,
                policy=self._storage_policy,
                sidecar_paths=sidecar_paths,
            )

    async def reconcile_storage_accounting(self) -> dict[str, int]:
        """Perform periodic storage-only accounting reconciliation off-loop.

        Unlike cold-start reconcile(), this NEVER alters event states, NEVER marks
        recordings interrupted, and NEVER cleans staging files of active writers.
        """
        return await asyncio.to_thread(self._sync_reconcile_storage_accounting)

    def _sync_reconcile_storage_accounting(self) -> dict[str, int]:
        with self._lock:
            assert self._conn is not None
            return self.ledger.reconcile(self._conn, self._media_root)

    @property
    def storage_ledger(self) -> StorageLedger:
        """Return the maintained storage ledger."""
        return self.ledger

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

    def subscribe_incidents(self, callback: Callable[[str, int], None]) -> Callable[[], None]:
        """Subscribe to post-commit incident revision updates.

        Dispatched outside the SQLite lock on the event loop.
        """
        self._incident_subscribers.add(callback)

        def _unsub() -> None:
            self._incident_subscribers.discard(callback)

        return _unsub

    def _notify_incident_subscribers(self, incident_id: str, revision: int) -> None:
        """Dispatch revision notification to all registered incident subscribers."""
        for cb in list(self._incident_subscribers):
            try:
                cb(incident_id, revision)
            except Exception as err:
                _LOGGER.warning("Error notifying incident subscriber for %s: %s", incident_id, err)

    def _create_pre_upgrade_backup(self, from_version: int, to_version: int) -> Path:
        """Create a consistent SQLite.backup of pre-upgrade database outside owned media tree.

        Atomic fresh per-upgrade-attempt backup tempfile + integrity/schema checks + rename.
        Fail-closed: if backup creation fails, upgrade aborts prior to any schema mutation.
        """
        assert self._conn is not None
        backup_dir = (self._db_path.parent / "event_recording_backups").resolve()
        backup_dir.mkdir(parents=True, exist_ok=True)
        temp_file = (backup_dir / f"event_recording_v{from_version}_pre_v{to_version}_{uuid.uuid4().hex[:8]}.tmp.db").resolve()
        final_file = (backup_dir / f"event_recording_v{from_version}_pre_v{to_version}.db").resolve()

        try:
            dest_conn = sqlite3.connect(str(temp_file))
            try:
                self._conn.backup(dest_conn)
            finally:
                dest_conn.close()

            # Verify integrity and schema version of the backup file before committing it
            chk_conn = sqlite3.connect(str(temp_file))
            try:
                chk_cur = chk_conn.cursor()
                chk_cur.execute("PRAGMA integrity_check;")
                row = chk_cur.fetchone()
                val = row[0] if isinstance(row, tuple) else (row["integrity_check"] if isinstance(row, dict) else row)
                if val != "ok":
                    raise RuntimeError(f"Backup failed SQLite integrity check: {val}")

                chk_cur.execute("SELECT MAX(version) FROM schema_info;")
                v_row = chk_cur.fetchone()
                b_ver = v_row[0] if isinstance(v_row, tuple) else (v_row["MAX(version)"] if isinstance(v_row, dict) else v_row)
                if b_ver is None or int(b_ver) < from_version:
                    raise RuntimeError(f"Backup schema version mismatch: {b_ver} < {from_version}")
            finally:
                chk_conn.close()

            # Atomic replace into final backup target
            temp_file.replace(final_file)
            return final_file
        except Exception as err:
            try:
                if temp_file.exists():
                    temp_file.unlink(missing_ok=True)
            except Exception:
                pass
            _LOGGER.error("Pre-upgrade backup failed for v%s -> v%s: %s", from_version, to_version, err)
            raise RuntimeError(f"PRE_UPGRADE_BACKUP_FAILED: {err}") from err

    def get_backup_status(self) -> dict[str, Any]:
        """Safely expose pre-upgrade backup status without leaking private filesystem paths."""
        backup_dir = self._db_path.parent / "event_recording_backups"
        if not backup_dir.is_dir():
            return {"backup_available": False, "backup_count": 0}
        backups = [p for p in backup_dir.glob("*.db") if p.is_file() and not p.name.endswith(".tmp.db")]
        return {
            "backup_available": len(backups) > 0,
            "backup_count": len(backups),
        }

    async def async_get_backup_status(self) -> dict[str, Any]:
        """Safely expose pre-upgrade backup status off the event loop."""
        return await asyncio.to_thread(self.get_backup_status)

    async def open(self) -> None:
        """Open database connection and execute migrations off event loop."""
        await asyncio.to_thread(self._sync_open)

    async def close(self) -> None:
        """Close database connection off event loop after settling active promotions."""
        if self._active_promotion_futures:
            await asyncio.gather(
                *[asyncio.shield(f) for f in list(self._active_promotion_futures)],
                return_exceptions=True,
            )
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
            self.ledger.reconcile(conn, self._media_root)
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

        # Upgrade protection: before own SQLite schema mutation (open migrations)
        # create consistent SQLite.backup of PRE-UPGRADE database outside owned media retention tree,
        # once per schema change/version, keep old copy unmodified.
        # Existing new-empty DB does not need backup.
        # Backup fail means upgrade fails closed prior ALTER.
        if 0 < current_version < 3:
            self._create_pre_upgrade_backup(current_version, 3)

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
                        requested_start TEXT,
                        requested_end TEXT,
                        capture_start TEXT,
                        capture_end TEXT,
                        time_quality TEXT,
                        clock_source TEXT,
                        time_uncertainty_seconds REAL,
                        start_offset_seconds REAL,
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

        if current_version < 2:
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute("PRAGMA table_info(media);")
                existing_cols = {col["name"] for col in cur.fetchall()}
                columns_to_add = [
                    ("requested_start", "TEXT"),
                    ("requested_end", "TEXT"),
                    ("capture_start", "TEXT"),
                    ("capture_end", "TEXT"),
                    ("time_quality", "TEXT"),
                    ("clock_source", "TEXT"),
                    ("time_uncertainty_seconds", "REAL"),
                    ("start_offset_seconds", "REAL"),
                ]
                for col_name, col_type in columns_to_add:
                    if col_name not in existing_cols:
                        cur.execute(f"ALTER TABLE media ADD COLUMN {col_name} {col_type};")
                cur.execute(
                    "INSERT INTO schema_info (version, applied_at) VALUES (?, ?);",
                    (2, format_iso(utc_now()) or ""),
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

        if current_version < 3:
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # Additive columns on existing tables
                cur.execute("PRAGMA table_info(events);")
                event_cols = {col["name"] for col in cur.fetchall()}
                if "correlation_id" not in event_cols:
                    cur.execute("ALTER TABLE events ADD COLUMN correlation_id TEXT;")

                cur.execute("PRAGMA table_info(source_journal);")
                journal_cols = {col["name"] for col in cur.fetchall()}
                if "correlation_id" not in journal_cols:
                    cur.execute("ALTER TABLE source_journal ADD COLUMN correlation_id TEXT;")

                # Additive tables for incidents (many-to-many)
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS incidents (
                        incident_id TEXT PRIMARY KEY,
                        anchor_at TEXT NOT NULL,
                        span_start TEXT NOT NULL,
                        span_end TEXT NOT NULL,
                        membership_revision INTEGER NOT NULL DEFAULT 1,
                        reason TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS incident_memberships (
                        incident_id TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        added_at TEXT NOT NULL,
                        reason TEXT,
                        PRIMARY KEY (incident_id, event_id),
                        FOREIGN KEY (incident_id) REFERENCES incidents (incident_id) ON DELETE CASCADE,
                        FOREIGN KEY (event_id) REFERENCES events (event_id) ON DELETE CASCADE
                    );
                    """
                )
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS incident_annotations (
                        incident_id TEXT NOT NULL,
                        namespace TEXT NOT NULL,
                        generation INTEGER NOT NULL DEFAULT 1,
                        input_token TEXT NOT NULL,
                        input_namespace TEXT NOT NULL,
                        profile_digest TEXT,
                        payload_json TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'ready',
                        worker_id TEXT,
                        reserved_until TEXT,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL,
                        PRIMARY KEY (incident_id, namespace),
                        FOREIGN KEY (incident_id) REFERENCES incidents (incident_id) ON DELETE CASCADE
                    );
                    """
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_incidents_anchor ON incidents(anchor_at DESC, incident_id DESC);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_incidents_span ON incidents(span_start DESC, span_end DESC);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_memberships_event ON incident_memberships(event_id);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_correlation ON events(correlation_id);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_journal_correlation ON source_journal(correlation_id);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_incident_annotations_lookup ON incident_annotations(incident_id, namespace);"
                )

                cur.execute(
                    "INSERT INTO schema_info (version, applied_at) VALUES (?, ?);",
                    (3, format_iso(utc_now()) or ""),
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
            SELECT id, source_id, kind, occurred_at, idempotency_key, label, payload_json, correlation_id, created_at
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
                correlation_id=r.get("correlation_id"),
                created_at=ensure_utc(r["created_at"]),
            )
            for r in source_rows
        )

        # Hydrate media items
        cur.execute(
            """
            SELECT id, kind, mime_type, file_name, duration, size_bytes, available, content_hash, created_at,
                   requested_start, requested_end, capture_start, capture_end,
                   time_quality, clock_source, time_uncertainty_seconds, start_offset_seconds
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
                requested_start=ensure_utc(m["requested_start"]) if m.get("requested_start") else None,
                requested_end=ensure_utc(m["requested_end"]) if m.get("requested_end") else None,
                capture_start=ensure_utc(m["capture_start"]) if m.get("capture_start") else None,
                capture_end=ensure_utc(m["capture_end"]) if m.get("capture_end") else None,
                time_quality=m.get("time_quality"),
                clock_source=m.get("clock_source"),
                time_uncertainty_seconds=m.get("time_uncertainty_seconds"),
                start_offset_seconds=m.get("start_offset_seconds"),
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
            correlation_id=event_row.get("correlation_id"),
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
                requested_start=ensure_utc(row["requested_start"]) if row.get("requested_start") else None,
                requested_end=ensure_utc(row["requested_end"]) if row.get("requested_end") else None,
                capture_start=ensure_utc(row["capture_start"]) if row.get("capture_start") else None,
                capture_end=ensure_utc(row["capture_end"]) if row.get("capture_end") else None,
                time_quality=row.get("time_quality"),
                clock_source=row.get("clock_source"),
                time_uncertainty_seconds=row.get("time_uncertainty_seconds"),
                start_offset_seconds=row.get("start_offset_seconds"),
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

            annotation_exclusion = filters.get("exclude_current_complete_annotation")
            if annotation_exclusion is not None:
                namespace = annotation_exclusion["namespace"]
                field_path = f'$.{annotation_exclusion["field"]}'
                encoded_value = json.dumps(annotation_exclusion["equals"], allow_nan=False)
                clauses.append(
                    """
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
        correlation_id: str | None = None,
        auto_association_config: dict[str, Any] | None = None,
        **extra_kwargs: Any,
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
                if caller_entities != existing_entities or owner_user_id != existing["owner_user_id"]:
                    raise PermissionError("Access denied")

                cur.execute("SELECT * FROM events WHERE event_id = ?;", (existing["event_id"],))
                ev_row = cur.fetchone()
                return self._sync_hydrate_event(cur, ev_row), False, None, None

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
                    _, _, inc_notify = self._sync_create_or_join_incident(
                        cur, correlation_id, event_id, reason="correlation_id"
                    )
                elif auto_association_config and auto_association_config.get("enabled", False):
                    auto_inc = self._sync_evaluate_auto_association(
                        cur,
                        event_id,
                        correlation_group=auto_association_config.get("correlation_group"),
                        zone=auto_association_config.get("zone"),
                        adjacent_zones=auto_association_config.get("adjacent_zones"),
                        source_clock_offset_seconds=float(auto_association_config.get("source_clock_offset_seconds", 0.0)),
                        source_time_uncertainty_seconds=float(auto_association_config.get("source_time_uncertainty_seconds", 0.0)),
                        global_tolerance_seconds=float(auto_association_config.get("global_tolerance_seconds", 10.0)),
                        max_incident_span_seconds=float(auto_association_config.get("max_incident_span_seconds", 300.0)),
                    )
                    if auto_inc:
                        inc_notify = (auto_inc.incident_id, auto_inc.membership_revision)

                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            ev = self._sync_hydrate_event(cur, cur.fetchone())
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
        validate_finite_number(tail_window_seconds, "tail_window_seconds", min_val=0.0, allow_zero=True)
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
                if caller_entities != existing_entities or owner_user_id != existing_source["owner_user_id"]:
                    raise PermissionError("Access denied")

                cur.execute(
                    "SELECT * FROM events WHERE event_id = ?;",
                    (existing_source["event_id"],),
                )
                ev_row = cur.fetchone()
                return self._sync_hydrate_event(cur, ev_row), False, None, None, None

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
                inc_notify = None

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
                        (desired_str, dt_str, session_hold, tail_window_seconds, now_str, resource_key),
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
                        _, _, inc_notify = self._sync_create_or_join_incident(
                            cur, correlation_id, target_event_id, reason="correlation_id"
                        )
                    elif auto_association_config and auto_association_config.get("enabled", False):
                        auto_inc = self._sync_evaluate_auto_association(
                            cur,
                            target_event_id,
                            correlation_group=auto_association_config.get("correlation_group"),
                            zone=auto_association_config.get("zone"),
                            adjacent_zones=auto_association_config.get("adjacent_zones"),
                            source_clock_offset_seconds=float(auto_association_config.get("source_clock_offset_seconds", 0.0)),
                            source_time_uncertainty_seconds=float(auto_association_config.get("source_time_uncertainty_seconds", 0.0)),
                            global_tolerance_seconds=float(auto_association_config.get("global_tolerance_seconds", 10.0)),
                            max_incident_span_seconds=float(auto_association_config.get("max_incident_span_seconds", 300.0)),
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
                        _, _, inc_notify = self._sync_create_or_join_incident(
                            cur, correlation_id, target_event_id, reason="correlation_id"
                        )
                    elif auto_association_config and auto_association_config.get("enabled", False):
                        auto_inc = self._sync_evaluate_auto_association(
                            cur,
                            target_event_id,
                            correlation_group=auto_association_config.get("correlation_group"),
                            zone=auto_association_config.get("zone"),
                            adjacent_zones=auto_association_config.get("adjacent_zones"),
                            source_clock_offset_seconds=float(auto_association_config.get("source_clock_offset_seconds", 0.0)),
                            source_time_uncertainty_seconds=float(auto_association_config.get("source_time_uncertainty_seconds", 0.0)),
                            global_tolerance_seconds=float(auto_association_config.get("global_tolerance_seconds", 10.0)),
                            max_incident_span_seconds=float(auto_association_config.get("max_incident_span_seconds", 300.0)),
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
            ev = self._sync_hydrate_event(cur, cur.fetchone())
            return ev, True, intent_id, (target_event_id, ev.revision), inc_notify

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
        requested_start: datetime | None = None,
        requested_end: datetime | None = None,
        capture_start: datetime | None = None,
        capture_end: datetime | None = None,
        time_quality: str | None = None,
        clock_source: str | None = None,
        time_uncertainty_seconds: float | None = None,
        start_offset_seconds: float | None = None,
        reservation_id: str | None = None,
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
            requested_start,
            requested_end,
            capture_start,
            capture_end,
            time_quality,
            clock_source,
            time_uncertainty_seconds,
            start_offset_seconds,
            reservation_id,
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
        requested_start: datetime | None = None,
        requested_end: datetime | None = None,
        capture_start: datetime | None = None,
        capture_end: datetime | None = None,
        time_quality: str | None = None,
        clock_source: str | None = None,
        time_uncertainty_seconds: float | None = None,
        start_offset_seconds: float | None = None,
        reservation_id: str | None = None,
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

            if reservation_id is None and size_bytes > 0:
                self.ledger.check_admission(
                    required_bytes=size_bytes,
                    media_root=self._media_root,
                    policy=self._storage_policy,
                )
            elif reservation_id is not None:
                self.ledger.check_and_admit_reservation(
                    reservation_id=reservation_id,
                    target_path=self._media_root / file_name,
                    actual_bytes=size_bytes,
                    media_root=self._media_root,
                    policy=self._storage_policy,
                )

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    INSERT INTO media (
                        id, event_id, kind, mime_type, file_name, duration,
                        size_bytes, available, content_hash, created_at,
                        requested_start, requested_end, capture_start, capture_end,
                        time_quality, clock_source, time_uncertainty_seconds, start_offset_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                        format_iso(requested_start),
                        format_iso(requested_end),
                        format_iso(capture_start),
                        format_iso(capture_end),
                        time_quality,
                        clock_source,
                        time_uncertainty_seconds,
                        start_offset_seconds,
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
                self.ledger.commit_media(reservation_id=reservation_id, final_bytes=size_bytes)
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
                requested_start=ensure_utc(requested_start) if requested_start else None,
                requested_end=ensure_utc(requested_end) if requested_end else None,
                capture_start=ensure_utc(capture_start) if capture_start else None,
                capture_end=ensure_utc(capture_end) if capture_end else None,
                time_quality=time_quality,
                clock_source=clock_source,
                time_uncertainty_seconds=time_uncertainty_seconds,
                start_offset_seconds=start_offset_seconds,
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
        requested_start: datetime | None = None,
        requested_end: datetime | None = None,
        capture_start: datetime | None = None,
        capture_end: datetime | None = None,
        time_quality: str | None = None,
        clock_source: str | None = None,
        time_uncertainty_seconds: float | None = None,
        start_offset_seconds: float | None = None,
        reservation_id: str | None = None,
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

        # 2.5 Common storage admission check and single reservation owner
        def _sync_admit_and_reserve() -> tuple[str, bool]:
            with self._lock:
                src_size = resolved_src.stat().st_size
                if reservation_id:
                    self.ledger.check_and_admit_reservation(
                        reservation_id=reservation_id,
                        target_path=resolved_src,
                        actual_bytes=src_size,
                        media_root=self._media_root,
                        policy=self._storage_policy,
                    )
                    return reservation_id, False
                else:
                    import_res_id = f"import_{event_id}_{uuid.uuid4().hex[:6]}"
                    self.ledger.check_and_reserve(
                        reservation_id=import_res_id,
                        target_path=resolved_src,
                        estimated_bytes=src_size,
                        media_root=self._media_root,
                        policy=self._storage_policy,
                    )
                    return import_res_id, True

        effective_res_id, is_new_import_reservation = await asyncio.to_thread(_sync_admit_and_reserve)

        cancel_event = threading.Event()
        worker_state: dict[str, Any] = {
            "rel_file": None,
            "commit_succeeded": False,
            "error": None,
            "result": None,
        }

        def _worker_stage_and_commit() -> None:
            try:
                # 3. Off-loop atomic staging and promotion
                media_id = str(uuid.uuid4())
                ext = resolved_src.suffix or (".mp4" if final_kind == "video" else ".jpg")
                rel_file, size_bytes, content_hash = stage_and_promote_media(
                    resolved_src,
                    self._media_root,
                    event_id,
                    media_id,
                    ext,
                )
                worker_state["rel_file"] = rel_file

                if cancel_event.is_set():
                    unlink_promoted_media(self._media_root, rel_file)
                    return

                # 4. Durable DB commit with single promotion owner
                ev, media_item, notify_tuple = self._sync_attach_media_commit(
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
                    requested_start,
                    requested_end,
                    capture_start,
                    capture_end,
                    time_quality,
                    clock_source,
                    time_uncertainty_seconds,
                    start_offset_seconds,
                    effective_res_id,
                )
                worker_state["commit_succeeded"] = True
                worker_state["result"] = (ev, media_item, notify_tuple)
            except Exception as exc:
                worker_state["error"] = exc
                if worker_state["rel_file"]:
                    try:
                        unlink_promoted_media(self._media_root, worker_state["rel_file"])
                    except Exception:
                        pass
                raise
            finally:
                if not worker_state["commit_succeeded"]:
                    if is_new_import_reservation and effective_res_id:
                        self.ledger.release_reservation(effective_res_id, force=True)

        loop = asyncio.get_running_loop()
        worker_fut = loop.run_in_executor(None, _worker_stage_and_commit)
        self._active_promotion_futures.add(worker_fut)
        worker_fut.add_done_callback(lambda f: self._active_promotion_futures.discard(f))
        try:
            await asyncio.shield(worker_fut)
        except asyncio.CancelledError:
            cancel_event.set()
            try:
                await asyncio.shield(worker_fut)
            except Exception:
                pass
            if worker_state["commit_succeeded"]:
                ev, media_item, notify_tuple = worker_state["result"]
                if notify_tuple:
                    self._notify_subscribers(notify_tuple[0], notify_tuple[1])
                    if len(notify_tuple) > 2:
                        for inc_id, m_rev in notify_tuple[2]:
                            self._notify_incident_subscribers(inc_id, m_rev)
            raise

        if worker_state["error"] is not None:
            raise worker_state["error"]

        if not worker_state["commit_succeeded"]:
            raise StorageCapacityError("Staging or promotion failed to complete")

        ev, media_item, notify_tuple = worker_state["result"]
        if notify_tuple:
            self._notify_subscribers(notify_tuple[0], notify_tuple[1])
            if len(notify_tuple) > 2:
                for inc_id, m_rev in notify_tuple[2]:
                    self._notify_incident_subscribers(inc_id, m_rev)
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
        requested_start: datetime | None = None,
        requested_end: datetime | None = None,
        capture_start: datetime | None = None,
        capture_end: datetime | None = None,
        time_quality: str | None = None,
        clock_source: str | None = None,
        time_uncertainty_seconds: float | None = None,
        start_offset_seconds: float | None = None,
        reservation_id: str | None = None,
    ) -> tuple[RecordedEvent, MediaItem, tuple[str, int, list[tuple[str, int]]] | None]:
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
                        size_bytes, available, content_hash, created_at,
                        requested_start, requested_end, capture_start, capture_end,
                        time_quality, clock_source, time_uncertainty_seconds, start_offset_seconds
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
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
                        format_iso(requested_start),
                        format_iso(requested_end),
                        format_iso(capture_start),
                        format_iso(capture_end),
                        time_quality,
                        clock_source,
                        time_uncertainty_seconds,
                        start_offset_seconds,
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

                cur.execute(
                    "SELECT incident_id FROM incident_memberships WHERE event_id = ?;",
                    (event_id,),
                )
                incident_ids = [r["incident_id"] for r in cur.fetchall()]
                incident_notifications: list[tuple[str, int]] = []
                for inc_id in incident_ids:
                    self._sync_recompute_incident_span(cur, inc_id)
                    cur.execute(
                        "SELECT membership_revision FROM incidents WHERE incident_id = ?;",
                        (inc_id,),
                    )
                    rev_row = cur.fetchone()
                    m_rev = rev_row["membership_revision"] if rev_row else 1
                    incident_notifications.append((inc_id, m_rev))

                cur.execute("COMMIT;")
                self.ledger.commit_media(reservation_id=reservation_id, final_bytes=size_bytes)
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
                requested_start=ensure_utc(requested_start) if requested_start else None,
                requested_end=ensure_utc(requested_end) if requested_end else None,
                capture_start=ensure_utc(capture_start) if capture_start else None,
                capture_end=ensure_utc(capture_end) if capture_end else None,
                time_quality=time_quality,
                clock_source=clock_source,
                time_uncertainty_seconds=time_uncertainty_seconds,
                start_offset_seconds=start_offset_seconds,
            )
            return ev, media_item, (event_id, ev.revision, incident_notifications)

    # -------------------------------------------------------------------------
    # Media Pins & Leases
    # -------------------------------------------------------------------------

    async def pin_media_lease(
        self,
        event_id: str,
        lease_seconds: float = DEFAULT_PIN_LEASE_SECONDS,
        reason: str | None = None,
    ) -> MediaPinLease | None:
        """Acquire a bounded media pin preventing retention deletion.

        TTL is clamped to a maximum of 900.0 seconds (15 minutes).
        """
        validate_safe_id(event_id, "event_id")
        validate_finite_number(lease_seconds, "lease_seconds", min_val=0.0, allow_zero=False)
        validate_bounded_string(reason, "reason", max_len=500, allow_none=True)
        return await asyncio.to_thread(self._sync_pin_media_lease, event_id, lease_seconds, reason)

    async def pin(
        self,
        event_id: str,
        lease_seconds: float = DEFAULT_PIN_LEASE_SECONDS,
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
        clamped_seconds = max(1.0, min(float(lease_seconds), MAX_PIN_LEASE_SECONDS))
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
    # Retention & Storage Accounting
    # -------------------------------------------------------------------------

    async def get_storage_stats(self) -> dict[str, int]:
        """Return committed media bytes and count from SQLite off-loop."""
        return await asyncio.to_thread(self._sync_get_storage_stats)

    def _sync_get_storage_stats(self) -> dict[str, int]:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                "SELECT COALESCE(SUM(size_bytes), 0) AS total_bytes, COUNT(*) AS count "
                "FROM media WHERE available = 1;"
            )
            row = cur.fetchone()
            total_bytes = int(row["total_bytes"]) if row and row["total_bytes"] is not None else 0
            count = int(row["count"]) if row and row["count"] is not None else 0
            return {"committed_bytes": total_bytes, "media_count": count}

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

                        # Derived previews must expire with their source media. They are
                        # disposable and regenerated for the next media revision.
                        thumbs = self._media_root / "thumbnails"
                        if thumbs.is_dir() and not thumbs.is_symlink():
                            for thumb in thumbs.iterdir():
                                if (
                                    thumb.name.startswith(f"{m['event_id']}_r")
                                    and thumb.name.endswith("_thumb.jpg")
                                    and not thumb.is_symlink()
                                    and thumb.resolve().is_relative_to(self._media_root.resolve())
                                ):
                                    try:
                                        thumb.unlink(missing_ok=True)
                                    except OSError:
                                        errors.append("Failed to remove derived thumbnail")

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
                self.ledger.record_unlinked(freed_bytes, count=deleted_files)
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

    # -------------------------------------------------------------------------
    # Incident Membership & Safe Grouped Retrieval
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
                                # Active unexpired reservation held by another worker
                                cur.execute("ROLLBACK;")
                                return None
                            # An explicit retry by this worker supersedes its
                            # previous generation. Automatic notification
                            # deduplication belongs to the consumer queue.

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

                # CAS requires status == 'reserved' (terminal rows cannot be rewritten with same generation)
                if row["status"] != "reserved":
                    cur.execute("ROLLBACK;")
                    return False, None

                # CAS requires unexpired reservation lease
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
                    inc = self._sync_hydrate_incident(cur, iid)
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

            cur.execute("SELECT file_name, size_bytes FROM media WHERE event_id = ?;", (event_id,))
            media_files = [(r["file_name"], r["size_bytes"] or 0) for r in cur.fetchall()]

            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute("DELETE FROM events WHERE event_id = ?;", (event_id,))
                for inc_id in inc_ids:
                    cur.execute(
                        "UPDATE incidents SET membership_revision = membership_revision + 1, updated_at = ? WHERE incident_id = ?;",
                        (now_str, inc_id),
                    )
                    self._sync_recompute_incident_span(cur, inc_id)
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
                # Transitive chain merge prevention!
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
