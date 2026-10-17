"""Database connection, pre-upgrade backups, and schema migrations for JournalStore.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
Preserves SQLite v3 schema, pre-upgrade backup isolation, and WAL durability contracts.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import uuid
from pathlib import Path
from typing import Any

from ..models import format_iso, utc_now
from .common import JournalStoreBase, _dict_factory

_LOGGER = logging.getLogger(__name__)


class MigrationsMixin(JournalStoreBase):
    """Encapsulates database lifecycle, backups, and schema upgrades."""

    def _create_pre_upgrade_backup(self, from_version: int, to_version: int) -> Path:
        """Create a consistent SQLite.backup of pre-upgrade database outside owned media tree.

        Atomic fresh per-upgrade-attempt backup tempfile + integrity/schema checks + rename.
        Fail-closed: if backup creation fails, upgrade aborts prior to any schema mutation.
        """
        assert self._conn is not None
        backup_dir = (self._db_path.parent / "event_recording_backups").resolve()
        backup_dir.mkdir(parents=True, exist_ok=True)
        temp_file = (
            backup_dir / f"event_recording_v{from_version}_pre_v{to_version}_{uuid.uuid4().hex[:8]}.tmp.db"
        ).resolve()
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
                val = (
                    row[0]
                    if isinstance(row, tuple)
                    else (row["integrity_check"] if isinstance(row, dict) else row)
                )
                if val != "ok":
                    raise RuntimeError(f"Backup failed SQLite integrity check: {val}")

                chk_cur.execute("SELECT MAX(version) FROM schema_info;")
                v_row = chk_cur.fetchone()
                b_ver = (
                    v_row[0]
                    if isinstance(v_row, tuple)
                    else (v_row["MAX(version)"] if isinstance(v_row, dict) else v_row)
                )
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
            _LOGGER.error(
                "Pre-upgrade backup failed for v%s -> v%s: %s", from_version, to_version, err
            )
            raise RuntimeError(f"PRE_UPGRADE_BACKUP_FAILED: {err}") from err

    def get_backup_status(self) -> dict[str, Any]:
        """Safely expose pre-upgrade backup status without leaking private filesystem paths."""
        backup_dir = self._db_path.parent / "event_recording_backups"
        if not backup_dir.is_dir():
            return {"backup_available": False, "backup_count": 0}
        backups = [
            p for p in backup_dir.glob("*.db") if p.is_file() and not p.name.endswith(".tmp.db")
        ]
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
            self._db_path = self._db_path.resolve()
            self._media_root = self._media_root.resolve()
            self._allowed_media_roots = [self._media_root] + [
                p.resolve() for p in self._allowed_media_roots if p != self._media_root
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
        """Execute SQLite migrations idempotently up to SCHEMA_VERSION."""
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
        # create consistent SQLite.backup of PRE-UPGRADE database outside owned media retention tree.
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

                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_start ON events(start_at DESC, event_id DESC);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_events_capture_state ON events(capture_state);"
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_events_source ON events(source_id);")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_journal_event_id ON source_journal(event_id);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_journal_source_idemp ON source_journal(source_id, idempotency_key);"
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_media_event ON media(event_id);")
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_intents_status ON recording_intents(status, leased_until);"
                )
                cur.execute(
                    "CREATE INDEX IF NOT EXISTS idx_pins_expiry ON media_pins(event_id, expires_at);"
                )

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
