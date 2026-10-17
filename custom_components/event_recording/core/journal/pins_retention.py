"""Media pin leases, retention policy enforcement, and crash recovery for JournalStore.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
Preserves pin lease TTL ceilings (900s), retention deletion guards, and orphan file reconciliation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from ..models import (
    MediaPinLease,
    ReconcileResult,
    RetentionResult,
    ensure_utc,
    format_iso,
    validate_bounded_string,
    validate_finite_number,
    validate_safe_id,
)
from .common import (
    DEFAULT_PIN_LEASE_SECONDS,
    MAX_PIN_LEASE_SECONDS,
    JournalStoreBase,
    _safe_utc_now,
)

_LOGGER = logging.getLogger(__name__)


class PinsRetentionMixin(JournalStoreBase):
    """Encapsulates media retention, pin lease protection, and startup crash recovery."""

    async def pin_media_lease(
        self,
        event_id: str,
        lease_seconds: float = DEFAULT_PIN_LEASE_SECONDS,
        reason: str | None = None,
    ) -> MediaPinLease | None:
        """Acquire a bounded media pin preventing retention deletion."""
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
        now_dt = _safe_utc_now()
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

    async def enforce_retention(
        self,
        max_bytes: int | None = None,
        max_age_days: int | None = None,
    ) -> RetentionResult:
        """Enforce retention policies protecting active bounded pins and captures."""
        res, notify_list = await asyncio.to_thread(
            self._sync_enforce_retention, max_bytes, max_age_days
        )
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
        now_dt = _safe_utc_now()
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
            cur.execute(
                "SELECT DISTINCT event_id FROM media_pins WHERE expires_at > ?;", (now_str,)
            )
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

                        # Derived previews must expire with their source media
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

    async def reconcile(self, worker_id: str = "reconciler") -> ReconcileResult:
        """Recover cold-start ownership and clean orphans before capture workers start."""
        return await asyncio.to_thread(self._sync_reconcile, worker_id)

    def _sync_reconcile(self, worker_id: str) -> ReconcileResult:
        now_dt = _safe_utc_now()
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
                            min(now_str, ev["desired_capture_end"]),
                            int(has_media),
                            json.dumps(coverage),
                            now_str,
                            ev["event_id"],
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
