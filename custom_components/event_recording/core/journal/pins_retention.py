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
    format_iso,
    validate_bounded_string,
    validate_finite_number,
    validate_safe_id,
)
from .common import (
    DEFAULT_PIN_LEASE_SECONDS,
    MAX_PIN_LEASE_SECONDS,
    JournalStoreBase,
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
        now_dt = self.now()
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

            cur.execute("SELECT 1 FROM media_deletions WHERE event_id = ? LIMIT 1;", (event_id,))
            if cur.fetchone():
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
        task = asyncio.create_task(
            asyncio.to_thread(self._sync_enforce_retention, max_bytes, max_age_days)
        )
        self._active_promotion_futures.add(task)
        task.add_done_callback(self._active_promotion_futures.discard)
        try:
            res, notify_list = await asyncio.shield(task)
        except asyncio.CancelledError:
            # Repeated shutdown cancellation must not cancel the thread's owner:
            # close() waits for this task before closing SQLite.
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
            _result, notifications = task.result()
            for event_id, revision in notifications:
                self._notify_subscribers(event_id, revision)
            raise
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
        """Claim a bounded batch durably, unlink off-lock, then settle accounting.

        Pin acquisition and claims serialize on the journal lock. Once claimed,
        media is unavailable and new pins on that event fail until deletion settles.
        Interrupted/failed unlinks remain in media_deletions for the next sweep.
        """
        with self._retention_lock:
            claims, notifications, expired = self._sync_claim_retention(max_bytes, max_age_days)
            deleted = missing = freed = 0
            errors: list[str] = []
            settled: list[dict[str, Any]] = []
            for claim in claims:
                try:
                    target = (self._media_root / claim["file_name"]).resolve()
                    if not target.is_relative_to(self._media_root):
                        raise ValueError("Media deletion path escapes owned root")
                    try:
                        target.unlink()
                        deleted += 1
                        freed += claim["size_bytes"]
                    except FileNotFoundError:
                        missing += 1
                    settled.append(claim)
                except (OSError, ValueError) as err:
                    errors.append(f"Failed to unlink media: {type(err).__name__}")

            # Enumerate derived previews once, regardless of the number of clips.
            affected = {claim["event_id"] for claim in settled}
            thumbs = self._media_root / "thumbnails"
            if affected and thumbs.is_dir() and not thumbs.is_symlink():
                for thumb in thumbs.iterdir():
                    event_id, sep, _revision = thumb.name.rpartition("_r")
                    if sep and event_id in affected and thumb.name.endswith("_thumb.jpg"):
                        try:
                            if not thumb.is_symlink() and thumb.resolve().is_relative_to(
                                self._media_root
                            ):
                                thumb.unlink(missing_ok=True)
                        except OSError:
                            errors.append("Failed to remove derived thumbnail")

            with self._lock:
                assert self._conn is not None
                self._conn.execute("BEGIN IMMEDIATE;")
                try:
                    uncharged_bytes = uncharged_count = 0
                    for claim in settled:
                        cur = self._conn.execute(
                            "DELETE FROM media_deletions WHERE media_id = ?;", (claim["media_id"],)
                        )
                        if cur.rowcount:
                            uncharged_bytes += claim["size_bytes"]
                            uncharged_count += 1
                    self._conn.execute("COMMIT;")
                    self.ledger.record_unlinked(uncharged_bytes, count=uncharged_count)
                except Exception:
                    self._conn.execute("ROLLBACK;")
                    raise
            return RetentionResult(
                deleted_files=deleted,
                freed_bytes=freed,
                marked_expired_events=expired,
                marked_missing_media=missing,
                errors=errors,
            ), notifications

    def _sync_claim_retention(
        self,
        max_bytes: int | None,
        max_age_days: int | None,
        limit: int = 100,
    ) -> tuple[list[dict[str, Any]], list[tuple[str, int]], int]:
        now = self.now()
        now_str = format_iso(now)
        cutoff = (
            format_iso(now - timedelta(days=max_age_days)) if max_age_days is not None else None
        )
        notifications: list[tuple[str, int]] = []
        expired = 0
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    "SELECT * FROM media_deletions ORDER BY created_at, media_id LIMIT ?;", (limit,)
                )
                claims = cur.fetchall()
                remaining = limit - len(claims)
                cur.execute(
                    "SELECT COALESCE(SUM(size_bytes), 0) AS total FROM media WHERE available = 1;"
                )
                total = cur.fetchone()["total"]
                if remaining and (
                    cutoff is not None or (max_bytes is not None and total > max_bytes)
                ):
                    cur.execute(
                        """
                        SELECT m.* FROM media m JOIN events e ON e.event_id = m.event_id
                        WHERE m.available = 1 AND e.capture_state NOT IN ('requested', 'recording')
                          AND NOT EXISTS (SELECT 1 FROM media_pins p
                              WHERE p.event_id = m.event_id AND p.expires_at > ?)
                          AND (? IS NULL OR m.created_at < ? OR (? IS NOT NULL AND ? > ?))
                        ORDER BY m.created_at, m.id LIMIT ?;
                    """,
                        (now_str, cutoff, cutoff, max_bytes, total, max_bytes, remaining),
                    )
                    candidates = cur.fetchall()
                    affected: set[str] = set()
                    for media in candidates:
                        if not (
                            (cutoff is not None and media["created_at"] < cutoff)
                            or (max_bytes is not None and total > max_bytes)
                        ):
                            break
                        cur.execute(
                            "INSERT INTO media_deletions VALUES (?, ?, ?, ?, ?);",
                            (
                                media["id"],
                                media["event_id"],
                                media["file_name"],
                                media["size_bytes"],
                                now_str,
                            ),
                        )
                        cur.execute("UPDATE media SET available = 0 WHERE id = ?;", (media["id"],))
                        claims.append(
                            dict(
                                media_id=media["id"],
                                event_id=media["event_id"],
                                file_name=media["file_name"],
                                size_bytes=media["size_bytes"],
                            )
                        )
                        affected.add(media["event_id"])
                        total -= media["size_bytes"]
                    for event_id in affected:
                        cur.execute(
                            "SELECT 1 FROM media WHERE event_id = ? AND available = 1 LIMIT 1;",
                            (event_id,),
                        )
                        exhausted = cur.fetchone() is None
                        cur.execute(
                            """UPDATE events SET
                            capture_state = CASE WHEN ? THEN 'expired' ELSE capture_state END,
                            media_revision = media_revision + 1, revision = revision + 1, updated_at = ?
                            WHERE event_id = ? RETURNING revision;""",
                            (exhausted, now_str, event_id),
                        )
                        notifications.append((event_id, cur.fetchone()["revision"]))
                        expired += int(exhausted)
                cur.execute(
                    "DELETE FROM media_pins WHERE pin_id IN (SELECT pin_id FROM media_pins WHERE expires_at <= ? LIMIT 1000);",
                    (now_str,),
                )
                cur.execute("COMMIT;")
            except Exception:
                cur.execute("ROLLBACK;")
                raise
        return claims, notifications, expired

    async def reconcile(self, worker_id: str = "reconciler") -> ReconcileResult:
        """Recover cold-start ownership and clean orphans before capture workers start."""
        result = await asyncio.to_thread(self._sync_reconcile, worker_id)
        await self.enforce_retention()  # Retry claims left by an interrupted unlink.
        return result

    def _sync_reconcile(self, worker_id: str) -> ReconcileResult:
        now_dt = self.now()
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
            cur.execute(
                "SELECT file_name FROM media WHERE available = 1 UNION SELECT file_name FROM media_deletions;"
            )
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
