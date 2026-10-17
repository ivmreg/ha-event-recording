"""Media queries, staging, capacity reservations, and promotion for JournalStore.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
Preserves streaming SHA-256 calculation, atomic rename promotion, and storage admission invariants.
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from ..media import (
    MediaTypeMismatchError,
    PathTraversalSecurityError,
    ProbeUnavailableError,
    stage_and_promote_media,
    unlink_promoted_media,
    verify_media_path,
)
from ..models import (
    MediaItem,
    RecordedEvent,
    ensure_utc,
    format_iso,
    utc_now,
    validate_bounded_string,
    validate_finite_number,
    validate_safe_id,
)
from ..storage_ledger import InFlightReservation, StorageCapacityError
from .common import JournalStoreBase

_LOGGER = logging.getLogger(__name__)


def _safe_stage_and_promote_media(*args: Any, **kwargs: Any) -> Any:
    """Stage and promote media, respecting dynamic module monkeypatching on core.store."""
    import sys

    store_mod = sys.modules.get("custom_components.event_recording.core.store")
    if store_mod is not None and hasattr(store_mod, "stage_and_promote_media"):
        return store_mod.stage_and_promote_media(*args, **kwargs)
    return stage_and_promote_media(*args, **kwargs)


def _safe_unlink_promoted_media(*args: Any, **kwargs: Any) -> Any:
    """Unlink promoted media, respecting dynamic module monkeypatching on core.store."""
    import sys

    store_mod = sys.modules.get("custom_components.event_recording.core.store")
    if store_mod is not None and hasattr(store_mod, "unlink_promoted_media"):
        return store_mod.unlink_promoted_media(*args, **kwargs)
    return unlink_promoted_media(*args, **kwargs)


class MediaMixin(JournalStoreBase):
    """Encapsulates media lookup, staging reservations, and atomic promotion."""

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
        """Perform periodic storage-only accounting reconciliation off-loop."""
        return await asyncio.to_thread(self._sync_reconcile_storage_accounting)

    def _sync_reconcile_storage_accounting(self) -> dict[str, int]:
        with self._lock:
            assert self._conn is not None
            return self.ledger.reconcile(self._conn, self._media_root)

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

    async def list_finalized_media(
        self,
        cursor: str | None = None,
        limit: int = 50,
        namespace_lacking: str | None = None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Startup catchup / poll query for retained ready events with available media."""
        return await asyncio.to_thread(
            self._sync_list_finalized_media, cursor, limit, namespace_lacking
        )

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

    def _sync_verify_event_exists(self, event_id: str) -> None:
        """Verify event exists in SQLite before executing any filesystem mutations."""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("SELECT 1 FROM events WHERE event_id = ?;", (event_id,))
            if not cur.fetchone():
                raise KeyError(f"Event {event_id} not found")

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
                new_rev = row["revision"] + 1
                new_media_rev = row["media_revision"] + 1
                cur.execute(
                    "UPDATE events SET revision = ?, media_revision = ?, updated_at = ? WHERE event_id = ?;",
                    (new_rev, new_media_rev, now_str, event_id),
                )
                cur.execute("COMMIT;")
                self.ledger.commit_media(reservation_id=reservation_id, final_bytes=size_bytes)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

            cur.execute("SELECT * FROM events WHERE event_id = ?;", (event_id,))
            ev = getattr(self, "_sync_hydrate_event")(cur, cur.fetchone())

            media_item = MediaItem(
                id=media_id,
                kind=kind,
                mime_type=mime_type,
                file_name=file_name,
                duration=duration,
                size_bytes=size_bytes,
                available=True,
                content_hash=content_hash,
                created_at=media_created_dt,
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
        adopt_staging: bool = False,
    ) -> tuple[RecordedEvent, MediaItem]:
        """Validate, stage, promote, and attach media asset to event."""
        validate_safe_id(event_id, "event_id")

        # 0. Verify event exists before any filesystem work
        await asyncio.to_thread(self._sync_verify_event_exists, event_id)

        raw_src_path = Path(src_path)
        if adopt_staging and raw_src_path.is_symlink():
            raise PathTraversalSecurityError("Cannot adopt symlinked staging file")

        # 1. Verify path containment off-loop
        resolved_src = await asyncio.to_thread(
            verify_media_path, src_path, self._allowed_media_roots
        )

        # 2. Execute probe validation off-loop
        if self._probe is None:
            raise ProbeUnavailableError("No media probe configured for store")

        probe_result = await asyncio.to_thread(self._probe.probe, resolved_src)
        if kind and kind != probe_result.kind:
            raise MediaTypeMismatchError(
                f"Expected kind '{kind}' but probe reported '{probe_result.kind}'"
            )
        if mime_type and mime_type.lower() != probe_result.mime_type.lower():
            raise MediaTypeMismatchError(
                f"Expected MIME '{mime_type}' but probe reported '{probe_result.mime_type}'"
            )

        final_kind = probe_result.kind
        final_mime = probe_result.mime_type
        final_duration = probe_result.duration

        # 2.5 Common storage admission check and single reservation owner
        def _sync_admit_and_reserve() -> tuple[str, bool]:
            with self._lock:
                src_size = resolved_src.stat().st_size
                if reservation_id:
                    res = self.ledger.get_reservation(reservation_id)
                    if res and res.is_quarantined:
                        raise StorageCapacityError(
                            f"Cannot promote quarantined reservation: {reservation_id}"
                        )
                    self.ledger.check_and_admit_reservation(
                        reservation_id=reservation_id,
                        target_path=resolved_src,
                        actual_bytes=src_size,
                        media_root=self._media_root,
                        policy=self._storage_policy,
                    )
                    return reservation_id, False
                else:
                    import_res_id = (
                        f"adopt_{event_id}_{uuid.uuid4().hex[:6]}"
                        if adopt_staging
                        else f"import_{event_id}_{uuid.uuid4().hex[:6]}"
                    )
                    self.ledger.check_and_reserve(
                        reservation_id=import_res_id,
                        target_path=resolved_src,
                        estimated_bytes=src_size,
                        media_root=self._media_root,
                        policy=self._storage_policy,
                    )
                    return import_res_id, True

        effective_res_id, is_new_import_reservation = await asyncio.to_thread(
            _sync_admit_and_reserve
        )

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
                stage_src = raw_src_path if adopt_staging else resolved_src
                rel_file, size_bytes, content_hash = _safe_stage_and_promote_media(
                    stage_src,
                    self._media_root,
                    event_id,
                    media_id,
                    ext,
                    adopt_staging=adopt_staging,
                )
                worker_state["rel_file"] = rel_file

                if cancel_event.is_set():
                    _safe_unlink_promoted_media(self._media_root, rel_file)
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
                        _safe_unlink_promoted_media(self._media_root, worker_state["rel_file"])
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
                    (
                        new_state,
                        end_at_str,
                        new_media_rev,
                        new_rev,
                        new_coverage_json,
                        now_str,
                        event_id,
                    ),
                )

                cur.execute(
                    "SELECT incident_id FROM incident_memberships WHERE event_id = ?;",
                    (event_id,),
                )
                incident_ids = [r["incident_id"] for r in cur.fetchall()]
                incident_notifications: list[tuple[str, int]] = []
                for inc_id in incident_ids:
                    getattr(self, "_sync_recompute_incident_span")(cur, inc_id)
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
            ev = getattr(self, "_sync_hydrate_event")(cur, cur.fetchone())

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
