"""Capture execution and adapter dispatch for Event Recording coordinator.

Coordinates per-resource capture sessions, slot concurrency acquisition,
adapter dispatch (native camera, snapshot, buffered provider), durable
ownership revalidation each chunk, and quarantine-aware staging cleanup.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

from .const import (
    DEFAULT_EXPORT_DELAY_SECONDS,
    ERROR_BUFFER_UNAVAILABLE,
    ERROR_EXPIRED_WINDOW,
    ERROR_QUEUE_OVERFLOW,
    ERROR_QUEUE_TIMEOUT,
    ERROR_RESOURCE_QUARANTINED,
    ERROR_STORAGE_CAPACITY_EXCEEDED,
    MODE_BUFFERED,
    MODE_EXTEND,
    MODE_NATIVE_FIXED,
    MODE_NATIVE_SNAPSHOT,
    TIME_QUALITY_PROVIDER_REPORTED,
    TIME_QUALITY_UNKNOWN,
)
from .core.models import RecordedEvent, ResourceSession, sanitize_capture_error, utc_now
from .core.storage_ledger import StorageCapacityError
from .core.store import JournalStore
from .recorder import (
    CameraRecorderAdapter,
    FrigateBufferedAdapter,
    RecorderError,
    RecorderRegistry,
    ResourceQuarantinedError,
    SnapshotRecorderAdapter,
)

_LOGGER = logging.getLogger(__name__)

# Nominal bytes per second of camera capture (1 MiB/s unmeasured nominal allocation)
ESTIMATED_BYTES_PER_SECOND = 1024 * 1024


def _safe_utc_now() -> Any:
    """Return UTC now, respecting dynamic module monkeypatching on coordinator."""
    import sys

    coord_mod = sys.modules.get("custom_components.event_recording.coordinator")
    if coord_mod is not None and hasattr(coord_mod, "utc_now"):
        return coord_mod.utc_now()
    return utc_now()


def _get_frigate_buffered_adapter() -> Any:
    """Return FrigateBufferedAdapter class, respecting dynamic module patching on coordinator."""
    import sys

    coord_mod = sys.modules.get("custom_components.event_recording.coordinator")
    if coord_mod is not None and hasattr(coord_mod, "FrigateBufferedAdapter"):
        return coord_mod.FrigateBufferedAdapter
    return FrigateBufferedAdapter


class CaptureExecutor:
    """Coordinates capture task execution, slot semaphore acquisition, and adapter dispatch."""

    def __init__(self, coordinator: Any) -> None:
        self.coordinator = coordinator

    @property
    def hass(self) -> HomeAssistant:
        return self.coordinator.hass

    @property
    def store(self) -> JournalStore:
        return self.coordinator.store

    @property
    def registry(self) -> RecorderRegistry:
        return self.coordinator.registry

    @property
    def adapter(self) -> CameraRecorderAdapter:
        return self.coordinator.adapter

    @property
    def snapshot_adapter(self) -> SnapshotRecorderAdapter:
        return self.coordinator.snapshot_adapter

    @property
    def slot_manager(self) -> Any:
        return self.coordinator._slot_manager

    async def finish_capture(
        self, event_id: str, valid_clips_count: int, error: str | None
    ) -> None:
        """Conclude capture state adhering strictly to invariant: zero clips CANNOT be ready."""
        _LOGGER.debug(
            "Finishing capture for event %s: valid_clips=%d, error=%s",
            event_id,
            valid_clips_count,
            error,
        )
        if valid_clips_count == 0:
            terminal_error = error or "ZERO_CLIPS_RECORDED"
            await self.store.update_capture_state(event_id, "failed", error_code=terminal_error)
        elif error:
            await self.store.update_capture_state(event_id, "partial", error_code=error)
        else:
            await self.store.finalize_event(event_id)

    async def run_resource_capture(self, resource_key: str) -> None:
        """Serialize capture for a specific resource under dynamic slot concurrency."""
        session = await self.store.get_resource_session(resource_key)
        if not session or not session.active_event_id:
            return

        event_id = session.active_event_id
        session_config = dict(session.recorder_config or {})
        configured_adapter = session_config.get("adapter")
        if configured_adapter not in (None, "camera"):
            await self.store.update_capture_state(
                event_id, "failed", error_code="UNSUPPORTED_ADAPTER"
            )
            await self.store.clear_resource_session(resource_key)
            return

        profile_snapshot = dict(session_config.get("_event_recording_profile") or {})
        recording_mode = (
            profile_snapshot.get("recording_mode")
            or session_config.get("recording_mode")
            or MODE_EXTEND
        )
        ownership_group = profile_snapshot.get("ownership_group") or session_config.get(
            "ownership_group"
        )
        _LOGGER.debug(
            "Starting run_resource_capture for %s (event_id=%s, mode=%s, ownership_group=%s)",
            resource_key,
            event_id,
            recording_mode,
            ownership_group,
        )

        is_expired, deadline_seconds, scene_deadline = (
            self.coordinator.session_coordinator.evaluate_queue_deadline(
                session=session,
                recording_mode=recording_mode,
                queue_wait_timeout_seconds=self.coordinator.queue_wait_timeout_seconds,
            )
        )
        if is_expired:
            self.coordinator.expired_captures_count += 1
            await self.store.update_capture_state(
                event_id, "failed", error_code=ERROR_EXPIRED_WINDOW
            )
            await self.store.clear_resource_session(resource_key)
            return

        from .coordinator import QueueOverflowError

        try:
            acquired = await self.slot_manager.acquire(timeout_seconds=deadline_seconds)
        except QueueOverflowError:
            self.coordinator.queue_overflow_count += 1
            await self.store.update_capture_state(
                event_id, "failed", error_code=ERROR_QUEUE_OVERFLOW
            )
            await self.store.clear_resource_session(resource_key)
            return

        if not acquired:
            now_post = _safe_utc_now()
            if (
                recording_mode != MODE_BUFFERED
                and not session.hold
                and now_post >= session.desired_capture_end
            ):
                self.coordinator.expired_captures_count += 1
                err_code = ERROR_EXPIRED_WINDOW
            else:
                self.coordinator.queue_timeouts_count += 1
                err_code = ERROR_QUEUE_TIMEOUT

            await self.store.update_capture_state(event_id, "failed", error_code=err_code)
            await self.store.clear_resource_session(resource_key)
            return

        valid_clips_count = 0
        last_error: str | None = None
        is_first_chunk = True

        try:
            # Re-read session and event after awaiting slot
            fresh_session = await self.store.get_resource_session(resource_key)
            if not fresh_session or not fresh_session.active_event_id:
                return
            session = fresh_session
            event_id = session.active_event_id

            event: RecordedEvent | None = await self.store.get_event(event_id)
            if not event:
                await self.store.clear_resource_session(resource_key)
                return
            valid_clips_count = sum(1 for media in event.media if media.available)

            now = _safe_utc_now()
            if (
                recording_mode != MODE_BUFFERED
                and not session.hold
                and now >= session.desired_capture_end
            ):
                self.coordinator.expired_captures_count += 1
                await self.store.update_capture_state(
                    event_id, "failed", error_code=ERROR_EXPIRED_WINDOW
                )
                await self.store.clear_resource_session(resource_key)
                return

            capture_start = now
            max_window_seconds = min(
                300.0,
                max(
                    1.0,
                    float(
                        profile_snapshot.get(
                            "max_window_seconds", self.coordinator.max_window_seconds
                        )
                    ),
                ),
            )
            max_end_time = capture_start + timedelta(seconds=max_window_seconds)

            if event.capture_state == "requested":
                await self.store.update_capture_state(event_id, "recording")

            # -------------------------------------------------------------
            # Mode A: Native Snapshot
            # -------------------------------------------------------------
            if recording_mode == MODE_NATIVE_SNAPSHOT:
                await self._capture_snapshot(
                    resource_key=resource_key,
                    session=session,
                    event=event,
                    scene_deadline=scene_deadline,
                    ownership_group=ownership_group,
                    valid_clips_count=valid_clips_count,
                )
                return

            # -------------------------------------------------------------
            # Mode B: Buffered Provider Export (Frigate NVR)
            # -------------------------------------------------------------
            if recording_mode == MODE_BUFFERED:
                await self._capture_buffered(
                    resource_key=resource_key,
                    session=session,
                    event=event,
                    max_window_seconds=max_window_seconds,
                    max_end_time=max_end_time,
                    profile_snapshot=profile_snapshot,
                    session_config=session_config,
                    ownership_group=ownership_group,
                    valid_clips_count=valid_clips_count,
                )
                return

            # -------------------------------------------------------------
            # Mode C: Native Camera Record (Extend or Native Fixed)
            # -------------------------------------------------------------
            chunk_seconds = int(
                profile_snapshot.get("chunk_seconds", self.coordinator.chunk_seconds)
            )
            is_fixed = recording_mode == MODE_NATIVE_FIXED

            while self.coordinator._running:
                # Revalidate scene state with fresh session
                fresh_s = await self.store.get_resource_session(resource_key)
                if not fresh_s or fresh_s.active_event_id != event_id:
                    break
                session = fresh_s
                now = _safe_utc_now()
                if not session.hold and now >= session.desired_capture_end:
                    break

                if now >= max_end_time:
                    _LOGGER.info(
                        "Capture for %s reached maximum window duration (%ss)",
                        resource_key,
                        max_window_seconds,
                    )
                    break

                max_remaining = (max_end_time - now).total_seconds()
                remaining = (
                    (session.desired_capture_end - now).total_seconds()
                    if not session.hold
                    else chunk_seconds
                )
                chunk_dur = min(chunk_seconds, max(1.0, remaining), max_remaining)

                lookback = self.coordinator._lookbacks.get(event_id, 0.0) if is_first_chunk else 0.0
                is_first_chunk = False

                chunk_reservation = int(chunk_dur * ESTIMATED_BYTES_PER_SECOND)
                res_id = f"chunk_{event_id}_{uuid.uuid4().hex[:6]}"
                staging_dir = self.store.media_root / ".staging"
                staging_path = staging_dir / f"{event_id}_{uuid.uuid4().hex[:8]}.mp4"
                sidecar_tmp = Path(str(staging_path) + ".tmp")

                _LOGGER.debug(
                    "Planning capture chunk for %s (event_id=%s): chunk_dur=%.2fs, lookback=%.2fs, remaining=%.2fs, res_id=%s",
                    resource_key,
                    event_id,
                    chunk_dur,
                    lookback,
                    remaining,
                    res_id,
                )

                try:
                    await self.store.reserve_in_flight(
                        res_id, staging_path, chunk_reservation, sidecar_paths=(sidecar_tmp,)
                    )
                except StorageCapacityError as cap_err:
                    _LOGGER.warning("Storage admission rejected for %s: %s", resource_key, cap_err)
                    last_error = ERROR_STORAGE_CAPACITY_EXCEEDED
                    await self.store.update_capture_state(event_id, "failed", error_code=last_error)
                    break

                try:
                    chunk_recorded = False
                    partial_error: str | None = None
                    chunk_req_start = now - timedelta(seconds=lookback) if lookback > 0 else now
                    chunk_req_end = now + timedelta(seconds=chunk_dur)

                    lock_timeout = (
                        min(self.coordinator.queue_wait_timeout_seconds, max(0.1, remaining))
                        if not session.hold
                        else self.coordinator.queue_wait_timeout_seconds
                    )

                    try:
                        chunk_result = await self.adapter.record_chunk(
                            camera_entity=resource_key,
                            target_path=staging_path,
                            duration_seconds=chunk_dur,
                            lookback_seconds=lookback,
                            ownership_group=ownership_group,
                            lock_timeout_seconds=lock_timeout,
                        )
                        chunk_recorded = True
                        if isinstance(chunk_result, dict):
                            raw_partial = chunk_result.get("partial_error")
                            if raw_partial:
                                partial_error = (
                                    raw_partial
                                    if isinstance(raw_partial, str)
                                    and raw_partial
                                    in (
                                        "STREAM_CAPTURE_FAILED",
                                        "STREAM_CAPTURE_TIMEOUT",
                                        "STREAM_SEQUENCE_GAP",
                                        "STREAM_PROVIDER_LOST",
                                    )
                                    else "STREAM_CAPTURE_FAILED"
                                )
                                last_error = partial_error
                    except ResourceQuarantinedError:
                        last_error = ERROR_RESOURCE_QUARANTINED
                        self.store.storage_ledger.mark_quarantined(res_id)
                        res_obj = self.store.storage_ledger.get_reservation(res_id)
                        if res_obj:
                            self.registry.register_quarantined_reservation(res_id, res_obj)
                        break
                    except RecorderError as err:
                        last_error = sanitize_capture_error(err) or "RECORDER_ERROR"
                        if self.registry.is_quarantined(resource_key, ownership_group):
                            self.store.storage_ledger.mark_quarantined(res_id)
                            res_obj = self.store.storage_ledger.get_reservation(res_id)
                            if res_obj:
                                self.registry.register_quarantined_reservation(res_id, res_obj)
                        break

                    def _inspect_staging() -> tuple[bool, int]:
                        if not staging_path.is_file():
                            return False, 0
                        return True, staging_path.stat().st_size

                    file_exists, file_size = await self.hass.async_add_executor_job(
                        _inspect_staging
                    )

                    if chunk_recorded and file_exists and file_size > 0:
                        if self.registry.is_quarantined(resource_key, ownership_group):
                            last_error = ERROR_RESOURCE_QUARANTINED
                            break
                        try:
                            await self.store.attach_and_promote_media(
                                event_id=event_id,
                                src_path=staging_path,
                                kind="video",
                                mime_type="video/mp4",
                                is_uncertain=True,
                                finalize=False,
                                requested_start=chunk_req_start,
                                requested_end=chunk_req_end,
                                capture_start=None,
                                capture_end=None,
                                time_quality=TIME_QUALITY_UNKNOWN,
                                clock_source="unknown",
                                time_uncertainty_seconds=None,
                                reservation_id=res_id,
                                adopt_staging=True,
                            )
                            valid_clips_count += 1
                            _LOGGER.debug(
                                "Promoted media chunk for %s (event_id=%s, size=%d bytes, valid_clips=%d)",
                                resource_key,
                                event_id,
                                file_size,
                                valid_clips_count,
                            )
                            if partial_error:
                                break
                        except Exception as prom_err:
                            last_error = (
                                sanitize_capture_error(prom_err) or "MEDIA_PROMOTION_FAILED"
                            )
                            _LOGGER.error("Failed to promote media: %s", last_error)
                            break
                    else:
                        last_error = "EMPTY_RECORDING_OUTPUT"
                        break
                finally:
                    if self.registry.is_quarantined(resource_key, ownership_group):
                        self.store.storage_ledger.mark_quarantined(res_id)
                        res_obj = self.store.storage_ledger.get_reservation(res_id)
                        if res_obj:
                            active_q_task = self.registry.get_quarantined_task(
                                resource_key, ownership_group
                            )
                            self.registry.register_quarantined_reservation(
                                res_id,
                                res_obj,
                                staging_path=staging_path,
                                sidecar_paths=(sidecar_tmp,),
                                task=active_q_task,
                            )
                    self.store.storage_ledger.release_reservation(res_id)
                    if not self.registry.is_quarantined(resource_key, ownership_group):

                        def _cleanup_staging() -> None:
                            staging_path.unlink(missing_ok=True)
                            sidecar_tmp.unlink(missing_ok=True)

                        await self.hass.async_add_executor_job(_cleanup_staging)

                if is_fixed:
                    break

            if event_id:
                async with self.coordinator.admission_lock:
                    await self.finish_capture(event_id, valid_clips_count, last_error)

        except asyncio.CancelledError:
            if event_id:
                final_state = "partial" if valid_clips_count > 0 else "interrupted"
                await self.store.update_capture_state(event_id, final_state, error_code="CANCELLED")
            raise
        finally:
            await self.slot_manager.release()

    async def _capture_snapshot(
        self,
        resource_key: str,
        session: ResourceSession,
        event: RecordedEvent,
        scene_deadline: datetime,
        ownership_group: str | None,
        valid_clips_count: int,
    ) -> None:
        """Execute single-frame snapshot capture."""
        event_id = session.active_event_id
        assert event_id is not None
        staging_dir = self.store.media_root / ".staging"
        staging_path = staging_dir / f"{event_id}_{uuid.uuid4().hex[:8]}.jpg"
        snap_reservation = 2 * 1024 * 1024
        res_id = f"snap_{event_id}_{uuid.uuid4().hex[:6]}"

        try:
            await self.store.reserve_in_flight(res_id, staging_path, snap_reservation)

            alias_timeout = max(0.1, (scene_deadline - _safe_utc_now()).total_seconds())
            snap_meta = await self.snapshot_adapter.capture_snapshot(
                camera_entity=resource_key,
                target_path=staging_path,
                ownership_group=ownership_group,
                lock_timeout_seconds=alias_timeout,
            )

            if self.registry.is_quarantined(resource_key, ownership_group):
                raise ResourceQuarantinedError(f"Resource {resource_key} is quarantined")

            await self.store.attach_and_promote_media(
                event_id=event_id,
                src_path=staging_path,
                kind="image",
                mime_type=snap_meta.get("mime_type", "image/jpeg"),
                is_uncertain=False,
                finalize=True,
                requested_start=event.start_at,
                requested_end=event.start_at,
                capture_start=None,
                capture_end=None,
                time_quality=TIME_QUALITY_UNKNOWN,
                clock_source="unknown",
                reservation_id=res_id,
                adopt_staging=True,
            )
            valid_clips_count += 1
            await self.finish_capture(event_id, valid_clips_count, None)
        except Exception as snap_err:
            clean_err = sanitize_capture_error(snap_err) or "SNAPSHOT_FAILED"
            if self.registry.is_quarantined(resource_key, ownership_group):
                self.store.storage_ledger.mark_quarantined(res_id)
                res_obj = self.store.storage_ledger.get_reservation(res_id)
                if res_obj:
                    active_q_task = self.registry.get_quarantined_task(
                        resource_key, ownership_group
                    )
                    self.registry.register_quarantined_reservation(
                        res_id, res_obj, staging_path=staging_path, task=active_q_task
                    )
            await self.finish_capture(event_id, valid_clips_count, clean_err)
        finally:
            if self.registry.is_quarantined(resource_key, ownership_group):
                self.store.storage_ledger.mark_quarantined(res_id)
                res_obj = self.store.storage_ledger.get_reservation(res_id)
                if res_obj:
                    active_q_task = self.registry.get_quarantined_task(
                        resource_key, ownership_group
                    )
                    self.registry.register_quarantined_reservation(
                        res_id, res_obj, staging_path=staging_path, task=active_q_task
                    )
            self.store.storage_ledger.release_reservation(res_id)
            if not self.registry.is_quarantined(resource_key, ownership_group):

                def _cleanup_snap_staging() -> None:
                    staging_path.unlink(missing_ok=True)

                await self.hass.async_add_executor_job(_cleanup_snap_staging)

    async def _capture_buffered(
        self,
        resource_key: str,
        session: ResourceSession,
        event: RecordedEvent,
        max_window_seconds: float,
        max_end_time: datetime,
        profile_snapshot: dict[str, Any],
        session_config: dict[str, Any],
        ownership_group: str | None,
        valid_clips_count: int,
    ) -> None:
        """Execute buffered clip export from upstream NVR."""
        event_id = session.active_event_id
        assert event_id is not None
        staging_dir = self.store.media_root / ".staging"
        staging_path = staging_dir / f"{event_id}_{uuid.uuid4().hex[:8]}.mp4"
        buf_reservation = int(min(max_window_seconds, 60.0) * ESTIMATED_BYTES_PER_SECOND)
        res_id = f"buf_{event_id}_{uuid.uuid4().hex[:6]}"

        try:
            await self.store.reserve_in_flight(res_id, staging_path, buf_reservation)

            start_ts = event.start_at.timestamp()
            end_ts = min(max_end_time, session.desired_capture_end).timestamp()
            if end_ts <= start_ts:
                end_ts = start_ts + float(profile_snapshot.get("window_seconds", 15.0))

            export_delay = float(
                profile_snapshot.get("export_delay_seconds", DEFAULT_EXPORT_DELAY_SECONDS)
            )
            now_ts = _safe_utc_now().timestamp()
            wait_sec = (end_ts + export_delay) - now_ts
            if wait_sec > 0:
                await asyncio.sleep(min(wait_sec, float(max_window_seconds)))

            cam_name = profile_snapshot.get("frigate_camera_name") or session_config.get(
                "frigate_camera_name"
            )
            if not cam_name:
                raise RecorderError("MISSING_FRIGATE_CAMERA_NAME")

            frigate_url = profile_snapshot.get("frigate_base_url") or session_config.get(
                "frigate_base_url"
            )
            frigate_token = profile_snapshot.get("frigate_auth_token") or session_config.get(
                "frigate_auth_token"
            )

            adapter_cls = _get_frigate_buffered_adapter()
            adapter = adapter_cls(
                base_url=frigate_url,
                auth_token=frigate_token,
            )

            coverage_info = await adapter.check_recordings_coverage(
                camera_name=cam_name, start_ts=start_ts, end_ts=end_ts
            )
            is_partial = not coverage_info.get("is_complete", False)
            actual_start_dt = (
                datetime.fromtimestamp(coverage_info["actual_start"], tz=UTC)
                if coverage_info.get("actual_start") is not None
                else None
            )
            actual_end_dt = (
                datetime.fromtimestamp(coverage_info["actual_end"], tz=UTC)
                if coverage_info.get("actual_end") is not None
                else None
            )

            _export_meta = await adapter.export_clip(
                camera_name=cam_name,
                start_ts=start_ts,
                end_ts=end_ts,
                target_path=staging_path,
            )

            if self.registry.is_quarantined(resource_key, ownership_group):
                raise ResourceQuarantinedError(f"Resource {resource_key} is quarantined")

            await self.store.attach_and_promote_media(
                event_id=event_id,
                src_path=staging_path,
                kind="video",
                mime_type="video/mp4",
                is_uncertain=is_partial,
                finalize=True,
                requested_start=event.start_at,
                requested_end=session.desired_capture_end,
                capture_start=actual_start_dt,
                capture_end=actual_end_dt,
                time_quality=TIME_QUALITY_PROVIDER_REPORTED,
                clock_source="frigate_nvr",
                reservation_id=res_id,
                adopt_staging=True,
            )
            valid_clips_count += 1
            await self.finish_capture(event_id, valid_clips_count, None)
        except Exception as buf_err:
            clean_err = sanitize_capture_error(buf_err) or ERROR_BUFFER_UNAVAILABLE
            await self.finish_capture(event_id, valid_clips_count, clean_err)
        finally:
            self.store.storage_ledger.release_reservation(res_id)
            if not self.registry.is_quarantined(resource_key, ownership_group):

                def _cleanup_buf_staging() -> None:
                    staging_path.unlink(missing_ok=True)

                await self.hass.async_add_executor_job(_cleanup_buf_staging)
