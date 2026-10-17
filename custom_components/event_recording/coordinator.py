"""Coordinator and durable intents worker for Event Recording.

Coordinates background intent leasing, independent per-resource capture tasks,
bounded parallel resource slots, queue depth limits, capacity pre-checks,
and retention pruning.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

from .const import (
    CONF_CHUNK_SECONDS,
    CONF_MAX_PARALLEL_RESOURCES,
    CONF_MAX_QUEUE_DEPTH,
    CONF_MAX_WINDOW_SECONDS,
    CONF_MIN_DISK_FREE_MB,
    CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
    CONF_RETENTION_DAYS,
    CONF_RETENTION_SIZE_MB,
    DEFAULT_CHUNK_SECONDS,
    DEFAULT_EXPORT_DELAY_SECONDS,
    DEFAULT_MAX_PARALLEL_RESOURCES,
    DEFAULT_MAX_QUEUE_DEPTH,
    DEFAULT_MAX_WINDOW_SECONDS,
    DEFAULT_MIN_DISK_FREE_MB,
    DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_RETENTION_SIZE_MB,
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
from .core.models import RecordedEvent, sanitize_capture_error, utc_now
from .core.storage_ledger import StorageCapacityError, StoragePolicy
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


def _inspect_staging_and_disk(media_root: Any) -> tuple[int, int]:
    """Inspect unreserved staging bytes and physical disk free space."""
    staging_bytes = 0
    import shutil
    from pathlib import Path

    root_path = Path(media_root)
    staging_dir = root_path / ".staging"
    if staging_dir.is_dir():
        for p in staging_dir.iterdir():
            try:
                if p.is_file():
                    staging_bytes += p.stat().st_size
            except Exception:
                pass
    free_bytes = 0
    try:
        _total, _used, free = shutil.disk_usage(str(root_path))
        free_bytes = free
    except Exception:
        pass
    return staging_bytes, free_bytes


class QueueOverflowError(Exception):
    """Raised when waiting tasks exceed max_queue_depth."""


class DynamicSlotManager:
    """Bounded slot manager supporting hot capacity adjustments without semaphore replacement.

    Uses an asyncio.Condition to coordinate slot acquisition and release,
    enforcing max_queue_depth overflow limits and counting unsettled quarantined writers.
    """

    def __init__(
        self,
        capacity: int = 2,
        max_queue_depth: int = 32,
        quarantined_count_fn: Any | None = None,
    ) -> None:
        self._capacity = max(1, min(8, int(capacity)))
        self._max_queue_depth = max(1, min(256, int(max_queue_depth)))
        self._quarantined_count_fn = quarantined_count_fn
        self._active = 0
        self._waiting = 0
        self._cond = asyncio.Condition()
        self._notify_tasks: set[asyncio.Task[Any]] = set()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def max_queue_depth(self) -> int:
        return self._max_queue_depth

    @property
    def active_slots(self) -> int:
        return self._active

    @property
    def waiting_tasks(self) -> int:
        return self._waiting

    @property
    def effective_capacity(self) -> int:
        """Physical capacity reduced by any active or unsettled quarantined native writers."""
        q = self._quarantined_count_fn() if self._quarantined_count_fn else 0
        return max(0, self._capacity - q)

    async def set_capacity(self, new_capacity: int) -> None:
        """Hot-adjust capacity without interrupting active holders."""
        async with self._cond:
            self._capacity = max(1, min(8, int(new_capacity)))
            self._cond.notify_all()

    async def set_max_queue_depth(self, new_depth: int) -> None:
        """Hot-adjust queue depth limit."""
        async with self._cond:
            self._max_queue_depth = max(1, min(256, int(new_depth)))

    async def acquire(self, timeout_seconds: float | None = None) -> bool:
        """Acquire a slot with optional timeout.

        Raises QueueOverflowError if waiting queue depth limit is exceeded.
        """
        async with self._cond:
            if self._waiting >= self._max_queue_depth:
                raise QueueOverflowError(
                    f"Queue depth limit ({self._max_queue_depth}) exceeded"
                )
            self._waiting += 1
            try:
                if timeout_seconds is None:
                    while self._active >= self.effective_capacity:
                        await self._cond.wait()
                    self._active += 1
                    return True
                else:
                    loop = asyncio.get_running_loop()
                    deadline = loop.time() + timeout_seconds
                    while self._active >= self.effective_capacity:
                        remaining = deadline - loop.time()
                        if remaining <= 0:
                            return False
                        try:
                            await asyncio.wait_for(self._cond.wait(), timeout=remaining)
                        except TimeoutError:
                            return False
                    self._active += 1
                    return True
            finally:
                self._waiting -= 1
                self._cond.notify_all()

    def notify_capacity_changed(self) -> None:
        """Wake up any waiting tasks when capacity or quarantined count changes."""
        async def _notify() -> None:
            async with self._cond:
                self._cond.notify_all()

        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(_notify())
            self._notify_tasks.add(task)
            task.add_done_callback(self._notify_tasks.discard)
        except RuntimeError:
            pass

    async def release(self) -> None:
        """Release a slot."""
        async with self._cond:
            self._active = max(0, self._active - 1)
            self._cond.notify_all()


class RecordingCoordinator:
    """Coordinates background intent leasing, capture tasks, and retention pruning."""

    def __init__(
        self,
        hass: HomeAssistant,
        store: JournalStore,
        entry: ConfigEntry,
        registry: RecorderRegistry,
    ) -> None:
        self.hass = hass
        self.store = store
        self.entry = entry
        self.registry = registry

        self.worker_id = f"worker_{uuid.uuid4().hex[:8]}"
        self._running = False
        self._worker_task: asyncio.Task[Any] | None = None
        self._resource_tasks: dict[str, asyncio.Task[Any]] = {}
        self._pending_resources: set[str] = set()
        self._intent_event = asyncio.Event()

        # Slot manager for bounded parallelism
        self._slot_manager = DynamicSlotManager(
            capacity=self.max_parallel_resources,
            max_queue_depth=self.max_queue_depth,
            quarantined_count_fn=lambda: self.registry.quarantined_count,
        )
        self.registry.register_settled_callback(self._slot_manager.notify_capacity_changed)

        retention_mb = (
            self.max_storage_bytes // (1024 * 1024) if self.max_storage_bytes > 0 else None
        )
        self.store.set_storage_policy(
            StoragePolicy(min_disk_free_mb=self.min_disk_free_mb, retention_size_mb=retention_mb)
        )

        self.admission_lock = asyncio.Lock()
        self._capacity_lock = asyncio.Lock()

        # Stream adapters
        self.adapter = CameraRecorderAdapter(hass, registry)
        self.snapshot_adapter = SnapshotRecorderAdapter(hass, registry)

        self._lookbacks: dict[str, float] = {}
        self._retention_at: float = 0.0

        # Diagnostics metrics
        self.expired_captures_count = 0
        self.queue_timeouts_count = 0
        self.queue_overflow_count = 0

    @property
    def slot_manager(self) -> DynamicSlotManager:
        """Return slot manager for diagnostics and concurrency monitoring."""
        return self._slot_manager

    @property
    def chunk_seconds(self) -> int:
        """Return configured chunk duration in seconds."""
        val = self.entry.options.get(
            CONF_CHUNK_SECONDS,
            self.entry.data.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS),
        )
        return int(min(60, max(1, int(val))))

    @property
    def max_window_seconds(self) -> int:
        """Return maximum duration of continuous capture in seconds."""
        val = self.entry.options.get(
            CONF_MAX_WINDOW_SECONDS,
            self.entry.data.get(CONF_MAX_WINDOW_SECONDS, DEFAULT_MAX_WINDOW_SECONDS),
        )
        return int(min(300, max(1, int(val))))

    @property
    def retention_days(self) -> int:
        """Return retention period in days."""
        val = self.entry.options.get(
            CONF_RETENTION_DAYS,
            self.entry.data.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
        )
        return int(max(1, int(val)))

    @property
    def max_storage_bytes(self) -> int:
        """Return retention budget ceiling in bytes."""
        mb = self.entry.options.get(
            CONF_RETENTION_SIZE_MB,
            self.entry.data.get(CONF_RETENTION_SIZE_MB, DEFAULT_RETENTION_SIZE_MB),
        )
        return int(max(0, int(mb))) * 1024 * 1024

    @property
    def max_parallel_resources(self) -> int:
        """Return maximum parallel capture resources (1..8)."""
        val = self.entry.options.get(
            CONF_MAX_PARALLEL_RESOURCES,
            self.entry.data.get(CONF_MAX_PARALLEL_RESOURCES, DEFAULT_MAX_PARALLEL_RESOURCES),
        )
        return int(min(8, max(1, int(val))))

    @property
    def max_queue_depth(self) -> int:
        """Return maximum waiting tasks allowed in queue (1..256)."""
        val = self.entry.options.get(
            CONF_MAX_QUEUE_DEPTH,
            self.entry.data.get(CONF_MAX_QUEUE_DEPTH, DEFAULT_MAX_QUEUE_DEPTH),
        )
        return int(min(256, max(1, int(val))))

    async def set_max_parallel_resources(self, new_capacity: int) -> None:
        """Hot-adjust parallel resource capacity without replacing locks."""
        await self._slot_manager.set_capacity(new_capacity)

    async def set_max_queue_depth(self, new_depth: int) -> None:
        """Hot-adjust queue depth limit."""
        await self._slot_manager.set_max_queue_depth(new_depth)

    @property
    def queue_wait_timeout_seconds(self) -> float:
        """Return queue wait timeout in seconds."""
        val = self.entry.options.get(
            CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
            self.entry.data.get(
                CONF_QUEUE_WAIT_TIMEOUT_SECONDS, DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS
            ),
        )
        return float(min(300.0, max(1.0, float(val))))

    @property
    def min_disk_free_mb(self) -> int:
        """Return minimum free disk space floor in megabytes (at least 50)."""
        val = self.entry.options.get(
            CONF_MIN_DISK_FREE_MB,
            self.entry.data.get(CONF_MIN_DISK_FREE_MB, DEFAULT_MIN_DISK_FREE_MB),
        )
        return int(max(50, int(val)))

    @property
    def min_disk_free_bytes(self) -> int:
        """Return minimum free disk space threshold in bytes."""
        return self.min_disk_free_mb * 1024 * 1024

    def notify_new_intent(self) -> None:
        """Wake worker loop when a new record intent is admitted."""
        self._intent_event.set()

    async def admit_record_intent(self, *args: Any, **kwargs: Any) -> Any:
        """Pin camera-profile defaults to a new session before durable admission."""
        profile_defaults = kwargs.pop("_profile_defaults", None)
        explicit_fields = set(kwargs.pop("_explicit_fields", ()))
        explicit_recorder_config = dict(kwargs.pop("_explicit_recorder_config", {}) or {})
        resource_key = kwargs.get("resource_key")

        if isinstance(profile_defaults, dict) and resource_key:
            session = await self.store.get_resource_session(resource_key)
            if session and session.active_event_id:
                session_config = dict(session.recorder_config or {})
                pinned_profile = dict(session_config.get("_event_recording_profile") or {})
                # Coalesced triggers inherit defaults from the active session's snapshot.
                # Explicit fields on this trigger remain explicit and keep their values.
                field_map = {
                    "source_id": "source_id",
                    "label": "label",
                    "window_seconds": "window_seconds",
                    "lookback_seconds": "lookback_seconds",
                    "hold": "hold",
                    "tail_window_seconds": "tail_window_seconds",
                }
                for arg_name, profile_name in field_map.items():
                    if arg_name not in explicit_fields and profile_name in pinned_profile:
                        kwargs[arg_name] = pinned_profile[profile_name]

                # The active session owns its recorder configuration and immutable
                # profile snapshot. Preserve it when the caller supplied no explicit
                # adapter configuration; incompatible explicit changes remain visible
                # to JournalStore's coalescing validation.
                merged_config = dict(session_config)
                if explicit_recorder_config:
                    merged_config.update(explicit_recorder_config)
                merged_config["_event_recording_profile"] = pinned_profile
                kwargs["recorder_config"] = merged_config
            else:
                recorder_config = dict(kwargs.get("recorder_config") or {})
                recorder_config["_event_recording_profile"] = dict(profile_defaults)
                kwargs["recorder_config"] = recorder_config

        return await self.store.admit_record_intent(*args, **kwargs)

    async def start(self) -> None:
        """Start the coordinator worker and reconcile store."""
        if hasattr(self.registry, "bind_ledger"):
            self.registry.bind_ledger(self.store.storage_ledger)
        # Adopt any surviving quarantined reservations from registry
        if hasattr(self.registry, "get_all_quarantined_reservations"):
            self.store.storage_ledger.transfer_reservations(
                self.registry.get_all_quarantined_reservations()
            )
        retention_mb = (
            self.max_storage_bytes // (1024 * 1024) if self.max_storage_bytes > 0 else None
        )
        self.store.set_storage_policy(
            StoragePolicy(min_disk_free_mb=self.min_disk_free_mb, retention_size_mb=retention_mb)
        )
        if not self.registry.has_unsettled:
            await self.store.reconcile(worker_id=self.worker_id)
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop(), name="event_recording_worker")

    async def stop(self) -> None:
        """Stop coordinator gracefully without cancelling quarantined background native writers."""
        if hasattr(self.registry, "unregister_settled_callback"):
            self.registry.unregister_settled_callback(self._slot_manager.notify_capacity_changed)
        if hasattr(self.registry, "unbind_ledger"):
            self.registry.unbind_ledger(self.store.storage_ledger)
        self._running = False
        self._intent_event.set()

        for t in list(self._resource_tasks.values()):
            if not t.done():
                t.cancel()

        if self._resource_tasks:
            await asyncio.gather(
                *self._resource_tasks.values(), return_exceptions=True
            )
        self._resource_tasks.clear()

        if self._worker_task and not self._worker_task.done():
            self._worker_task.cancel()
            try:
                await self._worker_task
            except asyncio.CancelledError:
                pass
            self._worker_task = None

    async def _worker_loop(self) -> None:
        """Main durable coordinator loop polling intents and managing tasks."""
        while self._running:
            try:
                await self._step_intents()
                await self._step_resources()
                await self._periodic_retention()

                # Sleep bounded or wake immediately on new intent notification
                try:
                    await asyncio.wait_for(self._intent_event.wait(), timeout=0.25)
                    self._intent_event.clear()
                except TimeoutError:
                    pass

            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Unexpected error in RecordingCoordinator worker loop: %s", err)
                await asyncio.sleep(1.0)

    async def _step_intents(self) -> None:
        """Poll and lease unhandled recording intents."""
        intents = await self.store.lease_record_intents(
            worker_id=self.worker_id, limit=16, lease_seconds=30.0
        )
        for intent in intents:
            if intent.operation == "stop":
                session = await self.store.get_resource_session(intent.resource_key)
                if session and session.active_event_id:
                    await self.store.update_capture_state(session.active_event_id, "ready")
                    await self.store.clear_resource_session(intent.resource_key)
                await self.store.complete_record_intent(intent.intent_id, self.worker_id)
                continue

            if intent.lookback_seconds > 0.0 and intent.event_id:
                self._lookbacks[intent.event_id] = intent.lookback_seconds

            self._pending_resources.add(intent.resource_key)
            await self.store.complete_record_intent(intent.intent_id, self.worker_id)

    async def _step_resources(self) -> None:
        """Spawn independent capture task per distinct resource_key, bounded by max_queue_depth."""
        for r_key in tuple(self._pending_resources):
            existing_task = self._resource_tasks.get(r_key)
            if existing_task is None or existing_task.done():
                session = await self.store.get_resource_session(r_key)
                if session is None or not session.active_event_id:
                    self._pending_resources.discard(r_key)
                    continue

                if self._slot_manager.waiting_tasks >= self.max_queue_depth:
                    self.queue_overflow_count += 1
                    self._pending_resources.discard(r_key)
                    await self.store.update_capture_state(
                        session.active_event_id, "failed", error_code=ERROR_QUEUE_OVERFLOW
                    )
                    await self.store.clear_resource_session(r_key)
                    continue

                self._pending_resources.discard(r_key)
                task = asyncio.create_task(
                    self._run_resource_capture(r_key),
                    name=f"event_recording_capture_{r_key}",
                )
                self._resource_tasks[r_key] = task

    async def _periodic_retention(self) -> None:
        """Run periodic storage ledger reconciliation and retention pruning."""
        loop = asyncio.get_running_loop()
        now_time = loop.time()
        if now_time - self._retention_at < 60.0:
            return
        self._retention_at = now_time

        try:
            await self.store.enforce_retention(
                max_bytes=self.max_storage_bytes,
                max_age_days=self.retention_days,
            )
            await self.store.reconcile_storage_accounting()
        except Exception as err:
            _LOGGER.debug("Notice on periodic retention maintenance: %s", err)

    async def _finish_capture(
        self, event_id: str, valid_clips_count: int, error: str | None
    ) -> None:
        """Conclude capture state adhering strictly to invariant: zero clips CANNOT be ready."""
        if valid_clips_count == 0:
            terminal_error = error or "ZERO_CLIPS_RECORDED"
            await self.store.update_capture_state(
                event_id, "failed", error_code=terminal_error
            )
        elif error:
            await self.store.update_capture_state(event_id, "partial", error_code=error)
        else:
            await self.store.finalize_event(event_id)

    async def _run_resource_capture(self, resource_key: str) -> None:
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
        ownership_group = (
            profile_snapshot.get("ownership_group")
            or session_config.get("ownership_group")
        )

        now = utc_now()
        deadline_seconds = float(self.queue_wait_timeout_seconds)
        if recording_mode != MODE_BUFFERED and not session.hold:
            remaining_scene = (session.desired_capture_end - now).total_seconds()
            if remaining_scene <= 0:
                self.expired_captures_count += 1
                await self.store.update_capture_state(
                    event_id, "failed", error_code=ERROR_EXPIRED_WINDOW
                )
                await self.store.clear_resource_session(resource_key)
                return
            deadline_seconds = min(deadline_seconds, remaining_scene)

        scene_deadline = now + timedelta(seconds=deadline_seconds)

        try:
            acquired = await self._slot_manager.acquire(timeout_seconds=deadline_seconds)
        except QueueOverflowError:
            self.queue_overflow_count += 1
            await self.store.update_capture_state(
                event_id, "failed", error_code=ERROR_QUEUE_OVERFLOW
            )
            await self.store.clear_resource_session(resource_key)
            return

        if not acquired:
            now_post = utc_now()
            if (
                recording_mode != MODE_BUFFERED
                and not session.hold
                and now_post >= session.desired_capture_end
            ):
                self.expired_captures_count += 1
                err_code = ERROR_EXPIRED_WINDOW
            else:
                self.queue_timeouts_count += 1
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

            now = utc_now()
            if (
                recording_mode != MODE_BUFFERED
                and not session.hold
                and now >= session.desired_capture_end
            ):
                self.expired_captures_count += 1
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
                    float(profile_snapshot.get("max_window_seconds", self.max_window_seconds)),
                ),
            )
            max_end_time = capture_start + timedelta(seconds=max_window_seconds)

            if event.capture_state == "requested":
                await self.store.update_capture_state(event_id, "recording")

            # -------------------------------------------------------------
            # Mode A: Native Snapshot
            # -------------------------------------------------------------
            if recording_mode == MODE_NATIVE_SNAPSHOT:
                staging_dir = self.store.media_root / ".staging"
                staging_path = staging_dir / f"{event_id}_{uuid.uuid4().hex[:8]}.jpg"
                snap_reservation = 2 * 1024 * 1024
                res_id = f"snap_{event_id}_{uuid.uuid4().hex[:6]}"

                try:
                    await self.store.reserve_in_flight(res_id, staging_path, snap_reservation)

                    alias_timeout = max(0.1, (scene_deadline - utc_now()).total_seconds())
                    snap_meta = await self.snapshot_adapter.capture_snapshot(
                        camera_entity=resource_key,
                        target_path=staging_path,
                        ownership_group=ownership_group,
                        lock_timeout_seconds=alias_timeout,
                    )

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
                    )
                    valid_clips_count += 1
                    await self._finish_capture(event_id, valid_clips_count, None)
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
                    await self._finish_capture(event_id, valid_clips_count, clean_err)
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
                return

            # -------------------------------------------------------------
            # Mode B: Buffered Provider Export (Frigate NVR)
            # -------------------------------------------------------------
            if recording_mode == MODE_BUFFERED:
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

                    # Post-trigger delay: wait until desired window end + delay
                    export_delay = float(
                        profile_snapshot.get("export_delay_seconds", DEFAULT_EXPORT_DELAY_SECONDS)
                    )
                    now_ts = utc_now().timestamp()
                    wait_sec = (end_ts + export_delay) - now_ts
                    if wait_sec > 0:
                        await asyncio.sleep(min(wait_sec, float(max_window_seconds)))

                    cam_name = (
                        profile_snapshot.get("frigate_camera_name")
                        or session_config.get("frigate_camera_name")
                    )
                    if not cam_name:
                        raise RecorderError("MISSING_FRIGATE_CAMERA_NAME")

                    frigate_url = (
                        profile_snapshot.get("frigate_base_url")
                        or session_config.get("frigate_base_url")
                    )
                    frigate_token = (
                        profile_snapshot.get("frigate_auth_token")
                        or session_config.get("frigate_auth_token")
                    )

                    adapter = FrigateBufferedAdapter(
                        base_url=frigate_url,
                        auth_token=frigate_token,
                    )

                    # Coverage preflight
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
                    )
                    valid_clips_count += 1
                    await self._finish_capture(event_id, valid_clips_count, None)
                except Exception as buf_err:
                    clean_err = sanitize_capture_error(buf_err) or ERROR_BUFFER_UNAVAILABLE
                    await self._finish_capture(event_id, valid_clips_count, clean_err)
                finally:
                    self.store.storage_ledger.release_reservation(res_id)
                    if not self.registry.is_quarantined(resource_key, ownership_group):
                        def _cleanup_buf_staging() -> None:
                            staging_path.unlink(missing_ok=True)

                        await self.hass.async_add_executor_job(_cleanup_buf_staging)
                return

            # -------------------------------------------------------------
            # Mode C: Native Camera Record (Extend or Native Fixed)
            # -------------------------------------------------------------
            chunk_seconds = int(profile_snapshot.get("chunk_seconds", self.chunk_seconds))
            is_fixed = recording_mode == MODE_NATIVE_FIXED

            while self._running:
                # Revalidate scene state with fresh session
                fresh_s = await self.store.get_resource_session(resource_key)
                if not fresh_s or fresh_s.active_event_id != event_id:
                    break
                session = fresh_s
                now = utc_now()
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

                lookback = (
                    self._lookbacks.get(event_id, 0.0)
                    if is_first_chunk
                    else 0.0
                )
                is_first_chunk = False

                chunk_reservation = int(chunk_dur * ESTIMATED_BYTES_PER_SECOND)
                res_id = f"chunk_{event_id}_{uuid.uuid4().hex[:6]}"
                staging_dir = self.store.media_root / ".staging"
                staging_path = staging_dir / f"{event_id}_{uuid.uuid4().hex[:8]}.mp4"
                sidecar_tmp = Path(str(staging_path) + ".tmp")

                try:
                    await self.store.reserve_in_flight(
                        res_id, staging_path, chunk_reservation, sidecar_paths=(sidecar_tmp,)
                    )
                except StorageCapacityError as cap_err:
                    _LOGGER.warning(
                        "Storage admission rejected for %s: %s", resource_key, cap_err
                    )
                    last_error = ERROR_STORAGE_CAPACITY_EXCEEDED
                    await self.store.update_capture_state(
                        event_id, "failed", error_code=last_error
                    )
                    break

                try:
                    chunk_recorded = False
                    chunk_req_start = now - timedelta(seconds=lookback) if lookback > 0 else now
                    chunk_req_end = now + timedelta(seconds=chunk_dur)

                    lock_timeout = (
                        min(self.queue_wait_timeout_seconds, max(0.1, remaining))
                        if not session.hold
                        else self.queue_wait_timeout_seconds
                    )

                    try:
                        await self.adapter.record_chunk(
                            camera_entity=resource_key,
                            target_path=staging_path,
                            duration_seconds=chunk_dur,
                            lookback_seconds=lookback,
                            ownership_group=ownership_group,
                            lock_timeout_seconds=lock_timeout,
                        )
                        chunk_recorded = True
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

                    file_exists, file_size = await self.hass.async_add_executor_job(_inspect_staging)

                    if chunk_recorded and file_exists and file_size > 0:
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
                            )
                            valid_clips_count += 1
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
                            active_q_task = self.registry.get_quarantined_task(resource_key, ownership_group)
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
                async with self.admission_lock:
                    await self._finish_capture(event_id, valid_clips_count, last_error)

        except asyncio.CancelledError:
            if event_id:
                final_state = "partial" if valid_clips_count > 0 else "interrupted"
                await self.store.update_capture_state(
                    event_id, final_state, error_code="CANCELLED"
                )
            raise
        finally:
            await self._slot_manager.release()
