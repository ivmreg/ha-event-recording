"""Coordinator and durable intents worker for Event Recording.

Coordinates background intent leasing, independent per-resource capture tasks,
bounded parallel resource slots, queue depth limits, capacity pre-checks,
and retention pruning.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

from .capture import ESTIMATED_BYTES_PER_SECOND, CaptureExecutor
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
    DEFAULT_MAX_PARALLEL_RESOURCES,
    DEFAULT_MAX_QUEUE_DEPTH,
    DEFAULT_MAX_WINDOW_SECONDS,
    DEFAULT_MIN_DISK_FREE_MB,
    DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_RETENTION_SIZE_MB,
    ERROR_QUEUE_OVERFLOW,
)
from .core.models import utc_now
from .core.storage_ledger import StoragePolicy
from .core.store import JournalStore
from .go2rtc_buffer import Go2rtcBufferManager
from .recorder import (
    CameraRecorderAdapter,
    FrigateBufferedAdapter,
    RecorderError,
    RecorderRegistry,
    ResourceQuarantinedError,
    SnapshotRecorderAdapter,
)
from .session import SessionCoordinator

_LOGGER = logging.getLogger(__name__)


def _inspect_staging_and_disk(media_root: Any) -> tuple[int, int]:
    """Inspect unreserved staging bytes and physical disk free space."""
    staging_bytes = 0
    import shutil

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
            old_cap = self._capacity
            self._capacity = max(1, min(8, int(new_capacity)))
            _LOGGER.debug(
                "DynamicSlotManager capacity adjusted: %d -> %d", old_cap, self._capacity
            )
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
            _LOGGER.debug(
                "DynamicSlotManager.acquire called: active=%d, waiting=%d, effective_capacity=%d, timeout=%s",
                self._active,
                self._waiting,
                self.effective_capacity,
                timeout_seconds,
            )
            try:
                if timeout_seconds is None:
                    while self._active >= self.effective_capacity:
                        await self._cond.wait()
                    self._active += 1
                    _LOGGER.debug(
                        "DynamicSlotManager.acquire granted: active=%d, waiting=%d",
                        self._active,
                        self._waiting,
                    )
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
                    _LOGGER.debug(
                        "DynamicSlotManager.acquire granted: active=%d, waiting=%d",
                        self._active,
                        self._waiting,
                    )
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
            old_active = self._active
            self._active = max(0, self._active - 1)
            _LOGGER.debug(
                "DynamicSlotManager.release: active=%d -> %d, waiting=%d",
                old_active,
                self._active,
                self._waiting,
            )
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
        self.go2rtc_buffers = Go2rtcBufferManager(hass, entry)
        self.adapter = CameraRecorderAdapter(hass, registry, buffer_manager=self.go2rtc_buffers)
        self.snapshot_adapter = SnapshotRecorderAdapter(hass, registry)

        # Specialized session and capture execution delegates
        self.session_coordinator = SessionCoordinator(self.store)
        self._capture_executor = CaptureExecutor(self)

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
        return await self.session_coordinator.admit_record_intent(*args, **kwargs)

    async def start(self) -> None:
        """Start the coordinator worker and reconcile store."""
        _LOGGER.debug(
            "RecordingCoordinator starting: worker_id=%s, max_parallel=%d, max_queue_depth=%d, chunk_seconds=%d",
            self.worker_id,
            self.max_parallel_resources,
            self.max_queue_depth,
            self.chunk_seconds,
        )
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
        await self.go2rtc_buffers.reconcile()
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop(), name="event_recording_worker")

    async def stop(self) -> None:
        """Stop coordinator gracefully without cancelling quarantined background native writers."""
        _LOGGER.debug(
            "RecordingCoordinator stopping: active_resource_tasks=%d",
            len(self._resource_tasks),
        )
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

        await self.go2rtc_buffers.stop()

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
        if intents:
            _LOGGER.debug("RecordingCoordinator leased %d intent(s)", len(intents))
        for intent in intents:
            if intent.operation == "stop":
                await self.session_coordinator.handle_stop_intent(intent.resource_key)
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
                _LOGGER.debug(
                    "Spawning capture task for resource %s (event_id=%s)",
                    r_key,
                    session.active_event_id,
                )
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
        _LOGGER.debug(
            "Running periodic storage retention pass (max_bytes=%d, max_age_days=%d)",
            self.max_storage_bytes,
            self.retention_days,
        )

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
        await self._capture_executor.finish_capture(event_id, valid_clips_count, error)

    async def _run_resource_capture(self, resource_key: str) -> None:
        """Serialize capture for a specific resource under dynamic slot concurrency."""
        await self._capture_executor.run_resource_capture(resource_key)


__all__ = [
    "ESTIMATED_BYTES_PER_SECOND",
    "CaptureExecutor",
    "DynamicSlotManager",
    "FrigateBufferedAdapter",
    "QueueOverflowError",
    "RecorderError",
    "RecordingCoordinator",
    "ResourceQuarantinedError",
    "SessionCoordinator",
    "_inspect_staging_and_disk",
    "utc_now",
]
