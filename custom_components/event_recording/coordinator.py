"""Coordinator and durable intents worker for Event Recording.

Coordinates background intent leasing, independent per-resource capture tasks,
bounded parallel resource slots, capacity pre-checks, and retention pruning.
"""

from __future__ import annotations

import asyncio
import logging
import shutil
import uuid
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

from .const import (
    CONF_CHUNK_SECONDS,
    CONF_MAX_WINDOW_SECONDS,
    CONF_RETENTION_DAYS,
    CONF_RETENTION_SIZE_MB,
    DEFAULT_CHUNK_SECONDS,
    DEFAULT_MAX_HOLD_SECONDS,
    DEFAULT_MAX_WINDOW_SECONDS,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_RETENTION_SIZE_MB,
    MAX_PARALLEL_RESOURCES,
)
from .core.models import RecordedEvent, sanitize_capture_error, utc_now
from .core.store import JournalStore
from .recorder import (
    CameraRecorderAdapter,
    RecorderError,
    RecorderRegistry,
    ResourceQuarantinedError,
)

_LOGGER = logging.getLogger(__name__)

# Minimum free disk space threshold (50 MB)
MIN_DISK_FREE_BYTES = 50 * 1024 * 1024
# Estimated bytes per second of camera capture (1 MB/s)
ESTIMATED_BYTES_PER_SECOND = 1024 * 1024


def _inspect_storage(media_root: Path) -> tuple[int, int | None]:
    """Inspect existing owned/staging media bytes and disk free space off-loop.

    Fails closed (free_bytes=None) if disk_usage encounters an error.
    """
    free_bytes: int | None = None
    try:
        free_bytes = shutil.disk_usage(media_root).free
    except Exception as err:
        _LOGGER.debug("Disk usage check failed for %s: %s", media_root, err)
        free_bytes = None

    existing_bytes = 0
    try:
        if media_root.is_dir():
            for p in media_root.rglob("*"):
                try:
                    if p.is_file():
                        existing_bytes += p.stat().st_size
                except Exception:
                    pass
    except Exception as err:
        _LOGGER.debug("Failed scanning media directory %s: %s", media_root, err)

    return existing_bytes, free_bytes


class RecordingCoordinator:
    """Coordinates durable recording worker, capture tasks, and capacity retention."""

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

        self.adapter = CameraRecorderAdapter(hass, registry)
        self._intent_event = asyncio.Event()
        self._running = False
        self._worker_task: asyncio.Task[None] | None = None
        self._resource_tasks: dict[str, asyncio.Task[None]] = {}
        self._slot_semaphore = asyncio.Semaphore(MAX_PARALLEL_RESOURCES)
        self._capacity_lock = asyncio.Lock()
        self._active_reservations: int = 0
        self.admission_lock = asyncio.Lock()
        self._pending_resources: set[str] = set()
        self._lookbacks: dict[str, float] = {}
        self._retention_at = 0.0

    async def admit_record_intent(self, **kwargs: Any) -> Any:
        """Commit intake atomically with the short capture-finalization decision."""
        profile_defaults = kwargs.pop("_profile_defaults", None)
        explicit_fields = kwargs.pop("_explicit_fields", frozenset(kwargs))
        explicit_recorder_config = kwargs.pop(
            "_explicit_recorder_config", kwargs.get("recorder_config")
        )
        async with self.admission_lock:
            session = await self.store.get_resource_session(kwargs["resource_key"])
            active = bool(session and session.active_event_id)
            pinned: dict[str, Any] = {}
            if active:
                active_event = await self.store.get_event(session.active_event_id)
                active = bool(active_event and active_event.capture_state in ("requested", "recording"))
                pinned = dict((session.recorder_config or {}).get("_event_recording_profile") or {})

            profile_values = ("source_id", "label", "window_seconds", "lookback_seconds")
            if active:
                for field in profile_values:
                    if field not in explicit_fields and field in pinned:
                        kwargs[field] = pinned[field]
                if not pinned:
                    # Legacy durable sessions have no profile snapshot. Preserve their
                    # event metadata and current end time when a new caller omits values.
                    if "source_id" not in explicit_fields:
                        kwargs["source_id"] = active_event.source_id
                    if "label" not in explicit_fields:
                        kwargs["label"] = active_event.label
                    if "window_seconds" not in explicit_fields:
                        kwargs["window_seconds"] = max(
                            1.0, (active_event.desired_capture_end - utc_now()).total_seconds()
                        )
                    if "lookback_seconds" not in explicit_fields:
                        kwargs["lookback_seconds"] = 0.0

                # Profile edits do not replace the active session's capture settings.
                merged_config = dict(session.recorder_config or {})
                for key, value in (explicit_recorder_config or {}).items():
                    merged_config[key] = value
                is_camera = (
                    merged_config.get("adapter") == "camera"
                    or "camera_entity" in merged_config
                    or kwargs["resource_key"].startswith("camera.")
                )
                if is_camera:
                    merged_config["adapter"] = "camera"
                    merged_config["camera_entity"] = kwargs["resource_key"]
                kwargs["recorder_config"] = merged_config
            elif profile_defaults is not None:
                chunk = float(profile_defaults.get("chunk_seconds", self.chunk_seconds))
                maximum = float(profile_defaults.get("max_window_seconds", self.max_window_seconds))
                snapshot = {
                    "source_id": kwargs["source_id"],
                    "label": kwargs.get("label"),
                    "window_seconds": kwargs["window_seconds"],
                    "lookback_seconds": kwargs.get("lookback_seconds", 0.0),
                    "chunk_seconds": min(15.0, max(1.0, chunk)),
                    "max_window_seconds": min(300.0, max(1.0, maximum)),
                }
                config = dict(kwargs.get("recorder_config") or {})
                config["_event_recording_profile"] = snapshot
                kwargs["recorder_config"] = config
            result = await self.store.admit_record_intent(**kwargs)
        self.notify_new_intent()
        return result

    @property
    def chunk_seconds(self) -> float:
        """Return configured chunk seconds (clamped <= 15)."""
        val = self.entry.options.get(
            CONF_CHUNK_SECONDS, self.entry.data.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS)
        )
        return float(min(15.0, max(1.0, float(val))))

    @property
    def max_window_seconds(self) -> float:
        """Return maximum recording window (clamped <= 300)."""
        val = self.entry.options.get(
            CONF_MAX_WINDOW_SECONDS,
            self.entry.data.get(CONF_MAX_WINDOW_SECONDS, DEFAULT_MAX_WINDOW_SECONDS),
        )
        return float(min(300.0, max(1.0, float(val))))

    @property
    def retention_days(self) -> int:
        """Return configured retention days."""
        val = self.entry.options.get(
            CONF_RETENTION_DAYS, self.entry.data.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS)
        )
        return int(max(1, int(val)))

    @property
    def max_storage_bytes(self) -> int:
        """Return configured retention storage budget in bytes."""
        mb = self.entry.options.get(
            CONF_RETENTION_SIZE_MB,
            self.entry.data.get(CONF_RETENTION_SIZE_MB, DEFAULT_RETENTION_SIZE_MB),
        )
        return int(max(50, int(mb))) * 1024 * 1024

    def notify_new_intent(self) -> None:
        """Wake worker loop when a new record intent is admitted."""
        self._intent_event.set()

    async def start(self) -> None:
        """Start the coordinator worker and reconcile store."""
        if not self.registry.has_unsettled:
            await self.store.reconcile(worker_id=self.worker_id)
        self._running = True
        self._worker_task = asyncio.create_task(self._worker_loop(), name="event_recording_worker")

    async def stop(self) -> None:
        """Stop worker, gather active capture tasks, and drain registry before store closes."""
        self._running = False
        self._intent_event.set()

        if self._worker_task:
            self._worker_task.cancel()
            try:
                await self._worker_task
            except (asyncio.CancelledError, Exception):
                pass
            self._worker_task = None

        # 1. Gather wrapper completion first before store closes
        tasks = list(self._resource_tasks.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        # 2. Drain underlying recording tasks through registry
        await self.registry.drain(timeout_seconds=5.0)
        self._resource_tasks.clear()

    async def _worker_loop(self) -> None:
        """Continuous background worker leasing and immediately scheduling durable recording intents."""
        while self._running:
            try:
                now_mono = asyncio.get_running_loop().time()
                if now_mono >= self._retention_at:
                    await self.store.enforce_retention(
                        max_bytes=self.max_storage_bytes, max_age_days=self.retention_days
                    )
                    self._retention_at = now_mono + 120.0
                intents = await self.store.lease_record_intents(
                    worker_id=self.worker_id,
                    limit=10,
                    lease_seconds=60.0,
                )

                for intent in intents:
                    adapter_type = intent.recorder_config.get("adapter", "camera")
                    if adapter_type != "camera":
                        err_code = (
                            sanitize_capture_error("UNSUPPORTED_ADAPTER") or "UNSUPPORTED_ADAPTER"
                        )
                        await self.store.update_capture_state(
                            intent.event_id, "failed", error_code=err_code
                        )
                        await self.store.complete_record_intent(
                            intent.intent_id, self.worker_id, error=err_code
                        )
                        continue

                    resource_key = intent.resource_key
                    self._lookbacks.setdefault(intent.event_id, intent.lookback_seconds)
                    self._pending_resources.add(resource_key)
                    await self.store.complete_record_intent(intent.intent_id, self.worker_id)

                for resource_key in tuple(self._pending_resources):
                    existing_task = self._resource_tasks.get(resource_key)
                    if existing_task is None or existing_task.done():
                        session = await self.store.get_resource_session(resource_key)
                        self._pending_resources.discard(resource_key)
                        if session is None or not session.active_event_id:
                            continue
                        task = asyncio.create_task(
                            self._run_resource_capture(resource_key),
                            name=f"event_recording_capture_{resource_key}",
                        )
                        self._resource_tasks[resource_key] = task
                        task.add_done_callback(
                            lambda done, key=resource_key: self._capture_done(key, done)
                        )

                if not intents:
                    try:
                        await asyncio.wait_for(self._intent_event.wait(), timeout=2.0)
                    except TimeoutError:
                        pass
                    self._intent_event.clear()

            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error(
                    "Unexpected error in durable recording worker loop: %s",
                    sanitize_capture_error(err),
                )
                await asyncio.sleep(1.0)

    def _capture_done(self, resource_key: str, task: asyncio.Task[None]) -> None:
        if self._resource_tasks.get(resource_key) is task:
            self._resource_tasks.pop(resource_key, None)
        self._intent_event.set()

    async def _finish_capture(self, event_id: str, count: int, error: str | None) -> None:
        if count == 0:
            await self.store.update_capture_state(
                event_id, "failed", error_code=error or "ZERO_CLIPS_RECORDED"
            )
        elif error:
            await self.store.update_capture_state(event_id, "partial", error_code=error)
        else:
            await self.store.finalize_event(event_id)

    async def _run_resource_capture(self, resource_key: str) -> None:
        """Serialize chunk capture loop for a specific resource under bounded concurrency."""
        async with self._slot_semaphore:
            event_id: str | None = None
            valid_clips_count = 0
            last_error: str | None = None
            is_first_chunk = True

            try:
                session = await self.store.get_resource_session(resource_key)
                if not session or not session.active_event_id:
                    return

                event_id = session.active_event_id
                session_config = dict(session.recorder_config or {})
                profile_snapshot = dict(session_config.get("_event_recording_profile") or {})
                event: RecordedEvent | None = await self.store.get_event(event_id)
                if not event:
                    return
                valid_clips_count = sum(1 for media in event.media if media.available)

                # Calculate deadline from actual capture start to preserve occurred_at chronology for delayed sources
                capture_start = utc_now()
                max_window_seconds = min(
                    300.0,
                    max(1.0, float(profile_snapshot.get("max_window_seconds", self.max_window_seconds))),
                )
                chunk_seconds = min(
                    15.0,
                    max(1.0, float(profile_snapshot.get("chunk_seconds", self.chunk_seconds))),
                )
                max_end_time = capture_start + timedelta(seconds=max_window_seconds)

                if event.capture_state == "requested":
                    await self.store.update_capture_state(event_id, "recording")

                while self._running:
                    async with self.admission_lock:
                        now = utc_now()
                        session = await self.store.get_resource_session(resource_key)
                        if not session or session.active_event_id != event_id:
                            return
                        if now >= max_end_time or (
                            now >= session.desired_capture_end and not session.hold
                        ):
                            await self._finish_capture(event_id, valid_clips_count, last_error)
                            return
                        max_remaining = (max_end_time - now).total_seconds()
                        if max_remaining < 1.0:
                            await self._finish_capture(event_id, valid_clips_count, last_error)
                            return

                    # Bounded hold: hold cannot exceed 300s total from capture start
                    if (
                        session.hold
                        and (now - capture_start).total_seconds() >= DEFAULT_MAX_HOLD_SECONDS
                    ):
                        _LOGGER.debug("Max hold duration reached for %s", resource_key)
                        break

                    # Check quarantine
                    if self.registry.is_quarantined(resource_key):
                        last_error = "RESOURCE_QUARANTINED"
                        break

                    # Compute chunk duration (<= 15s)
                    remaining = (
                        (session.desired_capture_end - now).total_seconds()
                        if not session.hold
                        else chunk_seconds
                    )
                    chunk_dur = min(chunk_seconds, max(1.0, remaining), max_remaining)

                    # Use requested lookback for first chunk only; subsequent chunks use 0.0
                    lookback = (
                        self._lookbacks.get(event_id, 0.0)
                        if is_first_chunk
                        else 0.0
                    )
                    is_first_chunk = False

                    # Pre-chunk storage capacity and disk space pre-check under shared capacity lock
                    chunk_reservation = int(chunk_dur * ESTIMATED_BYTES_PER_SECOND)
                    async with self._capacity_lock:
                        await self.store.enforce_retention(
                            max_bytes=self.max_storage_bytes,
                            max_age_days=self.retention_days,
                        )

                        existing_bytes, free_bytes = await self.hass.async_add_executor_job(
                            _inspect_storage, self.store.media_root
                        )

                        if free_bytes is None:
                            _LOGGER.warning(
                                "Disk check failed; recording rejected fail-closed for %s",
                                resource_key,
                            )
                            last_error = "DISK_CHECK_FAILED"
                            await self.store.update_capture_state(
                                event_id, "failed", error_code=last_error
                            )
                            break

                        if free_bytes < MIN_DISK_FREE_BYTES:
                            _LOGGER.warning(
                                "Disk space exhausted (%d free); recording rejected for %s",
                                free_bytes,
                                resource_key,
                            )
                            last_error = "DISK_SPACE_EXHAUSTED"
                            await self.store.update_capture_state(
                                event_id, "failed", error_code=last_error
                            )
                            break

                        total_projected = (
                            existing_bytes + self._active_reservations + chunk_reservation
                        )
                        if total_projected > self.max_storage_bytes:
                            _LOGGER.warning(
                                "Storage capacity budget reached (existing=%d, active=%d, chunk=%d, max=%d); recording rejected for %s",
                                existing_bytes,
                                self._active_reservations,
                                chunk_reservation,
                                self.max_storage_bytes,
                                resource_key,
                            )
                            last_error = "STORAGE_CAPACITY_EXCEEDED"
                            await self.store.update_capture_state(
                                event_id, "failed", error_code=last_error
                            )
                            break

                        self._active_reservations += chunk_reservation

                    staging_dir = self.store.media_root / ".staging"
                    staging_path = staging_dir / f"{event_id}_{uuid.uuid4().hex[:8]}.mp4"
                    chunk_recorded = False

                    try:
                        await self.adapter.record_chunk(
                            camera_entity=resource_key,
                            target_path=staging_path,
                            duration_seconds=chunk_dur,
                            lookback_seconds=lookback,
                        )
                        chunk_recorded = True
                    except ResourceQuarantinedError:
                        last_error = "RESOURCE_QUARANTINED"
                        break
                    except RecorderError as err:
                        last_error = sanitize_capture_error(err) or "RECORDER_ERROR"
                        break
                    finally:
                        async with self._capacity_lock:
                            self._active_reservations = max(
                                0, self._active_reservations - chunk_reservation
                            )

                    # Off-loop file validation
                    def _inspect_staging() -> tuple[bool, int]:
                        if not staging_path.is_file():
                            return False, 0
                        return True, staging_path.stat().st_size

                    file_exists, file_size = await self.hass.async_add_executor_job(
                        _inspect_staging
                    )

                    if chunk_recorded and file_exists and file_size > 0:
                        try:
                            # Native chunks ALWAYS mark is_uncertain=True
                            await self.store.attach_and_promote_media(
                                event_id=event_id,
                                src_path=staging_path,
                                kind="video",
                                mime_type="video/mp4",
                                is_uncertain=True,
                                finalize=False,
                            )
                            valid_clips_count += 1
                        except Exception as prom_err:
                            last_error = (
                                sanitize_capture_error(prom_err) or "MEDIA_PROMOTION_FAILED"
                            )
                            _LOGGER.error("Failed to promote media: %s", last_error)
                            break
                        finally:
                            # Cleanup owned staging file after native settled and attached, not while quarantined
                            if not self.registry.is_quarantined(resource_key):
                                await self.hass.async_add_executor_job(
                                    lambda: staging_path.unlink(missing_ok=True)
                                )
                    else:
                        last_error = "EMPTY_RECORDING_OUTPUT"
                        break

                # Conclude capture state: ZERO clips MUST NOT finalize READY
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
            except Exception as err:
                clean_err = sanitize_capture_error(err) or "UNEXPECTED_CAPTURE_ERROR"
                _LOGGER.error("Capture loop failure for %s: %s", resource_key, clean_err)
                if event_id:
                    final_state = "partial" if valid_clips_count > 0 else "failed"
                    await self.store.update_capture_state(
                        event_id, final_state, error_code=clean_err
                    )
            finally:
                if event_id:
                    self._lookbacks.pop(event_id, None)
