"""Recorder adapter and quarantine registry for Event Recording.

Serializes native camera.record calls per resource_key, coalesces recording windows,
and enforces fail-closed quarantine across task cancellations and entry reloads.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

from .core.models import sanitize_capture_error

_LOGGER = logging.getLogger(__name__)


class RecorderError(Exception):
    """Base exception for camera recorder adapter failures."""


class ResourceBusyError(RecorderError):
    """Raised when capture resource is already active with another owner."""


class ResourceQuarantinedError(RecorderError):
    """Raised when capture resource is quarantined awaiting an unsettled task."""


class RecorderRegistry:
    """Shared registry of locks, active tasks, and quarantined tasks across integration reloads."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._active_tasks: dict[str, asyncio.Task[Any]] = {}
        self._quarantined_tasks: dict[str, asyncio.Task[Any]] = {}

    def get_lock(self, resource_key: str) -> asyncio.Lock:
        """Get or create asyncio lock for resource."""
        if resource_key not in self._locks:
            self._locks[resource_key] = asyncio.Lock()
        return self._locks[resource_key]

    @property
    def has_unsettled(self) -> bool:
        """Prevent startup staging cleanup while a native writer is still alive."""
        return any(not t.done() for t in (*self._active_tasks.values(), *self._quarantined_tasks.values()))

    def is_busy(self, resource_key: str) -> bool:
        """Return True if resource currently has an active or quarantined task."""
        task = self._active_tasks.get(resource_key)
        if task and not task.done():
            return True
        q_task = self._quarantined_tasks.get(resource_key)
        if q_task and not q_task.done():
            return True
        return False

    def is_quarantined(self, resource_key: str) -> bool:
        """Check if resource currently has an unsettled quarantined task."""
        q_task = self._quarantined_tasks.get(resource_key)
        if q_task is None:
            return False
        if q_task.done():
            del self._quarantined_tasks[resource_key]
            return False
        return True

    def register_active(self, resource_key: str, task: asyncio.Task[Any]) -> None:
        """Register active recording task for resource."""
        self._active_tasks[resource_key] = task

    def quarantine(self, resource_key: str, task: asyncio.Task[Any]) -> None:
        """Place task in quarantine until settled."""
        self._quarantined_tasks[resource_key] = task

        def _consume(t: asyncio.Task[Any]) -> None:
            try:
                t.exception()
            except (asyncio.CancelledError, Exception):
                pass

        task.add_done_callback(_consume)

    def release_active(self, resource_key: str, task: asyncio.Task[Any]) -> None:
        """Release active task reference if matching."""
        if self._active_tasks.get(resource_key) is task:
            del self._active_tasks[resource_key]

    async def drain(self, timeout_seconds: float = 5.0) -> bool:
        """Wait for all active and quarantined tasks to settle."""
        tasks = [
            t
            for t in list(self._active_tasks.values()) + list(self._quarantined_tasks.values())
            if not t.done()
        ]
        if not tasks:
            return True
        _done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
        return len(pending) == 0


class CameraRecorderAdapter:
    """Optional native camera recorder adapter invoking camera.record."""

    def __init__(
        self,
        hass: HomeAssistant,
        registry: RecorderRegistry,
        timeout_padding_seconds: float = 20.0,
    ) -> None:
        self.hass = hass
        self.registry = registry
        self.timeout_padding = timeout_padding_seconds

    async def record_chunk(
        self,
        camera_entity: str,
        target_path: Path | str,
        duration_seconds: float,
        lookback_seconds: float = 0.0,
    ) -> None:
        """Execute a single camera.record chunk with quarantine safeguards."""
        resolved_path = await self.hass.async_add_executor_job(Path(target_path).resolve)
        await self.hass.async_add_executor_job(
            lambda: resolved_path.parent.mkdir(parents=True, exist_ok=True)
        )

        is_allowed = await self.hass.async_add_executor_job(
            self.hass.config.is_allowed_path, str(resolved_path)
        )
        if not is_allowed:
            _LOGGER.error(
                "Recording target path '%s' is not within Home Assistant allowlist_external_dirs",
                resolved_path,
            )
            raise RecorderError("MEDIA_ROOT_NOT_ALLOWED")

        if self.registry.is_quarantined(camera_entity):
            raise ResourceQuarantinedError(
                f"Resource '{camera_entity}' is quarantined awaiting previous task release."
            )

        lock = self.registry.get_lock(camera_entity)
        if lock.locked():
            raise ResourceBusyError(
                f"Resource '{camera_entity}' currently has an active recording owner."
            )

        async with lock:

            async def _invoke_camera_record() -> None:
                await self.hass.services.async_call(
                    "camera",
                    "record",
                    {
                        "entity_id": camera_entity,
                        "filename": str(resolved_path),
                        "duration": int(max(1, duration_seconds)),
                        "lookback": int(max(0, lookback_seconds)),
                    },
                    blocking=True,
                )

            record_task = asyncio.create_task(_invoke_camera_record())
            self.registry.register_active(camera_entity, record_task)

            expected_timeout = float(duration_seconds + lookback_seconds + self.timeout_padding)

            try:
                await asyncio.wait_for(asyncio.shield(record_task), timeout=expected_timeout)
            except (asyncio.CancelledError, TimeoutError) as err:
                if not record_task.done():
                    self.registry.quarantine(camera_entity, record_task)
                    _LOGGER.warning(
                        "Camera recording for %s was interrupted/cancelled; quarantined until settled.",
                        camera_entity,
                    )
                if isinstance(err, TimeoutError):
                    raise RecorderError("TIMEOUT") from err
                raise
            except Exception as err:
                clean_err = sanitize_capture_error(err) or "CAMERA_RECORD_FAILED"
                _LOGGER.error("Native camera.record failed for %s: %s", camera_entity, clean_err)
                raise RecorderError(clean_err) from err
            finally:
                self.registry.release_active(camera_entity, record_task)
