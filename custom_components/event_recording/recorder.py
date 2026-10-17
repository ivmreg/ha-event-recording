"""Recorder adapter and quarantine registry for Event Recording.

Serializes native camera.record calls per resource_key, coordinates stream aliases
sharing physical streams via ownership groups, and enforces fail-closed quarantine
across task cancellations and integration reloads.
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
import re
import urllib.parse
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

    from homeassistant.core import HomeAssistant

from .const import DEFAULT_WATCHDOG_PADDING_SECONDS
from .core.models import sanitize_capture_error

_LOGGER = logging.getLogger(__name__)


class RecorderError(Exception):
    """Base exception for camera recorder adapter failures."""


class ResourceBusyError(RecorderError):
    """Raised when capture resource is already active with another owner."""


class ResourceQuarantinedError(RecorderError):
    """Raised when capture resource is quarantined awaiting an unsettled task."""


class BufferUnavailableError(RecorderError):
    """Raised when external buffer export endpoint is unavailable or returns 404."""


class RecorderRegistry:
    """Shared registry of locks, active tasks, and quarantined tasks across integration reloads.

    Always preserves canonical camera entity ownership while also coordinating
    shared stream ownership groups and tracking physical aliases.
    """

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._active_by_camera: dict[str, asyncio.Task[Any]] = {}
        self._quarantined_by_camera: dict[str, asyncio.Task[Any]] = {}
        self._active_by_group: dict[str, asyncio.Task[Any]] = {}
        self._quarantined_by_group: dict[str, asyncio.Task[Any]] = {}
        self._camera_to_groups: dict[str, set[str]] = {}
        self._group_to_cameras: dict[str, set[str]] = {}
        self._quarantined_reservations: dict[str, Any] = {}
        self._bound_ledgers: list[Any] = []
        self._quarantined_staging_files: dict[str, Path] = {}
        self._quarantined_sidecar_files: dict[str, tuple[Path, ...]] = {}
        self._settled_callbacks: list[Any] = []

    def register_settled_callback(self, cb: Any) -> None:
        """Register callback invoked whenever a quarantined writer settles."""
        if cb not in self._settled_callbacks:
            self._settled_callbacks.append(cb)

    def unregister_settled_callback(self, cb: Any) -> None:
        """Unregister settled callback."""
        if cb in self._settled_callbacks:
            self._settled_callbacks.remove(cb)

    def _notify_settled(self) -> None:
        """Notify registered listeners when quarantined task settles or capacity changes."""
        for cb in list(self._settled_callbacks):
            try:
                cb()
            except Exception as exc:
                _LOGGER.warning("Error in settled callback: %s", exc)

    def get_lock_key(self, camera_entity: str, ownership_group: str | None = None) -> str:
        """Derive locking key for camera and optional ownership group."""
        if ownership_group and ownership_group.strip():
            return f"group:{ownership_group.strip()}"
        return camera_entity

    def get_lock(self, camera_entity: str, ownership_group: str | None = None) -> asyncio.Lock:
        """Get or create asyncio lock for a camera entity or group key."""
        key = self.get_lock_key(camera_entity, ownership_group)
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    @property
    def has_unsettled(self) -> bool:
        """Prevent startup staging cleanup while any native writer is still alive."""
        all_tasks = (
            *self._active_by_camera.values(),
            *self._quarantined_by_camera.values(),
            *self._active_by_group.values(),
            *self._quarantined_by_group.values(),
        )
        return any(not t.done() for t in all_tasks)

    @property
    def active_resources(self) -> list[str]:
        """List currently active camera entity and group keys."""
        cams = [k for k, t in self._active_by_camera.items() if not t.done()]
        grps = [k for k, t in self._active_by_group.items() if not t.done()]
        return sorted(set(cams + grps))

    @property
    def quarantined_resources(self) -> list[str]:
        """List currently quarantined camera entity and group keys."""
        cams = [k for k, t in self._quarantined_by_camera.items() if not t.done()]
        grps = [k for k, t in self._quarantined_by_group.items() if not t.done()]
        return sorted(set(cams + grps))

    @property
    def quarantined_count(self) -> int:
        """Return distinct count of unsettled quarantined background tasks."""
        all_tasks = {
            *self._quarantined_by_camera.values(),
            *self._quarantined_by_group.values(),
        }
        return sum(1 for t in all_tasks if not t.done())

    def bind_ledger(self, ledger: Any) -> None:
        """Register a storage ledger to receive quarantine settlement events."""
        if ledger not in self._bound_ledgers:
            self._bound_ledgers.append(ledger)

    def unbind_ledger(self, ledger: Any) -> None:
        """Remove a storage ledger when coordinator unloads, unless it holds active quarantined reservations."""
        if ledger in self._bound_ledgers:
            if hasattr(ledger, "get_all_reservations") and any(
                r.is_quarantined for r in ledger.get_all_reservations().values()
            ):
                return
            self._bound_ledgers.remove(ledger)

    def get_quarantined_task(
        self, camera_entity: str, ownership_group: str | None = None
    ) -> asyncio.Task[Any] | None:
        """Return active quarantined task for camera or group if any."""
        t = self._quarantined_by_camera.get(camera_entity)
        if t is not None and not t.done():
            return t
        if ownership_group and ownership_group.strip():
            g_key = f"group:{ownership_group.strip()}"
            t_grp = self._quarantined_by_group.get(g_key)
            if t_grp is not None and not t_grp.done():
                return t_grp
        return None

    def register_quarantined_reservation(
        self,
        res_id: str,
        res: Any,
        staging_path: Path | None = None,
        sidecar_paths: tuple[Path, ...] = (),
        task: asyncio.Task[Any] | None = None,
    ) -> None:
        """Track an active reservation and optional staging/sidecar paths associated with an unsettled quarantined writer."""
        self._quarantined_reservations[res_id] = res
        if staging_path:
            self._quarantined_staging_files[res_id] = Path(staging_path)
        if sidecar_paths:
            self._quarantined_sidecar_files[res_id] = tuple(Path(p) for p in sidecar_paths)

        if task is not None:
            def _settle_task(t: asyncio.Task[Any]) -> None:
                self.settle_quarantined_reservation(res_id)

            task.add_done_callback(_settle_task)

    def settle_quarantined_reservation(self, res_id: str) -> None:
        """Atomically settle a quarantined reservation across all bound ledgers off the event loop."""
        stg = self._quarantined_staging_files.pop(res_id, None)
        sidecars = self._quarantined_sidecar_files.pop(res_id, ())

        def _do_unlink() -> bool:
            success = True
            files_to_clean: list[Path] = []
            if stg is not None:
                files_to_clean.append(stg)
            files_to_clean.extend(sidecars)
            for f in files_to_clean:
                try:
                    f.unlink(missing_ok=True)
                except Exception as exc:
                    _LOGGER.warning("Failed to unlink quarantined file %s: %s", f, exc)
                    success = False
            return success

        def _on_cleaned(success: bool) -> None:
            if success:
                self._quarantined_reservations.pop(res_id, None)
                for ledger in list(self._bound_ledgers):
                    try:
                        ledger.release_reservation(res_id, force=True)
                    except Exception:
                        pass
                self._notify_settled()
            else:
                _LOGGER.warning(
                    "Quarantined reservation %s retained accounting due to failed disk cleanup",
                    res_id,
                )

        if stg is None and not sidecars:
            _on_cleaned(True)
            return

        try:
            loop = asyncio.get_running_loop()
            fut = loop.run_in_executor(None, _do_unlink)
            fut.add_done_callback(
                lambda f: _on_cleaned(
                    f.result() if not f.cancelled() and not f.exception() else False
                )
            )
        except RuntimeError:
            success = _do_unlink()
            _on_cleaned(success)

    def get_all_quarantined_reservations(self) -> dict[str, Any]:
        """Return all tracked quarantined reservations for transfer across coordinator reloads."""
        return dict(self._quarantined_reservations)

    def remove_quarantined_reservation(self, res_id: str) -> None:
        """Remove a settled quarantined reservation."""
        self.settle_quarantined_reservation(res_id)

    def is_quarantined(self, camera_entity: str, ownership_group: str | None = None) -> bool:
        """Check if camera, any associated group, or peer alias has an unsettled quarantined task.

        Preserves canonical camera ownership: changing or removing ownership group
        does NOT bypass an unsettled quarantine on the underlying camera entity or peer aliases!
        """
        # 1. Canonical camera entity check
        q_cam = self._quarantined_by_camera.get(camera_entity)
        if q_cam is not None:
            if not q_cam.done():
                return True
            del self._quarantined_by_camera[camera_entity]

        # 2. Historical or current groups associated with this camera
        associated_groups = set(self._camera_to_groups.get(camera_entity, set()))
        if ownership_group and ownership_group.strip():
            associated_groups.add(f"group:{ownership_group.strip()}")

        for grp_key in list(associated_groups):
            q_grp = self._quarantined_by_group.get(grp_key)
            if q_grp is not None:
                if not q_grp.done():
                    return True
                del self._quarantined_by_group[grp_key]

            # Also check all peer cameras associated with this group!
            for peer_cam in list(self._group_to_cameras.get(grp_key, set())):
                q_peer = self._quarantined_by_camera.get(peer_cam)
                if q_peer is not None:
                    if not q_peer.done():
                        return True
                    del self._quarantined_by_camera[peer_cam]

        return False

    def is_busy(self, camera_entity: str, ownership_group: str | None = None) -> bool:
        """Return True if camera, its group, or any peer alias currently has an active task."""
        act_cam = self._active_by_camera.get(camera_entity)
        if act_cam and not act_cam.done():
            return True

        associated_groups = set(self._camera_to_groups.get(camera_entity, set()))
        if ownership_group and ownership_group.strip():
            associated_groups.add(f"group:{ownership_group.strip()}")

        for grp_key in associated_groups:
            act_grp = self._active_by_group.get(grp_key)
            if act_grp and not act_grp.done():
                return True
            for peer_cam in self._group_to_cameras.get(grp_key, set()):
                p_act = self._active_by_camera.get(peer_cam)
                if p_act and not p_act.done():
                    return True

        return False

    def register_active(
        self,
        camera_entity: str,
        task: asyncio.Task[Any],
        ownership_group: str | None = None,
    ) -> None:
        """Register active recording task for canonical camera and optional ownership group."""
        self._active_by_camera[camera_entity] = task
        if ownership_group and ownership_group.strip():
            g_key = f"group:{ownership_group.strip()}"
            self._active_by_group[g_key] = task
            self._camera_to_groups.setdefault(camera_entity, set()).add(g_key)
            self._group_to_cameras.setdefault(g_key, set()).add(camera_entity)

    def quarantine(
        self,
        camera_entity: str,
        task: asyncio.Task[Any],
        ownership_group: str | None = None,
        reservation: Any | None = None,
    ) -> None:
        """Place task in quarantine until settled."""
        self._quarantined_by_camera[camera_entity] = task
        g_key: str | None = None
        if ownership_group and ownership_group.strip():
            g_key = f"group:{ownership_group.strip()}"
            self._quarantined_by_group[g_key] = task
            self._camera_to_groups.setdefault(camera_entity, set()).add(g_key)
            self._group_to_cameras.setdefault(g_key, set()).add(camera_entity)

        if reservation is not None:
            res_id = getattr(reservation, "reservation_id", str(reservation))
            self._quarantined_reservations[res_id] = reservation

        def _consume(t: asyncio.Task[Any]) -> None:
            try:
                t.exception()
            except (asyncio.CancelledError, Exception):
                pass
            if self._quarantined_by_camera.get(camera_entity) is t:
                self._quarantined_by_camera.pop(camera_entity, None)
            if g_key and self._quarantined_by_group.get(g_key) is t:
                self._quarantined_by_group.pop(g_key, None)
            self._notify_settled()
            if reservation is not None:
                res_id = getattr(reservation, "reservation_id", str(reservation))
                self.settle_quarantined_reservation(res_id)

        task.add_done_callback(_consume)

    def release_active(
        self,
        camera_entity: str,
        task: asyncio.Task[Any],
        ownership_group: str | None = None,
    ) -> None:
        """Release active task reference if matching."""
        if self._active_by_camera.get(camera_entity) is task:
            del self._active_by_camera[camera_entity]
        if ownership_group and ownership_group.strip():
            g_key = f"group:{ownership_group.strip()}"
            if self._active_by_group.get(g_key) is task:
                del self._active_by_group[g_key]

    @asynccontextmanager
    async def acquire_locks(
        self,
        camera_entity: str,
        ownership_group: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> AsyncIterator[None]:
        """Acquire camera entity and group locks in deterministic order with bounded wait.

        Allows stream aliases to queue cleanly and wait for an in-flight chunk rather
        than failing immediately with ResourceBusyError.
        """
        keys = [camera_entity]
        associated_groups = set(self._camera_to_groups.get(camera_entity, set()))
        if ownership_group and ownership_group.strip():
            associated_groups.add(f"group:{ownership_group.strip()}")
        keys.extend(associated_groups)
        sorted_keys = sorted(set(keys))

        loop = asyncio.get_running_loop()
        deadline = loop.time() + max(0.1, timeout_seconds)
        acquired_locks: list[asyncio.Lock] = []

        try:
            for k in sorted_keys:
                lock = self.get_lock(k)
                remaining = deadline - loop.time()
                if remaining <= 0:
                    raise ResourceBusyError(
                        f"Timed out waiting for lock on '{k}' (resource '{camera_entity}' busy)"
                    )
                try:
                    await asyncio.wait_for(lock.acquire(), timeout=remaining)
                    acquired_locks.append(lock)
                except TimeoutError as err:
                    raise ResourceBusyError(
                        f"Timed out acquiring lock on '{k}' for '{camera_entity}'"
                    ) from err
            if self.is_quarantined(camera_entity, ownership_group):
                raise ResourceQuarantinedError(
                    f"Resource '{camera_entity}' is quarantined awaiting previous task release."
                )
            yield
        finally:
            for lock in reversed(acquired_locks):
                lock.release()

    async def drain(self, timeout_seconds: float = 5.0) -> bool:
        """Wait for all active and quarantined tasks to settle."""
        tasks = [
            t
            for t in (
                *self._active_by_camera.values(),
                *self._quarantined_by_camera.values(),
                *self._active_by_group.values(),
                *self._quarantined_by_group.values(),
            )
            if not t.done()
        ]
        if not tasks:
            return True

        _done, pending = await asyncio.wait(tasks, timeout=timeout_seconds)
        return len(pending) == 0


class CameraRecorderAdapter:
    """Invokes native Home Assistant camera.record service off the event loop.

    Enforces path containment, fail-closed quarantine on timeout/cancellation,
    and safe serialization across stream aliases sharing physical camera feeds.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        registry: RecorderRegistry,
        timeout_padding_seconds: float = DEFAULT_WATCHDOG_PADDING_SECONDS,
    ) -> None:
        self.hass = hass
        self.registry = registry
        self.padding = timeout_padding_seconds

    async def record_chunk(
        self,
        camera_entity: str,
        target_path: Path | str,
        duration_seconds: float,
        lookback_seconds: float = 0.0,
        ownership_group: str | None = None,
        lock_timeout_seconds: float = 15.0,
    ) -> None:
        """Execute a single camera.record chunk with quarantine and alias coordination."""
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

        if self.registry.is_quarantined(camera_entity, ownership_group):
            raise ResourceQuarantinedError(
                f"Resource '{camera_entity}' is quarantined awaiting previous task release."
            )

        async with self.registry.acquire_locks(
            camera_entity, ownership_group, timeout_seconds=lock_timeout_seconds
        ):
            service_data: dict[str, Any] = {
                "entity_id": camera_entity,
                "filename": str(resolved_path),
                "duration": int(duration_seconds),
            }
            if lookback_seconds > 0:
                service_data["lookback"] = int(lookback_seconds)

            record_task = asyncio.create_task(
                self.hass.services.async_call(
                    "camera",
                    "record",
                    service_data,
                    blocking=True,
                )
            )

            self.registry.register_active(camera_entity, record_task, ownership_group)

            try:
                watchdog_timeout = duration_seconds + lookback_seconds + self.padding
                await asyncio.wait_for(asyncio.shield(record_task), timeout=watchdog_timeout)
            except (asyncio.CancelledError, TimeoutError) as err:
                if not record_task.done():
                    self.registry.quarantine(camera_entity, record_task, ownership_group)
                    _LOGGER.warning(
                        "Camera recording for %s was interrupted/cancelled; quarantined until settled.",
                        camera_entity,
                    )
                if isinstance(err, TimeoutError):
                    raise RecorderError("TIMEOUT") from err
                raise
            except Exception as err:
                clean_err = sanitize_capture_error(err) or "CAMERA_RECORD_FAILED"
                _LOGGER.error("Camera recording failed for %s: %s", camera_entity, clean_err)
                raise RecorderError(clean_err) from err
            finally:
                self.registry.release_active(camera_entity, record_task, ownership_group)


class SnapshotRecorderAdapter:
    """Captures still snapshot images via Home Assistant camera.async_get_image.

    Enforces target path containment, safe image probing, off-loop non-blocking writes,
    bounded file size caps, and fail-closed quarantine on cancellation/timeout.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        registry: RecorderRegistry,
        timeout_seconds: float = 15.0,
        max_bytes: int = 10 * 1024 * 1024,  # 10 MiB snapshot cap
    ) -> None:
        self.hass = hass
        self.registry = registry
        self.timeout = timeout_seconds
        self.max_bytes = max_bytes

    async def capture_snapshot(
        self,
        camera_entity: str,
        target_path: Path | str,
        ownership_group: str | None = None,
        lock_timeout_seconds: float = 15.0,
    ) -> dict[str, Any]:
        """Execute a single still snapshot with format detection and safe off-loop writes."""
        resolved_path = await self.hass.async_add_executor_job(Path(target_path).resolve)
        await self.hass.async_add_executor_job(
            lambda: resolved_path.parent.mkdir(parents=True, exist_ok=True)
        )

        is_allowed = await self.hass.async_add_executor_job(
            self.hass.config.is_allowed_path, str(resolved_path)
        )
        if not is_allowed:
            _LOGGER.error(
                "Snapshot target path '%s' is not within Home Assistant allowlist_external_dirs",
                resolved_path,
            )
            raise RecorderError("MEDIA_ROOT_NOT_ALLOWED")

        if self.registry.is_quarantined(camera_entity, ownership_group):
            raise ResourceQuarantinedError(
                f"Resource '{camera_entity}' is quarantined awaiting previous task release."
            )

        async with self.registry.acquire_locks(
            camera_entity, ownership_group, timeout_seconds=lock_timeout_seconds
        ):
            async def _do_snapshot() -> dict[str, Any]:
                from homeassistant.components.camera import async_get_image

                image = await async_get_image(self.hass, camera_entity)
                content = image.content
                if not content or len(content) < 100:
                    raise RecorderError("EMPTY_SNAPSHOT_OUTPUT")
                if len(content) > self.max_bytes:
                    raise RecorderError("SNAPSHOT_EXCEEDS_MAX_SIZE")

                # Probe content type; do not falsely claim JPEG for PNG
                if content.startswith(b"\xff\xd8\xff"):
                    mime_type = "image/jpeg"
                elif content.startswith(b"\x89PNG\r\n\x1a\n"):
                    mime_type = "image/png"
                else:
                    raise RecorderError("UNRECOGNIZED_IMAGE_FORMAT")

                await self.hass.async_add_executor_job(resolved_path.write_bytes, content)
                return {
                    "mime_type": mime_type,
                    "size_bytes": len(content),
                    "capture_start": None,
                    "capture_end": None,
                    "time_quality": "unknown",
                    "clock_source": "unknown",
                }

            op_task = asyncio.create_task(_do_snapshot())
            self.registry.register_active(camera_entity, op_task, ownership_group)

            try:
                return await asyncio.wait_for(asyncio.shield(op_task), timeout=self.timeout)
            except (asyncio.CancelledError, TimeoutError) as err:
                if not op_task.done():
                    self.registry.quarantine(camera_entity, op_task, ownership_group)
                if isinstance(err, TimeoutError):
                    raise RecorderError("TIMEOUT") from err
                raise
            except Exception as err:
                clean_err = sanitize_capture_error(err) or "CAMERA_SNAPSHOT_FAILED"
                _LOGGER.error("Camera snapshot failed for %s: %s", camera_entity, clean_err)
                raise RecorderError(clean_err) from err
            finally:
                self.registry.release_active(camera_entity, op_task, ownership_group)


class FrigateBufferedAdapter:
    """Buffered recording provider adapter exporting clips from Frigate NVR HTTP API.

    Calls GET /api/<camera_name>/start/<start_ts>/end/<end_ts>/clip.mp4.
    Requires explicit base_url and camera_name (no implicit defaults).
    Enforces path traversal rejection, redirect blocking, streaming size caps,
    and safe off-loop filesystem cleanup before unlinking on Windows.
    """

    def __init__(
        self,
        base_url: str | None = None,
        auth_token: str | None = None,
        session: Any | None = None,
        timeout_seconds: float = 30.0,
        max_bytes: int = 100 * 1024 * 1024,
    ) -> None:
        if base_url and base_url.strip():
            b_url = base_url.strip().rstrip("/")
            parsed = urllib.parse.urlsplit(b_url)
            if parsed.scheme not in ("http", "https"):
                raise RecorderError("INVALID_URL_SCHEME")
            if parsed.username or parsed.password:
                raise RecorderError("CREDENTIALS_IN_URL_NOT_ALLOWED")
            if parsed.query or parsed.fragment:
                raise RecorderError("QUERY_OR_FRAGMENT_NOT_ALLOWED")
            self.base_url = b_url
        else:
            self.base_url = None

        self.auth_token = auth_token.strip() if auth_token and auth_token.strip() else None
        self._session = session
        self.timeout = timeout_seconds
        self.max_bytes = max_bytes

    async def check_recordings_coverage(
        self,
        camera_name: str,
        start_ts: float,
        end_ts: float,
    ) -> dict[str, Any]:
        """Check recordings endpoint to verify coverage before clip export.

        Calls GET /api/<camera_name>/recordings?after=<start_ts>&before=<end_ts>
        and parses segment objects to verify full contiguous coverage.
        """
        if not self.base_url:
            return {"configured": False, "has_coverage": False, "is_complete": False}

        clean_name = camera_name.strip()
        if not re.match(r"^[a-zA-Z0-9_-]+$", clean_name):
            raise RecorderError("INVALID_CAMERA_NAME")

        import aiohttp

        url = f"{self.base_url}/api/{urllib.parse.quote(clean_name)}/recordings"
        params = {"after": str(int(start_ts)), "before": str(int(end_ts))}
        headers: dict[str, str] = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        close_session = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        try:
            async with session.get(
                url,
                params=params,
                headers=headers,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 200:
                    max_bytes = 1024 * 1024  # 1 MiB
                    chunks: list[bytes] = []
                    total_bytes = 0
                    while total_bytes <= max_bytes:
                        chunk = await resp.content.read(min(64 * 1024, max_bytes + 1 - total_bytes))
                        if not isinstance(chunk, bytes):
                            raise RecorderError("INVALID_COVERAGE_RESPONSE")
                        if not chunk:
                            break
                        total_bytes += len(chunk)
                        if total_bytes > max_bytes:
                            return {"configured": True, "has_coverage": False, "is_complete": False}
                        chunks.append(chunk)
                    raw_body = b"".join(chunks)

                    try:
                        data = json.loads(raw_body.decode("utf-8")) if raw_body else []
                    except Exception:
                        return {"configured": True, "has_coverage": False, "is_complete": False}

                    if not isinstance(data, list):
                        return {"configured": True, "has_coverage": False, "is_complete": False}

                    segments = data[:500]  # Bounded item processing
                    intervals: list[tuple[float, float]] = []
                    for seg in segments:
                        if isinstance(seg, dict):
                            try:
                                s_time = float(seg.get("start_time", 0.0))
                                dur = float(seg.get("duration", 0.0))
                                e_time = float(seg.get("end_time", s_time + dur))
                                if not (math.isfinite(s_time) and math.isfinite(e_time)):
                                    continue
                            except (ValueError, TypeError):
                                continue
                            c_start = max(start_ts, s_time)
                            c_end = min(end_ts, e_time)
                            if c_start < c_end:
                                intervals.append((c_start, c_end))

                    intervals.sort(key=lambda x: x[0])
                    merged: list[tuple[float, float]] = []
                    for s, e in intervals:
                        if not merged or s > merged[-1][1] + 1.0:
                            merged.append((s, e))
                        else:
                            merged[-1] = (merged[-1][0], max(merged[-1][1], e))

                    total_covered = sum(e - s for s, e in merged)
                    req_duration = max(0.1, end_ts - start_ts)
                    is_complete = bool(
                        merged
                        and merged[0][0] <= start_ts + 1.0
                        and merged[-1][1] >= end_ts - 1.0
                        and len(merged) == 1
                    )
                    gaps: list[dict[str, float]] = []
                    if merged:
                        if merged[0][0] > start_ts + 1.0:
                            gaps.append({"start": start_ts, "end": merged[0][0]})
                        for i in range(len(merged) - 1):
                            gaps.append({"start": merged[i][1], "end": merged[i + 1][0]})
                        if merged[-1][1] < end_ts - 1.0:
                            gaps.append({"start": merged[-1][1], "end": end_ts})
                    else:
                        gaps.append({"start": start_ts, "end": end_ts})

                    gap_quality = (
                        "complete"
                        if is_complete
                        else ("partial" if total_covered > 0 else "empty")
                    )

                    return {
                        "configured": True,
                        "has_coverage": len(merged) > 0,
                        "is_complete": is_complete,
                        "gap_quality": gap_quality,
                        "segments_count": len(segments),
                        "requested_duration": round(req_duration, 2),
                        "covered_duration": round(total_covered, 2),
                        "actual_start": merged[0][0] if merged else None,
                        "actual_end": merged[-1][1] if merged else None,
                        "gaps": gaps if not is_complete else [],
                    }
                return {"configured": True, "has_coverage": False, "is_complete": False}
        except Exception as err:
            _LOGGER.debug("Coverage preflight check notice for %s: %s", clean_name, err)
            return {"configured": True, "has_coverage": False, "is_complete": False}
        finally:
            if close_session:
                await session.close()

    async def export_clip(
        self,
        camera_name: str,
        start_ts: float,
        end_ts: float,
        target_path: Path | str,
    ) -> dict[str, Any]:
        """Export retained clip window from Frigate HTTP API with bounded stream cap."""
        if not self.base_url:
            raise BufferUnavailableError("Frigate buffered provider not configured (missing base_url)")

        clean_name = camera_name.strip()
        if not re.match(r"^[a-zA-Z0-9_-]+$", clean_name):
            raise RecorderError("INVALID_CAMERA_NAME")

        resolved_path = await asyncio.to_thread(Path(target_path).resolve)
        await asyncio.to_thread(lambda: resolved_path.parent.mkdir(parents=True, exist_ok=True))

        import aiohttp

        quoted_name = urllib.parse.quote(clean_name)
        url = f"{self.base_url}/api/{quoted_name}/start/{int(start_ts)}/end/{int(end_ts)}/clip.mp4"

        headers: dict[str, str] = {}
        if self.auth_token:
            headers["Authorization"] = f"Bearer {self.auth_token}"

        close_session = False
        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            close_session = True

        file_handle: Any | None = None
        download_success = False
        try:
            async with session.get(
                url,
                headers=headers,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=self.timeout),
            ) as resp:
                if resp.status == 404:
                    raise BufferUnavailableError("BUFFER_UNAVAILABLE")
                if resp.status in (301, 302, 303, 307, 308):
                    raise RecorderError("REDIRECT_NOT_ALLOWED")
                if resp.status != 200:
                    raise RecorderError(f"HTTP_{resp.status}")

                file_handle = await asyncio.to_thread(open, resolved_path, "wb")
                total_downloaded = 0
                async for chunk in resp.content.iter_chunked(65536):
                    total_downloaded += len(chunk)
                    if total_downloaded > self.max_bytes:
                        raise RecorderError("EXPORT_SIZE_EXCEEDED")
                    await asyncio.to_thread(file_handle.write, chunk)

                await asyncio.to_thread(file_handle.close)
                file_handle = None

                if total_downloaded == 0:
                    raise BufferUnavailableError("EMPTY_BUFFER_EXPORT")

                download_success = True
                return {
                    "downloaded_bytes": total_downloaded,
                    "clock_source": "frigate_nvr",
                    "time_quality": "provider_reported",
                    "time_uncertainty_seconds": None,
                }
        except (aiohttp.ClientError, TimeoutError) as err:
            raise BufferUnavailableError(f"Frigate connection error: {err}") from err
        finally:
            if file_handle is not None:
                try:
                    await asyncio.to_thread(file_handle.close)
                except Exception:
                    pass
            if not download_success:
                await asyncio.to_thread(resolved_path.unlink, missing_ok=True)
            if close_session:
                await session.close()
