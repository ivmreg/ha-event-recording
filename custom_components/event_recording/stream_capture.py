"""Capture shared Home Assistant HLS segments into an integration-owned MP4."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from .const import DEFAULT_WATCHDOG_PADDING_SECONDS
from .stream_mux import SegmentSnapshot, StreamMuxWriter

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)
MAX_LOOKBACK_SEGMENTS = 50
_PROVIDER = "hls"
_HEARTBEAT_SECONDS = 1.0
_MIN_DEADLINE_PADDING_SECONDS = 5.0


class CameraStreamUnavailableError(Exception):
    """Raised when Home Assistant cannot create a camera stream."""


class StreamCaptureError(Exception):
    """Raised when shared-stream capture cannot produce the requested recording."""


def _get_camera(hass: HomeAssistant, entity_id: str) -> Any:
    """Resolve an entity through Home Assistant's camera helper."""
    from homeassistant.components.camera.helper import get_camera_from_entity_id

    return get_camera_from_entity_id(hass, entity_id)


async def _await_executor_job[T](
    hass: HomeAssistant, function: Callable[..., T], *args: Any
) -> T:
    """Wait for an executor Future to finish, even across repeated cancellation."""
    future = hass.async_add_executor_job(function, *args)
    cancellation: asyncio.CancelledError | None = None
    while not future.done():
        try:
            await asyncio.shield(future)
        except asyncio.CancelledError as err:
            cancellation = err
        except Exception:
            break
    try:
        result = future.result()
    except BaseException:
        if cancellation is not None:
            raise cancellation
        raise
    if cancellation is not None:
        raise cancellation
    return result


async def _snapshot_segment(hass: HomeAssistant, segment: Any) -> SegmentSnapshot:
    """Copy immutable HLS segment bytes away from the event loop."""
    # Segment.parts is appended on HA's event loop. Take a stable tuple of immutable
    # part bytes before offloading the potentially large concatenation.
    part_bytes = tuple(part.data for part in segment.parts)
    data = await _await_executor_job(hass, b"".join, part_bytes)
    return SegmentSnapshot(
        sequence=segment.sequence,
        stream_id=segment.stream_id,
        start_time=segment.start_time,
        duration=float(segment.duration),
        init=segment.init,
        data=data,
    )


async def _create_writer(
    hass: HomeAssistant, target_path: Path | str, min_segment_duration: float, orientation: int
) -> StreamMuxWriter:
    """Construct the writer off-loop and close it if cancellation races construction."""
    created: list[StreamMuxWriter] = []

    def construct() -> StreamMuxWriter:
        writer = StreamMuxWriter(target_path, min_segment_duration, orientation)
        created.append(writer)
        return writer

    try:
        return await _await_executor_job(hass, construct)
    except asyncio.CancelledError:
        if created:
            await _await_executor_job(hass, created[0].close)
        raise


def _completed_lookback(segments: list[Any], seconds: float) -> list[Any]:
    completed = [segment for segment in segments if segment.complete]
    if seconds <= 0 or not completed:
        return []
    selected: list[Any] = []
    total = 0.0
    for segment in reversed(completed[-MAX_LOOKBACK_SEGMENTS:]):
        selected.append(segment)
        total += float(segment.duration)
        if total >= seconds:
            break
    return list(reversed(selected))


async def async_record_stream(
    hass: HomeAssistant,
    camera_entity: str,
    target_path: Path | str,
    duration_seconds: float,
    lookback_seconds: float = 0.0,
    watchdog_padding_seconds: float = DEFAULT_WATCHDOG_PADDING_SECONDS,
) -> dict[str, Any]:
    """Capture shared HLS segments without consuming or removing the provider."""
    camera = _get_camera(hass, camera_entity)
    stream = await camera.async_create_stream()
    if not stream:
        raise CameraStreamUnavailableError("Camera returned no stream")

    outputs = stream.outputs()
    hls = outputs.get(_PROVIDER)
    if hls is None:
        hls = stream.add_provider(_PROVIDER)
    await stream.start()

    settings = getattr(stream, "_stream_settings", None)
    min_segment_duration = float(getattr(settings, "min_segment_duration", 0.0))
    dynamic_settings = getattr(stream, "dynamic_stream_settings", None)
    orientation = int(getattr(dynamic_settings, "orientation", 1))
    writer = await _create_writer(hass, target_path, min_segment_duration, orientation)

    target_duration = float(getattr(hls, "target_duration", 6.0) or 6.0)
    heartbeat_interval = min(_HEARTBEAT_SECONDS, max(0.1, target_duration / 4))
    deadline_padding = min(
        max(0.1, watchdog_padding_seconds - 0.1),
        max(_MIN_DEADLINE_PADDING_SECONDS, target_duration * 2),
    )
    loop = asyncio.get_running_loop()
    deadline = loop.time() + duration_seconds + lookback_seconds + deadline_padding
    captured_live_duration = 0.0
    last_sequence: int | None = None

    def check_provider() -> None:
        if stream.outputs().get(_PROVIDER) is not hls or getattr(hls, "idle", False):
            raise StreamCaptureError("Shared HLS stream provider became idle or was removed")

    def wake_provider() -> None:
        idle_timer = getattr(hls, "idle_timer", None)
        awake = getattr(idle_timer, "awake", None)
        if awake is not None:
            awake()

    def mark_sequence(segment: Any) -> None:
        nonlocal last_sequence
        sequence = int(segment.sequence)
        if last_sequence is not None and sequence != last_sequence + 1:
            raise StreamCaptureError("Shared HLS stream segment sequence gap detected")
        last_sequence = sequence

    heartbeat: asyncio.Task[None] | None = None

    async def maintain_provider() -> None:
        while True:
            await asyncio.sleep(heartbeat_interval)
            check_provider()
            wake_provider()

    try:
        # Freeze the boundary before awaiting segment copies. Only completed buffered
        # segments are eligible for lookback; an incomplete current segment is live.
        boundary_segments = list(hls.get_segments())
        selected = _completed_lookback(boundary_segments, lookback_seconds)
        completed_at_boundary = [segment for segment in boundary_segments if segment.complete]
        if selected:
            first_sequence = int(selected[0].sequence)
            previous = [s for s in completed_at_boundary if int(s.sequence) < first_sequence]
            last_sequence = int(previous[-1].sequence) if previous else first_sequence - 1
        elif completed_at_boundary:
            last_sequence = int(completed_at_boundary[-1].sequence)
        else:
            incomplete = [s for s in boundary_segments if not s.complete]
            last_sequence = int(incomplete[0].sequence) - 1 if incomplete else None

        heartbeat = asyncio.create_task(maintain_provider())

        # Drain eligible historical segments first, in order.
        for segment in selected:
            mark_sequence(segment)
            await _await_executor_job(hass, writer.write_segment, await _snapshot_segment(hass, segment))

        while captured_live_duration < duration_seconds:
            check_provider()
            wake_provider()
            if loop.time() >= deadline:
                raise StreamCaptureError("Timed out before requested live media duration was captured")

            # Process any segment completed since the last receive before waiting. HA
            # signals a newly-created segment, by which point its predecessor is complete.
            segments = list(hls.get_segments())
            ready = [s for s in segments if s.complete and (last_sequence is None or int(s.sequence) > last_sequence)]
            for segment in ready:
                mark_sequence(segment)
                snapshot = await _snapshot_segment(hass, segment)
                await _await_executor_job(hass, writer.write_segment, snapshot)
                captured_live_duration += snapshot.duration

            if captured_live_duration >= duration_seconds:
                break
            check_provider()
            remaining = deadline - loop.time()
            try:
                await asyncio.wait_for(hls.recv(), timeout=min(heartbeat_interval, remaining))
            except TimeoutError:
                continue

        # A whole-segment capture can exceed the requested duration slightly.
        result = await _await_executor_job(hass, writer.finish)
        return result
    finally:
        try:
            if heartbeat is not None:
                heartbeat.cancel()
                await asyncio.gather(heartbeat, return_exceptions=True)
        finally:
            await _await_executor_job(hass, writer.close)
