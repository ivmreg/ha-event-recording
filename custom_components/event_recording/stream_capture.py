"""Capture shared Home Assistant HLS segments into an integration-owned MP4."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
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
_CLEANUP_MARGIN_SECONDS = 0.5
MIN_LIVE_CAPTURE_RATIO = 0.5


class CameraStreamUnavailableError(Exception):
    """Raised when Home Assistant cannot create a camera stream."""


STREAM_CAPTURE_FAILED = "STREAM_CAPTURE_FAILED"
STREAM_CAPTURE_TIMEOUT = "STREAM_CAPTURE_TIMEOUT"
STREAM_SEQUENCE_GAP = "STREAM_SEQUENCE_GAP"
STREAM_PROVIDER_LOST = "STREAM_PROVIDER_LOST"


class StreamCaptureError(Exception):
    """Raised when shared-stream capture cannot produce the requested recording."""

    def __init__(
        self,
        message: str = "Stream capture failed",
        code: str = STREAM_CAPTURE_FAILED,
    ) -> None:
        super().__init__(message)
        self.code = code


def _get_camera(hass: HomeAssistant, entity_id: str) -> Any:
    """Resolve an entity through Home Assistant's camera helper."""
    from homeassistant.components.camera.helper import get_camera_from_entity_id

    return get_camera_from_entity_id(hass, entity_id)


async def _await_executor_job[T](hass: HomeAssistant, function: Callable[..., T], *args: Any) -> T:
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


def _attach_observed_interval(
    result: dict[str, Any],
    *,
    observed_start: datetime | None,
    observed_end: datetime | None,
    segments: int,
    max_segment_duration: float,
    continuity: str,
) -> None:
    """Add host-measured interval evidence to a capture result."""
    if (
        observed_start is None
        or observed_end is None
        or observed_end <= observed_start
    ):
        result["observed_start_ts"] = None
        result["observed_end_ts"] = None
        result["observed_segments"] = segments
        result["continuity"] = (
            "broken" if continuity == "broken" else "unverified"
        )
        result["time_uncertainty_seconds"] = None
        return
    result["observed_start_ts"] = observed_start.timestamp()
    result["observed_end_ts"] = observed_end.timestamp()
    result["observed_segments"] = segments
    result["continuity"] = continuity
    # A segment boundary is observed on the host clock, but the exact frame
    # boundary is quantised by the HLS segmenter; keep that bounded and honest.
    result["time_uncertainty_seconds"] = round(
        min(0.5, max(0.1, max_segment_duration / 2.0)), 3
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
    watchdog_deadline: float | None = None,
    min_live_ratio: float = MIN_LIVE_CAPTURE_RATIO,
) -> dict[str, Any]:
    """Capture shared HLS segments without consuming or removing the provider."""
    loop = asyncio.get_running_loop()
    started_at = loop.time()
    total_budget = duration_seconds + lookback_seconds + watchdog_padding_seconds
    hard_deadline = (
        watchdog_deadline if watchdog_deadline is not None else (started_at + total_budget)
    )
    cleanup_margin = min(_CLEANUP_MARGIN_SECONDS, max(0.0, watchdog_padding_seconds) * 0.1)
    capture_deadline = hard_deadline - cleanup_margin
    min_live_ratio = max(0.0, min(1.0, min_live_ratio))
    min_live_duration = duration_seconds * min_live_ratio

    camera = _get_camera(hass, camera_entity)
    stream = await camera.async_create_stream()
    if not stream:
        raise CameraStreamUnavailableError("Camera returned no stream")

    outputs = stream.outputs()
    hls = outputs.get(_PROVIDER)
    if hls is None:
        hls = stream.add_provider(_PROVIDER)

    target_duration = float(getattr(hls, "target_duration", 6.0) or 6.0)
    heartbeat_interval = min(_HEARTBEAT_SECONDS, max(0.1, target_duration / 4))
    captured_live_duration = 0.0
    completed_live_segments = 0
    written_segments = 0
    last_sequence: int | None = None
    observed_start: datetime | None = None
    observed_end: datetime | None = None
    max_segment_duration = 0.0
    heartbeat_error: BaseException | None = None
    setup_done_at: float | None = None

    _LOGGER.debug(
        "Starting shared HLS stream capture for %s: duration=%.2fs, lookback=%.2fs, target_duration=%.2fs, watchdog_budget=%.2fs",
        camera_entity,
        duration_seconds,
        lookback_seconds,
        target_duration,
        hard_deadline - started_at,
    )

    def raise_capture_timeout() -> None:
        now = loop.time()
        elapsed_setup = (
            (setup_done_at - started_at) if setup_done_at is not None else (now - started_at)
        )
        _LOGGER.warning(
            "Shared stream capture timed out: elapsed_setup=%.2fs, elapsed_total=%.2fs, "
            "captured_live=%.2fs, requested_live=%.2fs, completed_segments=%d, "
            "target_duration=%.2fs, watchdog_budget=%.2fs",
            elapsed_setup,
            now - started_at,
            captured_live_duration,
            duration_seconds,
            completed_live_segments,
            target_duration,
            hard_deadline - started_at,
        )
        raise StreamCaptureError(
            "Timed out before requested live media duration was captured",
            code=STREAM_CAPTURE_TIMEOUT,
        )

    def wake_provider() -> None:
        idle_timer = getattr(hls, "idle_timer", None)
        awake = getattr(idle_timer, "awake", None)
        if callable(awake):
            awake()

    def check_provider() -> None:
        if heartbeat_error is not None:
            raise heartbeat_error
        if stream.outputs().get(_PROVIDER) is not hls or getattr(hls, "idle", False):
            raise StreamCaptureError(
                "Shared HLS stream provider became idle or was removed",
                code=STREAM_PROVIDER_LOST,
            )

    def mark_sequence(segment: Any) -> None:
        nonlocal last_sequence
        sequence = int(segment.sequence)
        if last_sequence is not None and sequence != last_sequence + 1:
            raise StreamCaptureError(
                "Shared HLS stream segment sequence gap detected",
                code=STREAM_SEQUENCE_GAP,
            )
        last_sequence = sequence

    async def maintain_provider() -> None:
        nonlocal heartbeat_error
        try:
            while True:
                await asyncio.sleep(heartbeat_interval)
                check_provider()
                wake_provider()
        except asyncio.CancelledError:
            raise
        except Exception as err:
            heartbeat_error = err
            raise

    # Wake provider immediately on acquisition before any startup/writer awaits
    wake_provider()

    writer: StreamMuxWriter | None = None
    heartbeat: asyncio.Task[None] | None = None
    cancellation: asyncio.CancelledError | None = None

    try:
        # Keep provider alive throughout startup, writer construction, and capture
        heartbeat = asyncio.create_task(maintain_provider())

        await stream.start()
        check_provider()

        settings = getattr(stream, "_stream_settings", None)
        min_segment_duration = float(getattr(settings, "min_segment_duration", 0.0))
        dynamic_settings = getattr(stream, "dynamic_stream_settings", None)
        orientation = int(getattr(dynamic_settings, "orientation", 1))
        writer = await _create_writer(hass, target_path, min_segment_duration, orientation)
        check_provider()

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

        _LOGGER.debug(
            "Lookback analysis for %s: boundary_count=%d, lookback_count=%d, last_sequence=%s",
            camera_entity,
            len(boundary_segments),
            len(selected),
            last_sequence,
        )

        # Drain eligible historical segments first, in order.
        for segment in selected:
            check_provider()
            wake_provider()
            mark_sequence(segment)
            await _await_executor_job(
                hass, writer.write_segment, await _snapshot_segment(hass, segment)
            )
            written_segments += 1
            segment_start = getattr(segment, "start_time", None)
            if isinstance(segment_start, datetime):
                if segment_start.tzinfo is None:
                    segment_start = segment_start.replace(tzinfo=UTC)
                else:
                    segment_start = segment_start.astimezone(UTC)
                segment_end = segment_start + timedelta(
                    seconds=float(segment.duration or 0.0)
                )
                if observed_start is None or segment_start < observed_start:
                    observed_start = segment_start
                if observed_end is None or segment_end > observed_end:
                    observed_end = segment_end
                max_segment_duration = max(
                    max_segment_duration, float(segment.duration or 0.0)
                )
            _LOGGER.debug(
                "Wrote lookback segment %s for %s: duration=%.2fs",
                segment.sequence,
                camera_entity,
                segment.duration,
            )

        setup_done_at = loop.time()

        while captured_live_duration < duration_seconds:
            check_provider()
            wake_provider()
            if loop.time() >= capture_deadline:
                if captured_live_duration >= min_live_duration:
                    break
                raise_capture_timeout()

            # Process any segment completed since the last receive before waiting. HA
            # signals a newly-created segment, by which point its predecessor is complete.
            segments = list(hls.get_segments())
            ready = [
                s
                for s in segments
                if s.complete and (last_sequence is None or int(s.sequence) > last_sequence)
            ]
            for segment in ready:
                check_provider()
                wake_provider()
                mark_sequence(segment)
                snapshot = await _snapshot_segment(hass, segment)
                await _await_executor_job(hass, writer.write_segment, snapshot)
                written_segments += 1
                captured_live_duration += snapshot.duration
                completed_live_segments += 1
                segment_start = getattr(snapshot, "start_time", None)
                if isinstance(segment_start, datetime):
                    if segment_start.tzinfo is None:
                        segment_start = segment_start.replace(tzinfo=UTC)
                    else:
                        segment_start = segment_start.astimezone(UTC)
                    segment_end = segment_start + timedelta(
                        seconds=float(snapshot.duration or 0.0)
                    )
                    if observed_start is None or segment_start < observed_start:
                        observed_start = segment_start
                    if observed_end is None or segment_end > observed_end:
                        observed_end = segment_end
                    max_segment_duration = max(
                        max_segment_duration, float(snapshot.duration or 0.0)
                    )
                _LOGGER.debug(
                    "Wrote live segment %s for %s: seg_dur=%.2fs, captured_live=%.2fs/%.2fs (total written=%d)",
                    segment.sequence,
                    camera_entity,
                    snapshot.duration,
                    captured_live_duration,
                    duration_seconds,
                    written_segments,
                )

            if captured_live_duration >= duration_seconds:
                break
            check_provider()
            remaining = capture_deadline - loop.time()
            if remaining <= 0:
                if captured_live_duration >= min_live_duration:
                    break
                raise_capture_timeout()
            _LOGGER.debug(
                "Waiting for next HLS segment for %s: remaining_budget=%.2fs",
                camera_entity,
                remaining,
            )
            try:
                await asyncio.wait_for(hls.recv(), timeout=min(heartbeat_interval, remaining))
            except TimeoutError:
                continue

        check_provider()
        # A whole-segment capture can exceed the requested duration slightly.
        raw_result = await _await_executor_job(hass, writer.finish)
        result = dict(raw_result) if isinstance(raw_result, dict) else {}
        if captured_live_duration < duration_seconds:
            shortfall = max(0.0, round(duration_seconds - captured_live_duration, 4))
            result["captured_live_duration"] = captured_live_duration
            result["requested_duration"] = duration_seconds
            result["duration_shortfall"] = shortfall
            _LOGGER.info(
                "Shared stream capture accepted at deadline with relaxed duration: "
                "captured_live=%.2fs, requested_live=%.2fs (shortfall=%.2fs), total_written=%d",
                captured_live_duration,
                duration_seconds,
                shortfall,
                written_segments,
            )
        else:
            _LOGGER.debug(
                "Stream capture finished cleanly for %s: total_written=%d, live_duration=%.2fs",
                camera_entity,
                written_segments,
                captured_live_duration,
            )
        _attach_observed_interval(
            result,
            observed_start=observed_start,
            observed_end=observed_end,
            segments=written_segments,
            max_segment_duration=max_segment_duration,
            continuity="verified_segments",
        )
        return result
    except StreamCaptureError as err:
        if writer is not None and written_segments > 0:
            try:
                res = await _await_executor_job(hass, writer.finish)
            except Exception:
                raise err from None
            result = dict(res) if isinstance(res, dict) else {}
            result["partial_error"] = getattr(err, "code", STREAM_CAPTURE_FAILED)
            _attach_observed_interval(
                result,
                observed_start=observed_start,
                observed_end=observed_end,
                segments=written_segments,
                max_segment_duration=max_segment_duration,
                continuity=(
                    "broken"
                    if getattr(err, "code", None) == STREAM_SEQUENCE_GAP
                    else "unverified"
                ),
            )
            _LOGGER.debug(
                "Stream capture partial recovery for %s: written=%d, error=%s",
                camera_entity,
                written_segments,
                result.get("partial_error"),
            )
            return result
        raise
    except asyncio.CancelledError as err:
        cancellation = err
        raise
    finally:
        try:
            if heartbeat is not None:
                heartbeat.cancel()
                while not heartbeat.done():
                    try:
                        await asyncio.shield(heartbeat)
                    except asyncio.CancelledError as err:
                        cur = asyncio.current_task()
                        cancelling = getattr(cur, "cancelling", None)
                        if cancelling is not None and cancelling() > 0:
                            cancellation = err
                        elif cancelling is None:
                            cancellation = err
                    except Exception:
                        break
                if not heartbeat.cancelled():
                    _ = heartbeat.exception()
        finally:
            try:
                if writer is not None:
                    try:
                        await _await_executor_job(hass, writer.close)
                    except asyncio.CancelledError as err:
                        if cancellation is None:
                            cancellation = err
            finally:
                _LOGGER.debug(
                    "Stream capture cleanup complete for %s (elapsed=%.2fs)",
                    camera_entity,
                    loop.time() - started_at,
                )
            if cancellation is not None:
                raise cancellation
