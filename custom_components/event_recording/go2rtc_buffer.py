"""Continuously maintained, video-only go2rtc HLS fragment buffers."""

from __future__ import annotations

import asyncio
import logging
import math
import re
import time
import urllib.parse
from collections import deque
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any

from .camera_profiles import validate_go2rtc_stream_alias
from .const import (
    CONF_GO2RTC_BASE_URL,
    CONF_GO2RTC_PASSWORD,
    CONF_GO2RTC_STREAM,
    CONF_GO2RTC_USERNAME,
    CONF_RECORDING_PROVIDER,
    MODE_EXTEND,
    MODE_NATIVE_FIXED,
    PROVIDER_GO2RTC,
)

_LOGGER = logging.getLogger(__name__)
_MAX_PRODUCER_BYTES = 32 * 1024 * 1024
_MAX_TOTAL_RING_BYTES = 64 * 1024 * 1024
_MAX_TOTAL_BUFFER_BYTES = 128 * 1024 * 1024
_MAX_TOTAL_LEASE_BYTES = 64 * 1024 * 1024
_MAX_TOTAL_PENDING_BYTES = 16 * 1024 * 1024
_MAX_FRAGMENT_BYTES = 8 * 1024 * 1024
_MAX_INIT_BYTES = 1024 * 1024
_MAX_PLAYLIST_BYTES = 64 * 1024
_MAX_SEGMENTS = 180
_MAX_AGE_SECONDS = 75.0
_POLL_INTERVAL = 0.25
_REQUEST_TIMEOUT = 4.0
_SAFE_SESSION_ID = re.compile(r"^[a-zA-Z0-9_-]{1,32}$")


class Go2rtcBufferError(Exception):
    """Safe error code for go2rtc buffer failures."""

    def __init__(self, code: str, *, partial: bool = False) -> None:
        self.code = code
        self.partial = partial
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class BufferedFragment:
    snapshot: Any
    duration: float
    first_pts: float
    last_end_pts: float
    keyframe_pts: tuple[float, ...]
    keyframe_indices: tuple[int, ...]
    packet_durations: tuple[float, ...]
    observed_at: float
    observed_wall: float
    generation: int
    hls_sequence: int

    @property
    def size_bytes(self) -> int:
        return len(self.snapshot.init) + len(self.snapshot.data)

    @property
    def sequence(self) -> int:
        return int(self.snapshot.sequence)


def _measure_fragment(
    init: bytes, data: bytes
) -> tuple[float, float, float, tuple[float, ...], tuple[int, ...], tuple[float, ...]]:
    """Measure video packet timing/keyframes from actual fMP4 packets."""
    try:
        import av
    except ImportError:
        raise Go2rtcBufferError("PYAV_UNAVAILABLE") from None
    try:
        container = av.open(BytesIO(init + data), mode="r", format="mp4")
        try:
            if len(container.streams.video) != 1 or len(container.streams.audio) != 0:
                raise Go2rtcBufferError("INVALID_VIDEO_FRAGMENT")
            stream = container.streams.video[0]
            time_base = float(stream.time_base)
            starts: list[float] = []
            ends: list[float] = []
            keyframes: list[float] = []
            keyframe_indices: list[int] = []
            packet_durations: list[float] = []
            for packet in container.demux(stream):
                if packet.pts is None or packet.dts is None or packet.size <= 0:
                    continue
                pts = float(packet.pts) * time_base
                duration = float(packet.duration or 0) * time_base
                starts.append(pts)
                ends.append(pts + max(0.0, duration))
                packet_durations.append(max(0.0, duration))
                if packet.is_keyframe:
                    keyframes.append(pts)
                    keyframe_indices.append(len(starts) - 1)
            if not starts:
                raise Go2rtcBufferError("INVALID_VIDEO_FRAGMENT")
            first = min(starts)
            end = max(ends)
            duration = end - first
            if not math.isfinite(duration) or duration <= 0:
                raise Go2rtcBufferError("INVALID_VIDEO_DURATION")
            return (
                first,
                end,
                duration,
                tuple(keyframes),
                tuple(keyframe_indices),
                tuple(packet_durations),
            )
        finally:
            container.close()
    except Go2rtcBufferError:
        raise
    except Exception:
        raise Go2rtcBufferError("INVALID_VIDEO_FRAGMENT") from None


def _playlist_fields(text: str) -> tuple[str | None, list[tuple[int, str]]]:
    """Parse only go2rtc's generated fMP4 map and segment references."""
    init_uri: str | None = None
    media_sequence = 0
    segments: list[tuple[int, str]] = []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith("#EXT-X-MEDIA-SEQUENCE:"):
            try:
                media_sequence = int(line.partition(":")[2])
            except ValueError:
                raise Go2rtcBufferError("INVALID_HLS_PLAYLIST") from None
        elif line.startswith("#EXT-X-MAP:"):
            match = re.search(r'URI="([^"]+)"', line)
            if not match:
                raise Go2rtcBufferError("INVALID_HLS_PLAYLIST")
            init_uri = match.group(1)
        elif line and not line.startswith("#"):
            segments.append((media_sequence + len(segments), line))
    if init_uri is None or not segments:
        raise Go2rtcBufferError("INVALID_HLS_PLAYLIST")
    return init_uri, segments


class Go2rtcHlsProducer:
    """One continuously polled go2rtc session and bounded fragment ring."""

    def __init__(
        self,
        *,
        base_url: str,
        alias: str,
        username: str | None,
        password: str | None,
        session: Any,
        memory_available: Any,
        pending_available: Any,
        transient_available: Any | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.alias = validate_go2rtc_stream_alias(alias)
        self.username = username.strip() if username and username.strip() else None
        self.password = password if password else None
        self.session = session
        self._memory_available = memory_available
        self._pending_available = pending_available
        self._transient_available = transient_available or self._unbounded_transient
        self.ring: deque[BufferedFragment] = deque()
        self.ring_bytes = 0
        self.generation = 0
        self.sequence = 0
        self.last_hls_sequence: int | None = None
        self.status = "starting"
        self.error_code: str | None = None
        self.reconnects = 0
        self.last_fragment_at: float | None = None
        self._condition = asyncio.Condition()
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()
        self._generation_gap = False

    @staticmethod
    async def _unbounded_transient(_size: int) -> bool:
        return True

    @property
    def buffered_seconds(self) -> float:
        return sum(fragment.duration for fragment in self.ring)

    @property
    def bytes_used(self) -> int:
        return self.ring_bytes

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self._run(), name="go2rtc_video_buffer")

    async def stop(self) -> None:
        self._stop.set()
        if self._task and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        self._task = None
        self.status = "stopped"

    async def wait_for_sequence(
        self, after_sequence: int, timeout: float, expected_generation: int | None = None
    ) -> BufferedFragment | None:
        expected_generation = self.generation if expected_generation is None else expected_generation

        async def _wait() -> BufferedFragment | None:
            async with self._condition:
                while not self._stop.is_set():
                    if self.generation != expected_generation:
                        raise Go2rtcBufferError("STREAM_SEQUENCE_GAP", partial=True)
                    if self.generation_gap:
                        raise Go2rtcBufferError("STREAM_SEQUENCE_GAP", partial=True)
                    if (
                        self.last_fragment_at is not None
                        and time.monotonic() - self.last_fragment_at > 10.0
                    ):
                        self.status = "degraded"
                        self.error_code = "STREAM_CAPTURE_TIMEOUT"
                    for fragment in self.ring:
                        # A queued event may be several seconds old by the time
                        # its worker starts. The ring age bound and per-event
                        # trigger cursor determine eligibility; freshness here
                        # is only for producer readiness, not event backlog.
                        if fragment.sequence > after_sequence:
                            return fragment
                    await self._condition.wait()
                return None

        try:
            return await asyncio.wait_for(_wait(), timeout=max(0.1, timeout))
        except TimeoutError:
            return None

    async def wait_ready(self, timeout: float) -> bool:
        async def _wait() -> bool:
            async with self._condition:
                while not self._stop.is_set():
                    if (
                        self.ring
                        and self.status == "ready"
                        and self.last_fragment_at is not None
                        and time.monotonic() - self.last_fragment_at <= 10.0
                    ):
                        return True
                    await self._condition.wait()
                return False

        try:
            return await asyncio.wait_for(_wait(), max(0.1, timeout))
        except TimeoutError:
            return False

    @property
    def generation_gap(self) -> bool:
        return self._generation_gap

    def history(self) -> tuple[BufferedFragment, ...]:
        now = time.monotonic()
        return tuple(f for f in self.ring if now - f.observed_at <= _MAX_AGE_SECONDS)

    def _headers(self) -> dict[str, str]:
        if not self.username:
            return {}
        import aiohttp

        encoder = getattr(aiohttp, "encode_basic_auth", None)
        auth = (
            encoder(self.username, self.password or "")
            if callable(encoder)
            else aiohttp.BasicAuth(self.username, self.password or "", encoding="utf-8").encode()
        )
        return {"Authorization": auth}

    def _allowed_url(
        self,
        relative: str,
        *,
        expected: tuple[str, ...],
        resource_base_url: str | None = None,
    ) -> str:
        base = urllib.parse.urlsplit(self.base_url)
        resolved = urllib.parse.urljoin(
            resource_base_url or f"{self.base_url}/api/stream.m3u8", relative
        )
        parsed = urllib.parse.urlsplit(resolved)
        root_path = base.path.rstrip("/")
        allowed_prefix = f"{root_path}/api/hls/"
        if (
            parsed.scheme != base.scheme
            or parsed.netloc.lower() != base.netloc.lower()
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or parsed.path not in tuple(f"{allowed_prefix}{name}" for name in expected)
        ):
            raise Go2rtcBufferError("INVALID_HLS_RESOURCE_URI")
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if set(query) - {"id", "n"} or len(query.get("id", [])) != 1:
            raise Go2rtcBufferError("INVALID_HLS_RESOURCE_URI")
        if not _SAFE_SESSION_ID.fullmatch(query["id"][0]):
            raise Go2rtcBufferError("INVALID_HLS_RESOURCE_URI")
        if "n" in query and (len(query["n"]) != 1 or not query["n"][0].isdigit()):
            raise Go2rtcBufferError("INVALID_HLS_RESOURCE_URI")
        return urllib.parse.urlunsplit(parsed)

    async def _get(
        self,
        url: str,
        *,
        params: dict[str, str] | None = None,
        max_bytes: int = _MAX_FRAGMENT_BYTES,
    ) -> tuple[int, bytes, int]:
        import aiohttp

        timeout = aiohttp.ClientTimeout(
            total=_REQUEST_TIMEOUT,
            connect=min(2.0, _REQUEST_TIMEOUT),
            sock_read=_REQUEST_TIMEOUT,
        )
        async with self.session.get(
            url,
            params=params,
            headers=self._headers(),
            allow_redirects=False,
            timeout=timeout,
        ) as response:
            if 300 <= response.status < 400:
                raise Go2rtcBufferError("REDIRECT_NOT_ALLOWED")
            if response.status != 200:
                return response.status, b"", 0
            chunks: list[bytes] = []
            total = 0
            transient_reserved = 0
            pending_reserved = 0
            try:
                async for chunk in response.content.iter_chunked(65536):
                    total += len(chunk)
                    if total > max_bytes:
                        raise Go2rtcBufferError("BUFFER_FRAGMENT_TOO_LARGE")
                    # The response chunk list and final joined body coexist
                    # briefly. Reserve both before retaining another chunk.
                    transient_size = len(chunk) * 2
                    if transient_size and not await self._transient_available(transient_size):
                        raise Go2rtcBufferError("BUFFER_MEMORY_LIMIT", partial=True)
                    transient_reserved += transient_size
                    chunks.append(chunk)
                if not chunks:
                    return response.status, b"", 0
                body = b"".join(chunks)
                if not await self._pending_available(len(body)):
                    raise Go2rtcBufferError("BUFFER_MEMORY_LIMIT", partial=True)
                pending_reserved = len(body)
                chunks.clear()
                if transient_reserved:
                    await self._transient_available(-transient_reserved)
                    transient_reserved = 0
                return response.status, body, pending_reserved
            except BaseException:
                if pending_reserved:
                    await self._pending_available(-pending_reserved)
                raise
            finally:
                if transient_reserved:
                    await self._transient_available(-transient_reserved)

    async def _clear_generation(self, error_code: str | None = None) -> None:
        old_bytes = self.ring_bytes
        async with self._condition:
            self.generation += 1
            self.sequence = 0
            self.last_hls_sequence = None
            self.ring.clear()
            self.ring_bytes = 0
            self._generation_gap = False
            self.error_code = error_code
            self.status = "reconnecting"
            self.reconnects += 1
            self._condition.notify_all()
        if old_bytes:
            await self._memory_available(-old_bytes, self)

    async def _append(self, hls_sequence: int, init: bytes, data: bytes) -> None:
        from .recorder import _settled_file_operation

        scratch_bytes = 2 * (len(init) + len(data))
        if not await self._transient_available(scratch_bytes):
            raise Go2rtcBufferError("BUFFER_MEMORY_LIMIT", partial=True)
        try:
            measured, cancellation = await _settled_file_operation(
                _measure_fragment, init, data
            )
            if cancellation is not None:
                raise cancellation
            first, end, duration, keyframes, keyframe_indices, packet_durations = measured
        finally:
            await self._transient_available(-scratch_bytes)
        if self.ring:
            previous = self.ring[-1]
            gap_tolerance = max(0.25, min(previous.duration, duration) * 0.25)
            if (
                first < previous.first_pts - 0.5
                or first > previous.last_end_pts + gap_tolerance
            ):
                raise Go2rtcBufferError("STREAM_SEQUENCE_GAP", partial=True)
        if not await self._memory_available(len(init) + len(data), self):
            async with self._condition:
                self._generation_gap = True
                self.status = "degraded"
                self.error_code = "BUFFER_MEMORY_LIMIT"
                self._condition.notify_all()
            return
        self.sequence += 1
        snapshot = _make_snapshot(
            sequence=self.sequence,
            generation=self.generation,
            start_time=first,
            duration=duration,
            init=init,
            data=data,
        )
        fragment = BufferedFragment(
            snapshot=snapshot,
            duration=duration,
            first_pts=first,
            last_end_pts=end,
            keyframe_pts=keyframes,
            keyframe_indices=keyframe_indices,
            packet_durations=packet_durations,
            observed_at=time.monotonic(),
            observed_wall=time.time(),
            generation=self.generation,
            hls_sequence=hls_sequence,
        )
        async with self._condition:
            before = self.ring_bytes
            self.ring.append(fragment)
            self.ring_bytes += fragment.size_bytes
            self.last_fragment_at = fragment.observed_at
            self.error_code = None
            self.status = "ready"
            self._trim_ring()
            trimmed = max(0, before + fragment.size_bytes - self.ring_bytes)
            self._condition.notify_all()
        if trimmed:
            await self._memory_available(-trimmed, self)

    def _trim_ring(self) -> None:
        now = time.monotonic()
        while self.ring and (
            self.ring_bytes > _MAX_PRODUCER_BYTES
            or len(self.ring) > _MAX_SEGMENTS
            or now - self.ring[0].observed_at > _MAX_AGE_SECONDS
        ):
            self.ring_bytes -= self.ring.popleft().size_bytes

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self._session_loop()
            except asyncio.CancelledError:
                raise
            except Go2rtcBufferError as err:
                await self._clear_generation(err.code)
                _LOGGER.debug("go2rtc video buffer session reset: %s", err.code)
            except Exception:
                await self._clear_generation("STREAM_CAPTURE_FAILED")
                _LOGGER.debug("go2rtc video buffer transport reset")
            if not self._stop.is_set():
                self.status = "reconnecting"
                await asyncio.sleep(min(2.0, _POLL_INTERVAL * 4))

    async def _session_loop(self) -> None:
        endpoint = f"{self.base_url}/api/stream.m3u8"
        status, master, master_reserved = await self._get(
            endpoint,
            params={"src": self.alias, "video": "h264,h265"},
            max_bytes=_MAX_PLAYLIST_BYTES,
        )
        if status != 200:
            raise Go2rtcBufferError(f"GO2RTC_HTTP_{status}")
        try:
            playlist_match = re.search(r"(?m)^(hls/playlist\.m3u8\?id=[a-zA-Z0-9_-]+)\s*$", master.decode("utf-8", "replace"))
        finally:
            if master_reserved:
                await self._pending_available(-master_reserved)
        if not playlist_match:
            raise Go2rtcBufferError("INVALID_HLS_PLAYLIST")
        playlist_url = self._allowed_url(playlist_match.group(1), expected=("playlist.m3u8",))
        self.generation += 1
        self.sequence = 0
        self.last_hls_sequence = None
        self.status = "warming"
        init_data: bytes | None = None
        init_reserved = 0
        pending: deque[tuple[int, bytes]] = deque()
        pending_bytes = 0
        init_task: asyncio.Task[tuple[bytes, int]] | None = None

        async def wait_for_init(init_url: str) -> tuple[bytes, int]:
            while not self._stop.is_set():
                init_status, body, reserved = await self._get(
                    init_url, max_bytes=_MAX_INIT_BYTES
                )
                if init_status == 200 and body:
                    return body, reserved
                if reserved:
                    await self._pending_available(-reserved)
                if init_status not in (200, 404):
                    raise Go2rtcBufferError(f"GO2RTC_HTTP_{init_status}")
                await asyncio.sleep(_POLL_INTERVAL)
            raise asyncio.CancelledError

        try:
            while not self._stop.is_set():
                status, raw_playlist, playlist_reserved = await self._get(
                    playlist_url, max_bytes=_MAX_PLAYLIST_BYTES
                )
                if status == 404:
                    raise Go2rtcBufferError("STREAM_PROVIDER_LOST")
                if status != 200:
                    raise Go2rtcBufferError(f"GO2RTC_HTTP_{status}")
                try:
                    init_ref, refs = _playlist_fields(raw_playlist.decode("utf-8", "replace"))
                    init_url = self._allowed_url(
                        init_ref,
                        expected=("init.mp4",),
                        resource_base_url=playlist_url,
                    )
                finally:
                    if playlist_reserved:
                        await self._pending_available(-playlist_reserved)
                if init_data is None and init_task is None:
                    init_task = asyncio.create_task(wait_for_init(init_url))
                next_seq = refs[0][0] if self.last_hls_sequence is None else self.last_hls_sequence + 1
                available = {seq: ref for seq, ref in refs}
                relative = available.get(next_seq)
                if relative is None:
                    if available and min(available) > next_seq:
                        raise Go2rtcBufferError("STREAM_SEQUENCE_GAP")
                    await asyncio.sleep(_POLL_INTERVAL)
                    continue
                segment_url = self._allowed_url(
                    relative,
                    expected=("segment.m4s",),
                    resource_base_url=playlist_url,
                )
                seg_status, data, data_reserved = await self._get(
                    segment_url, max_bytes=_MAX_FRAGMENT_BYTES
                )
                if seg_status == 404:
                    # Empty fragments refresh the session TTL without advancing n.
                    await asyncio.sleep(_POLL_INTERVAL)
                    continue
                if seg_status != 200:
                    raise Go2rtcBufferError(f"GO2RTC_HTTP_{seg_status}")
                if data:
                    pending.append((next_seq, data))
                    pending_bytes += len(data)
                    if pending_bytes > _MAX_FRAGMENT_BYTES:
                        raise Go2rtcBufferError("BUFFER_FRAGMENT_TOO_LARGE")
                    self.last_hls_sequence = next_seq
                elif data_reserved:
                    await self._pending_available(-data_reserved)
                if init_data is None and init_task is not None and init_task.done():
                    init_data, init_reserved = init_task.result()
                    while pending:
                        pending_seq, pending_data = pending.popleft()
                        try:
                            await self._append(pending_seq, init_data, pending_data)
                        finally:
                            pending_bytes -= len(pending_data)
                            await self._pending_available(-len(pending_data))
                elif init_data is not None and pending:
                    pending_seq, pending_data = pending.popleft()
                    try:
                        await self._append(pending_seq, init_data, pending_data)
                    finally:
                        pending_bytes -= len(pending_data)
                        await self._pending_available(-len(pending_data))
                await asyncio.sleep(_POLL_INTERVAL)
        finally:
            if pending_bytes:
                await self._pending_available(-pending_bytes)
            if init_reserved:
                await self._pending_available(-init_reserved)
            if init_task is not None:
                from .recorder import _settled_async_operation

                if not init_task.done():
                    init_task.cancel()
                async def _collect_init_task() -> list[Any]:
                    return await asyncio.gather(init_task, return_exceptions=True)

                outcomes, cancellation = await _settled_async_operation(
                    _collect_init_task()
                )
                if init_data is None and outcomes:
                    outcome = outcomes[0]
                    if isinstance(outcome, tuple) and len(outcome) == 2:
                        _unused_body, unclaimed_reserved = outcome
                        if unclaimed_reserved:
                            await self._pending_available(-unclaimed_reserved)
                if cancellation is not None:
                    raise cancellation


def _make_snapshot(**kwargs: Any) -> Any:
    from .stream_mux import SegmentSnapshot

    return SegmentSnapshot(
        sequence=kwargs["sequence"],
        stream_id=kwargs["generation"],
        start_time=kwargs["start_time"],
        duration=kwargs["duration"],
        init=kwargs["init"],
        data=kwargs["data"],
    )


class Go2rtcBufferManager:
    """Own, deduplicate, bound, and expose persistent go2rtc video buffers."""

    def __init__(self, hass: Any, entry: Any) -> None:
        self.hass = hass
        self.entry = entry
        self._producers: dict[tuple[str, str], Go2rtcHlsProducer] = {}
        self._camera_keys: dict[str, tuple[str, str]] = {}
        self._cursors: dict[tuple[str, tuple[str, str]], tuple[int, int, int]] = {}
        self._total_ring_bytes = 0
        self._total_lease_bytes = 0
        self._total_pending_bytes = 0
        self._total_transient_bytes = 0
        self._lease_lock = asyncio.Lock()
        self._session: Any | None = None
        self._connection_signature: tuple[str | None, str | None, str | None] | None = None
        self._last_exports: dict[tuple[str, str], dict[str, Any]] = {}

    def _go2rtc_settings(self) -> tuple[str | None, str | None, str | None]:
        from .camera_profiles import validate_go2rtc_base_url

        options = getattr(self.entry, "options", {}) or {}
        try:
            base_url = validate_go2rtc_base_url(options.get(CONF_GO2RTC_BASE_URL))
        except ValueError:
            return None, None, None
        username = options.get(CONF_GO2RTC_USERNAME)
        password = options.get(CONF_GO2RTC_PASSWORD)
        if bool(username and str(username).strip()) != bool(password):
            return None, None, None
        return base_url, username, password

    async def _get_session(self) -> Any:
        if self._session is None:
            from homeassistant.helpers.aiohttp_client import async_get_clientsession

            self._session = async_get_clientsession(self.hass)
        return self._session

    async def _memory_available(self, size: int, producer: Go2rtcHlsProducer) -> bool:
        async with self._lease_lock:
            if size < 0:
                self._total_ring_bytes = max(0, self._total_ring_bytes + size)
                return True
            self._trim_global()
            ring_projected = self._total_ring_bytes + size
            if ring_projected > _MAX_TOTAL_RING_BYTES:
                # Reclaim oldest unleased ring data across producers.
                candidates = sorted(
                    (
                        (fragment.observed_at, owner, fragment)
                        for owner in self._producers.values()
                        for fragment in owner.ring
                    ),
                    key=lambda item: item[0],
                )
                for _when, owner, fragment in candidates:
                    try:
                        owner.ring.remove(fragment)
                    except ValueError:
                        continue
                    owner.ring_bytes -= fragment.size_bytes
                    self._total_ring_bytes -= fragment.size_bytes
                    ring_projected -= fragment.size_bytes
                    if ring_projected <= _MAX_TOTAL_RING_BYTES:
                        break
            aggregate_projected = (
                ring_projected
                + self._total_lease_bytes
                + self._total_pending_bytes
                + self._total_transient_bytes
            )
            if (
                ring_projected > _MAX_TOTAL_RING_BYTES
                or aggregate_projected > _MAX_TOTAL_BUFFER_BYTES
            ):
                return False
            self._total_ring_bytes += size
            return True

    async def _reserve_lease(self, size: int) -> bool:
        async with self._lease_lock:
            if (
                self._total_lease_bytes + size > _MAX_TOTAL_LEASE_BYTES
                or self._total_ring_bytes + self._total_lease_bytes + self._total_pending_bytes
                + self._total_transient_bytes + size
                > _MAX_TOTAL_BUFFER_BYTES
            ):
                return False
            self._total_lease_bytes += size
            return True

    async def _pending_available(self, size: int) -> bool:
        async with self._lease_lock:
            if size < 0:
                self._total_pending_bytes = max(0, self._total_pending_bytes + size)
                return True
            if (
                self._total_pending_bytes + size > _MAX_TOTAL_PENDING_BYTES
                or self._total_ring_bytes + self._total_lease_bytes + self._total_pending_bytes
                + self._total_transient_bytes + size
                > _MAX_TOTAL_BUFFER_BYTES
            ):
                return False
            self._total_pending_bytes += size
            return True

    async def _transient_available(self, size: int) -> bool:
        async with self._lease_lock:
            if size < 0:
                self._total_transient_bytes = max(0, self._total_transient_bytes + size)
                return True
            if (
                self._total_ring_bytes
                + self._total_lease_bytes
                + self._total_pending_bytes
                + self._total_transient_bytes
                + size
                > _MAX_TOTAL_BUFFER_BYTES
            ):
                return False
            self._total_transient_bytes += size
            return True

    async def _release_lease(self, size: int) -> None:
        async with self._lease_lock:
            self._total_lease_bytes = max(0, self._total_lease_bytes - size)

    def _trim_global(self) -> None:
        for producer in self._producers.values():
            before = producer.ring_bytes
            producer._trim_ring()
            self._total_ring_bytes -= max(0, before - producer.ring_bytes)

    async def reconcile(self) -> None:
        """Start producers for current profiles and stop removed/changed ones."""
        from .camera_profiles import get_camera_profiles, validate_go2rtc_base_url

        base_url, username, password = self._go2rtc_settings()
        profiles = get_camera_profiles(self.entry)
        desired: dict[str, tuple[str, str]] = {}
        if base_url:
            for camera, profile in profiles.items():
                if (
                    profile.get(CONF_RECORDING_PROVIDER) != PROVIDER_GO2RTC
                    or profile.get("recording_mode", MODE_EXTEND)
                    not in (MODE_EXTEND, MODE_NATIVE_FIXED)
                ):
                    continue
                try:
                    alias = validate_go2rtc_stream_alias(profile.get(CONF_GO2RTC_STREAM))
                    validated_base = validate_go2rtc_base_url(base_url)
                except ValueError:
                    continue
                desired[camera] = (validated_base.rstrip("/"), alias)
        self._camera_keys = desired
        wanted = set(desired.values())
        signature = (base_url, username, password)
        if self._connection_signature is not None and signature != self._connection_signature:
            for producer in self._producers.values():
                await producer.stop()
                self._total_ring_bytes -= producer.ring_bytes
            self._producers.clear()
        for key in list(self._producers):
            if key not in wanted or key[0] != (base_url or "").rstrip("/"):
                producer = self._producers.pop(key)
                await producer.stop()
                self._total_ring_bytes -= producer.ring_bytes
        self._connection_signature = signature
        if not wanted:
            return
        session = await self._get_session()
        for key in wanted:
            if key not in self._producers:
                producer = Go2rtcHlsProducer(
                    base_url=key[0],
                    alias=key[1],
                    username=username,
                    password=password,
                    session=session,
                    memory_available=self._memory_available,
                    pending_available=self._pending_available,
                    transient_available=self._transient_available,
                )
                self._producers[key] = producer
                producer.start()

    async def stop(self) -> None:
        producers = list(self._producers.values())
        self._producers.clear()
        self._camera_keys.clear()
        self._cursors.clear()
        await asyncio.gather(*(producer.stop() for producer in producers), return_exceptions=True)
        self._total_ring_bytes = 0
        self._total_lease_bytes = 0
        self._total_pending_bytes = 0
        self._total_transient_bytes = 0

    def producer_for(self, camera_entity: str, alias: str | None = None) -> Go2rtcHlsProducer:
        key = self._camera_keys.get(camera_entity)
        if key is None and alias:
            base, _username, _password = self._go2rtc_settings()
            candidate = (str(base).rstrip("/"), alias) if base else None
            if candidate in self._producers:
                key = candidate
        if key is None or (alias is not None and key[1] != alias):
            raise Go2rtcBufferError("PREROLL_UNAVAILABLE")
        producer = self._producers.get(key)
        if producer is None:
            raise Go2rtcBufferError("PREROLL_UNAVAILABLE")
        return producer

    def diagnostics(self) -> dict[str, Any]:
        return {
            "configured_buffers": len(self._producers),
            "ready_buffers": sum(p.status == "ready" for p in self._producers.values()),
            "buffers": [
                {
                    "state": p.status,
                    "buffered_seconds": round(p.buffered_seconds, 2),
                    "buffer_age_seconds": round(max(0.0, time.monotonic() - p.last_fragment_at), 2)
                    if p.last_fragment_at is not None
                    else None,
                    "bytes": p.bytes_used,
                    "reconnects": p.reconnects,
                    "error_code": p.error_code,
                    "video_only": True,
                    "last_export": self._last_exports.get((p.base_url, p.alias)),
                }
                for p in self._producers.values()
            ],
            "total_ring_bytes": self._total_ring_bytes,
            "total_lease_bytes": self._total_lease_bytes,
        }

    def note_export(self, alias: str, details: dict[str, Any]) -> None:
        """Keep one safe, anonymized latest export summary for diagnostics."""
        base, _username, _password = self._go2rtc_settings()
        if base:
            self._last_exports[(base.rstrip("/"), alias)] = {
                key: value
                for key, value in details.items()
                if key
                in {
                    "video_track_count",
                    "audio_track_count",
                    "written_packet_count",
                    "requested_preroll_seconds",
                    "obtained_preroll_seconds",
                    "buffered_before_trigger",
                    "continuity",
                    "partial_state",
                    "probed_duration_seconds",
                }
            }

    def release_event(self, event_id: str) -> None:
        for key in [key for key in self._cursors if key[0] == event_id]:
            self._cursors.pop(key, None)

    async def capture_chunk(
        self,
        *,
        camera_entity: str,
        alias: str,
        target_path: Path,
        duration_seconds: float,
        lookback_seconds: float,
        event_id: str,
        include_preroll: bool,
        event_start_timestamp: float,
        max_bytes: int,
        timeout_seconds: float,
    ) -> dict[str, Any]:
        """Write ring history and future live fragments to one MP4 staging file."""
        from .recorder import _settled_file_operation
        from .stream_mux import StreamMuxWriter

        started = time.monotonic()
        producer = self.producer_for(camera_entity, alias)
        if producer.status != "ready":
            # A cold producer has no history, but the event can still produce
            # useful post-trigger media. Give the already-running collector a
            # short opportunity to warm; if it cannot, continue from its live
            # sequence and report the missing pre-roll as partial.
            if not await producer.wait_ready(min(timeout_seconds, 3.0)):
                cold_start = True
            else:
                cold_start = False
        else:
            cold_start = False
        key = (event_id, (producer.base_url, producer.alias))
        async with self._lease_lock:
            cursor_state = self._cursors.get(key)
            generation = producer.generation
            all_history = producer.history()
            history = all_history
            if (
                producer.last_fragment_at is not None
                and time.monotonic() - producer.last_fragment_at > 10.0
            ):
                history = ()
            if include_preroll:
                history = tuple(f for f in history if f.observed_wall <= event_start_timestamp)
            cursor = 0
            cursor_packet = 0
            partial_error: str | None = "PREROLL_UNAVAILABLE" if cold_start else None
            selected: list[BufferedFragment] = []
            selected_start_index = 0
            obtained_preroll = 0.0
            if include_preroll and lookback_seconds > 0:
                selected, obtained_preroll, selected_start_index = _select_preroll_cursor(
                    history, lookback_seconds
                )
                if obtained_preroll + 0.01 < lookback_seconds:
                    partial_error = "PREROLL_UNAVAILABLE"
                if history:
                    tail_gap = max(
                        0.0, event_start_timestamp - history[-1].observed_wall
                    )
                    freshness_tolerance = max(1.0, 2.0 * history[-1].duration)
                else:
                    tail_gap = 0.0
                    freshness_tolerance = 0.0
                if history and tail_gap > freshness_tolerance:
                    # Source PTS has no UTC anchor. Once the observed stream
                    # tail is stale relative to the trigger, report no
                    # continuous preroll coverage even if older media exists.
                    obtained_preroll = 0.0
                    partial_error = "PREROLL_UNAVAILABLE"
                    posttrigger = tuple(
                        f for f in all_history if f.observed_wall > event_start_timestamp
                    )
                    has_contiguous_posttrigger = bool(
                        posttrigger
                        and posttrigger[0].generation == history[-1].generation
                        and posttrigger[0].sequence == history[-1].sequence + 1
                    )
                    if not has_contiguous_posttrigger:
                        partial_error = "STREAM_SEQUENCE_GAP"
            if cursor_state is not None:
                previous_generation, cursor, cursor_packet = cursor_state
                if previous_generation != generation:
                    partial_error = "STREAM_SEQUENCE_GAP"
                    cursor = producer.sequence + 1
                    cursor_packet = 0
            if selected:
                generation = selected[0].generation
                cursor = selected[-1].sequence + 1
                cursor_packet = 0
            elif cursor_state is None:
                if include_preroll:
                    posttrigger = tuple(
                        f for f in all_history if f.observed_wall > event_start_timestamp
                    )
                    position = _first_keyframe_position(posttrigger)
                    if position is not None:
                        cursor, cursor_packet = position
                    elif posttrigger:
                        cursor = posttrigger[0].sequence
                        partial_error = "PREROLL_UNAVAILABLE"
                    else:
                        cursor = producer.sequence + 1
                else:
                    cursor = producer.sequence + 1
            self._cursors[key] = (generation, cursor, cursor_packet)

            leased = sum(fragment.size_bytes for fragment in selected)
            if selected and (
                self._total_lease_bytes + leased > _MAX_TOTAL_LEASE_BYTES
                or self._total_ring_bytes + self._total_lease_bytes + self._total_pending_bytes
                + self._total_transient_bytes + leased
                > _MAX_TOTAL_BUFFER_BYTES
            ):
                selected = []
                partial_error = "PREROLL_UNAVAILABLE"
                leased = 0
            elif selected:
                self._total_lease_bytes += leased

        target_path.parent.mkdir(parents=True, exist_ok=True)
        writer = StreamMuxWriter(
            target_path,
            video_only=True,
            keyframe_start=True,
            max_bytes=max_bytes,
        )
        cancellation: asyncio.CancelledError | None = None
        live_duration = 0.0
        written_bytes = 0
        handoff_deadline: float | None = None
        observed_start_wall: float | None = None
        observed_end_wall: float | None = None
        max_observed_fragment = 0.0
        written_fragments = 0

        def note_written_fragment(fragment: BufferedFragment) -> None:
            nonlocal observed_start_wall, observed_end_wall
            nonlocal max_observed_fragment, written_fragments
            fragment_duration = max(0.0, float(fragment.duration))
            fragment_start = float(fragment.observed_wall) - fragment_duration
            if observed_start_wall is None or fragment_start < observed_start_wall:
                observed_start_wall = fragment_start
            if observed_end_wall is None or fragment.observed_wall > observed_end_wall:
                observed_end_wall = float(fragment.observed_wall)
            max_observed_fragment = max(max_observed_fragment, fragment_duration)
            written_fragments += 1

        async def write_fragment(
            fragment: BufferedFragment, *, start: int, end: int | None = None
        ) -> tuple[Any, asyncio.CancelledError | None]:
            # StreamMuxWriter concatenates init+media before demuxing; keep
            # that temporary copy inside the same aggregate memory ceiling.
            if not await self._transient_available(fragment.size_bytes):
                raise Go2rtcBufferError("BUFFER_MEMORY_LIMIT", partial=True)
            try:
                return await _settled_file_operation(
                    writer.write_segment,
                    fragment.snapshot,
                    packet_start_index=start,
                    packet_end_index=end,
                )
            finally:
                await self._transient_available(-fragment.size_bytes)

        try:
            for index, fragment in enumerate(selected):
                start_index = selected_start_index if index == 0 else 0
                _result, cancelled = await write_fragment(fragment, start=start_index)
                if cancelled:
                    cancellation = cancelled
                    raise cancelled
                written_bytes += fragment.size_bytes
                note_written_fragment(fragment)
            deadline = started + max(1.0, timeout_seconds)
            while time.monotonic() < deadline:
                now = time.monotonic()
                effective_deadline = min(deadline, handoff_deadline) if handoff_deadline else deadline
                if now >= effective_deadline:
                    if handoff_deadline:
                        partial_error = partial_error or "STREAM_SEQUENCE_GAP"
                    break
                try:
                    fragment = await producer.wait_for_sequence(
                        cursor - 1,
                        min(2.0, effective_deadline - now),
                        expected_generation=generation,
                    )
                except Go2rtcBufferError as err:
                    partial_error = err.code
                    break
                if fragment is None:
                    continue
                if fragment.generation != generation or fragment.sequence != cursor:
                    partial_error = "STREAM_SEQUENCE_GAP"
                    break
                async with self._lease_lock:
                    if fragment not in producer.ring:
                        partial_error = "STREAM_SEQUENCE_GAP"
                        break
                    if (
                        self._total_lease_bytes + fragment.size_bytes > _MAX_TOTAL_LEASE_BYTES
                        or self._total_ring_bytes + self._total_lease_bytes + self._total_pending_bytes
                        + self._total_transient_bytes + fragment.size_bytes > _MAX_TOTAL_BUFFER_BYTES
                    ):
                        partial_error = "BUFFER_MEMORY_LIMIT"
                        break
                    self._total_lease_bytes += fragment.size_bytes
                start_index = cursor_packet
                next_boundary: int | None = None
                if live_duration >= duration_seconds:
                    next_boundary = next(
                        (idx for idx in fragment.keyframe_indices if idx >= start_index), None
                    )
                else:
                    remaining = duration_seconds - live_duration
                    elapsed = 0.0
                    prior_keyframe_index = start_index
                    for keyframe_index in fragment.keyframe_indices:
                        if keyframe_index < start_index:
                            continue
                        elapsed += sum(
                            fragment.packet_durations[prior_keyframe_index:keyframe_index]
                        )
                        prior_keyframe_index = keyframe_index
                        if elapsed + 1e-9 >= remaining:
                            next_boundary = keyframe_index
                            break

                if next_boundary is not None:
                    if next_boundary > start_index:
                        try:
                            _result, cancelled = await write_fragment(
                                fragment, start=start_index, end=next_boundary
                            )
                        finally:
                            await self._release_lease(fragment.size_bytes)
                        if cancelled:
                            cancellation = cancelled
                            raise cancelled
                        written_bytes += fragment.size_bytes
                        note_written_fragment(fragment)
                        live_duration += sum(
                            fragment.packet_durations[start_index:next_boundary]
                        )
                    else:
                        await self._release_lease(fragment.size_bytes)
                    cursor = fragment.sequence
                    cursor_packet = next_boundary
                    self._cursors[key] = (generation, cursor, cursor_packet)
                    break

                try:
                    _result, cancelled = await write_fragment(fragment, start=start_index)
                finally:
                    await self._release_lease(fragment.size_bytes)
                if cancelled:
                    cancellation = cancelled
                    raise cancelled
                written_bytes += fragment.size_bytes
                note_written_fragment(fragment)
                live_duration += sum(fragment.packet_durations[start_index:])
                cursor = fragment.sequence + 1
                cursor_packet = 0
                self._cursors[key] = (generation, cursor, cursor_packet)
                if live_duration >= duration_seconds and handoff_deadline is None:
                    handoff_deadline = min(deadline, time.monotonic() + 5.0)
            if live_duration + 0.25 < duration_seconds:
                partial_error = partial_error or "STREAM_CAPTURE_TIMEOUT"
            if written_bytes == 0:
                raise Go2rtcBufferError(partial_error or "PREROLL_UNAVAILABLE")
            finish_result, cancelled = await _settled_file_operation(writer.finish)
            if cancelled:
                cancellation = cancelled
                raise cancelled
            result = {
                "downloaded_bytes": written_bytes,
                "requested_duration": duration_seconds + (lookback_seconds if selected else 0),
                "buffered_preroll_seconds": obtained_preroll,
                "requested_preroll_seconds": lookback_seconds if include_preroll else 0.0,
                "obtained_preroll_seconds": obtained_preroll,
                "buffered_before_trigger": bool(selected)
                and all(f.observed_wall <= event_start_timestamp for f in selected),
                "video_track_count": int(finish_result.get("video_track_count", 0)),
                "audio_track_count": int(finish_result.get("audio_track_count", 0)),
                "written_packet_count": int(finish_result.get("packets_written", 0)),
                "continuity": (
                    "continuous"
                    if partial_error is None
                    else (
                        "broken"
                        if partial_error == "STREAM_SEQUENCE_GAP"
                        else "unverified"
                    )
                ),
                "partial_state": partial_error,
                "generation": generation,
                "partial_error": partial_error,
                "observed_start_ts": observed_start_wall,
                "observed_end_ts": observed_end_wall,
                "observed_segments": written_fragments,
                "time_quality": "estimated" if observed_start_wall is not None else "unknown",
                "clock_source": "go2rtc",
                "time_uncertainty_seconds": (
                    round(min(5.0, max(0.1, max_observed_fragment)), 3)
                    if observed_start_wall is not None and observed_end_wall is not None
                    else None
                ),
            }
            self.note_export(
                alias,
                {
                    **result,
                    "obtained_preroll_seconds": result["obtained_preroll_seconds"],
                },
            )
            return result
        except BaseException:
            _result, cancelled = await _settled_file_operation(writer.close)
            cancellation = cancellation or cancelled
            if cancellation is not None:
                raise cancellation
            raise
        finally:
            if leased:
                await self._release_lease(leased)


def _select_preroll(
    history: tuple[BufferedFragment, ...], requested_seconds: float
) -> tuple[list[BufferedFragment], float]:
    selected, covered, _packet_index = _select_preroll_cursor(history, requested_seconds)
    return selected, covered


def _select_preroll_cursor(
    history: tuple[BufferedFragment, ...], requested_seconds: float
) -> tuple[list[BufferedFragment], float, int]:
    """Choose a keyframe-bearing fragment at/before the requested history boundary."""
    if not history or requested_seconds <= 0:
        return [], 0.0, 0
    latest_end = history[-1].last_end_pts
    requested_boundary = latest_end - requested_seconds
    candidates = [
        (index, keyframe, packet_index)
        for index, fragment in enumerate(history)
        for keyframe, packet_index in zip(fragment.keyframe_pts, fragment.keyframe_indices)
        if keyframe <= requested_boundary
    ]
    if candidates:
        keyframe_index, keyframe_pts, packet_index = candidates[-1]
    else:
        first_candidate = next(
            (
                (index, keyframe, packet_index)
                for index, fragment in enumerate(history)
                for keyframe, packet_index in zip(
                    fragment.keyframe_pts, fragment.keyframe_indices
                )
            ),
            None,
        )
        if first_candidate is None:
            return [], 0.0, 0
        keyframe_index, keyframe_pts, packet_index = first_candidate
    selected = list(history[keyframe_index:])
    covered = max(0.0, latest_end - keyframe_pts)
    return selected, covered, packet_index


def _first_keyframe_position(
    fragments: tuple[BufferedFragment, ...],
) -> tuple[int, int] | None:
    """Return the first keyframe cursor in the provided fragment order."""
    for fragment in fragments:
        if fragment.keyframe_indices:
            return fragment.sequence, fragment.keyframe_indices[0]
    return None
