"""Integration-owned MP4 stream remuxer with monotonic DTS enforcement.

Ensures output DTS is strictly monotonically increasing to satisfy MP4/ISOBMFF
container invariants and FFmpeg libavformat muxer requirements, while preserving
video PTS/DTS relationships (B-frame ordering), audio/video alignment across
discontinuities, and frame integrity without dropping meaningful media packets.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from fractions import Fraction
from io import BytesIO
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger(__name__)

RECORDER_CONTAINER_FORMAT = "mp4"
SEGMENT_CONTAINER_FORMAT = "mp4"


class StreamMuxError(Exception):
    """Base exception for stream remuxing failures."""


class MissingTimestampError(StreamMuxError):
    """Raised when an incoming non-empty packet lacks required DTS or PTS."""


class IncompatibleStreamError(StreamMuxError):
    """Raised when stream codecs or configurations change incompatibly across segments."""


class EmptyRecordingError(StreamMuxError):
    """Raised when no valid media packets were written to the recording."""


@dataclass(slots=True, frozen=True)
class SegmentSnapshot:
    """Immutable snapshot of a completed stream segment."""

    sequence: int
    stream_id: int
    start_time: Any
    duration: float
    init: bytes
    data: bytes


class MonotonicDtsNormalizer:
    """Stateful timestamp normalizer enforcing strictly increasing DTS per stream.

    Maintains presentation time offsets (PTS - DTS) for B-frames, carries forward
    minimal forward adjustments across duplicate/backwards timestamps so later packets
    do not reverse, and respects rational time base rescaling and epoch shifts.
    """

    def __init__(self) -> None:
        self._last_dts: dict[str, int] = {}
        self._cumulative_bump: dict[str, int] = {}

    def get_last_dts(self, stream_type: str) -> int | None:
        """Return the last output DTS recorded for the stream type."""
        return self._last_dts.get(stream_type)

    def normalize(
        self,
        packet_pts: int | None,
        packet_dts: int | None,
        stream_type: str,
        in_time_base: Fraction,
        out_time_base: Fraction,
        epoch_shift: Fraction = Fraction(0),
    ) -> tuple[int, int]:
        """Normalize PTS and DTS to ensure strictly increasing output DTS.

        Returns (normalized_pts, normalized_dts) in the output time base.
        Fails explicitly if packet_dts or packet_pts is None on non-empty packets.
        """
        if packet_dts is None or packet_pts is None:
            raise MissingTimestampError(
                f"Packet in stream '{stream_type}' has absent timestamp "
                f"(pts={packet_pts}, dts={packet_dts}); cannot safely mux"
            )

        shift_seconds = Fraction(epoch_shift)

        # Rescale into output time base once with rational epoch shift
        if in_time_base == out_time_base and shift_seconds == 0:
            rescaled_dts = packet_dts
            rescaled_pts = packet_pts
        else:
            rescaled_dts = round((Fraction(packet_dts, 1) * in_time_base + shift_seconds) / out_time_base)
            rescaled_pts = round((Fraction(packet_pts, 1) * in_time_base + shift_seconds) / out_time_base)

        # Preserve original composition time offset (PTS - DTS) in output timebase
        composition_offset = rescaled_pts - rescaled_dts

        # Determine monotonic DTS
        prev_dts = self._last_dts.get(stream_type)
        current_bump = self._cumulative_bump.get(stream_type, 0)
        candidate_dts = rescaled_dts + current_bump

        if prev_dts is None:
            # First packet in stream: establish baseline
            out_dts = candidate_dts
        elif candidate_dts <= prev_dts:
            # Duplicate or backwards DTS: bump to strictly monotonically increasing tick
            needed_bump = (prev_dts + 1) - candidate_dts
            current_bump += needed_bump
            out_dts = prev_dts + 1
            self._cumulative_bump[stream_type] = current_bump
        else:
            if rescaled_dts > prev_dts:
                self._cumulative_bump[stream_type] = 0
                out_dts = rescaled_dts
            else:
                out_dts = candidate_dts

        # Shift PTS equally to strictly preserve B-frame composition offset
        out_pts = out_dts + composition_offset

        self._last_dts[stream_type] = out_dts
        return out_pts, out_dts


class StreamMuxWriter:
    """Integration-owned PyAV container writer enforcing monotonic timestamps."""

    def __init__(
        self,
        target_path: Path | str,
        min_segment_duration: float = 0.0,
        orientation: int = 1,
    ) -> None:
        self.target_path = Path(target_path).resolve()
        self.tmp_path = Path(str(self.target_path) + ".tmp")
        self.min_segment_duration = min_segment_duration
        self.orientation = orientation
        self.normalizer = MonotonicDtsNormalizer()

        self._output_container: Any = None
        self._output_v: Any = None
        self._output_a: Any = None

        self._video_codec: str | None = None
        self._video_width: int | None = None
        self._video_height: int | None = None
        self._video_time_base: Fraction | None = None

        self._audio_codec: str | None = None
        self._audio_rate: int | None = None
        self._audio_channels: int | None = None
        self._audio_time_base: Fraction | None = None

        self._last_stream_id: int | None = None
        self._max_timeline_seconds: Fraction = Fraction(0)
        self._last_sequence: float = -float("inf")
        self._packets_written: int = 0
        self._closed: bool = False
        self._epoch_shift_seconds = Fraction(0)

    def write_segment(self, segment: SegmentSnapshot) -> None:
        """Demux and remux a segment snapshot into the output container."""
        if self._closed:
            raise StreamMuxError("Cannot write to closed StreamMuxWriter")

        if segment.sequence <= self._last_sequence:
            return
        self._last_sequence = segment.sequence

        try:
            import av  # Lazy import PyAV
        except ImportError as err:
            raise StreamMuxError("PyAV ('av') package is required for stream remuxing") from err

        source_bytes = segment.init + segment.data
        if not source_bytes:
            return

        source = av.open(BytesIO(source_bytes), "r", format=SEGMENT_CONTAINER_FORMAT)
        try:
            if len(source.streams.video) != 1 or len(source.streams.audio) > 1:
                raise IncompatibleStreamError("Segment must contain exactly one video and at most one audio stream")
            source_v = source.streams.video[0] if source.streams.video else None
            if not source_v:
                raise IncompatibleStreamError("Segment contains no video stream")
            source_a = source.streams.audio[0] if source.streams.audio else None

            # Initialize output container and add ALL template streams before start_encoding()
            if not self._output_container:
                self.tmp_path.parent.mkdir(parents=True, exist_ok=True)
                container_options: dict[str, str] = {
                    "video_track_timescale": str(int(1 / source_v.time_base)),
                    "movflags": "frag_keyframe+empty_moov",
                    "min_frag_duration": str(int(self.min_segment_duration * 1_000_000)),
                }
                self._output_container = av.open(
                    str(self.tmp_path),
                    "w",
                    format=RECORDER_CONTAINER_FORMAT,
                    container_options=container_options,
                )

                # Add video stream
                self._output_v = self._output_container.add_stream_from_template(source_v)
                if source_v.name == "hevc":
                    self._output_v.codec_context.codec_tag = "hvc1"
                self._video_codec = source_v.name
                self._video_width = source_v.width
                self._video_height = source_v.height
                self._video_time_base = Fraction(source_v.time_base)

                # Add audio stream before header/start_encoding
                if source_a:
                    self._output_a = self._output_container.add_stream_from_template(source_a)
                    self._audio_codec = source_a.name
                    self._audio_rate = getattr(source_a, "sample_rate", None) or getattr(
                        source_a.codec_context, "sample_rate", None
                    )
                    self._audio_channels = getattr(source_a, "channels", None) or getattr(
                        source_a.codec_context, "channels", None
                    )
                    self._audio_time_base = Fraction(source_a.time_base)

                # Finalize header and establish output stream time bases
                self._output_container.start_encoding()
            else:
                # Subsequent segment: verify stream/codec compatibility
                if source_v.name != self._video_codec:
                    raise IncompatibleStreamError(
                        f"Video codec changed across segments from {self._video_codec} to {source_v.name}"
                    )
                if source_v.width != self._video_width or source_v.height != self._video_height:
                    raise IncompatibleStreamError(
                        f"Video resolution changed across segments: expected {self._video_width}x{self._video_height}, "
                        f"got {source_v.width}x{source_v.height}"
                    )
                if Fraction(source_v.time_base) != self._video_time_base:
                    raise IncompatibleStreamError(
                        f"Video timebase changed across segments: expected {self._video_time_base}, "
                        f"got {Fraction(source_v.time_base)}"
                    )

                if bool(source_a) != bool(self._output_a):
                    raise IncompatibleStreamError(
                        f"Audio stream presence changed across segments: "
                        f"expected {bool(self._output_a)}, got {bool(source_a)}"
                    )
                if source_a:
                    if source_a.name != self._audio_codec:
                        raise IncompatibleStreamError(
                            f"Audio codec changed across segments: "
                            f"expected {self._audio_codec}, got {source_a.name}"
                        )
                    a_rate = getattr(source_a, "sample_rate", None) or getattr(
                        source_a.codec_context, "sample_rate", None
                    )
                    if self._audio_rate is not None and a_rate is not None and a_rate != self._audio_rate:
                        raise IncompatibleStreamError(
                            f"Audio sample rate changed across segments: "
                            f"expected {self._audio_rate}, got {a_rate}"
                        )
                    a_channels = getattr(source_a, "channels", None) or getattr(
                        source_a.codec_context, "channels", None
                    )
                    if (
                        self._audio_channels is not None
                        and a_channels is not None
                        and a_channels != self._audio_channels
                    ):
                        raise IncompatibleStreamError(
                            f"Audio channels changed across segments: "
                            f"expected {self._audio_channels}, got {a_channels}"
                        )
                    if Fraction(source_a.time_base) != self._audio_time_base:
                        raise IncompatibleStreamError(
                            f"Audio timebase changed across segments: "
                            f"expected {self._audio_time_base}, got {Fraction(source_a.time_base)}"
                        )

            # Discontinuity epoch alignment across stream restarts
            source_start_seconds = (
                Fraction(source.start_time, av.time_base)
                if source.start_time is not None
                else Fraction(0)
            )
            if self._last_stream_id != segment.stream_id:
                self._last_stream_id = segment.stream_id
                if self._packets_written:
                    # start_time is the absolute origin of the new source epoch;
                    # duration is only a duration and is not an absolute timestamp.
                    self._epoch_shift_seconds = self._max_timeline_seconds - source_start_seconds

            out_v_tb = Fraction(self._output_v.time_base)
            out_a_tb = Fraction(self._output_a.time_base) if self._output_a else None

            # Remux packets with monotonic DTS correction and explicit output time base
            for packet in source.demux():
                stype = packet.stream.type
                if stype == "video":
                    out_stream = self._output_v
                    out_tb = out_v_tb
                elif stype == "audio":
                    if not self._output_a:
                        raise IncompatibleStreamError(
                            "Encountered audio packet but recording has no audio stream"
                        )
                    out_stream = self._output_a
                    out_tb = out_a_tb
                else:
                    raise IncompatibleStreamError(f"Encountered unsupported stream type '{stype}'")

                packet_size = getattr(packet, "size", 0)

                # Skip only empty flush packets at EOF
                if packet_size == 0 and packet.pts is None and packet.dts is None:
                    continue

                # Meaningful non-empty packet with missing timestamps fails explicitly
                if packet.pts is None or packet.dts is None:
                    raise MissingTimestampError(
                        f"Non-empty packet in stream '{stype}' has absent timestamp "
                        f"(pts={packet.pts}, dts={packet.dts}, size={packet_size})"
                    )

                in_tb = Fraction(packet.time_base or packet.stream.time_base)
                assert out_tb is not None

                pts, dts = self.normalizer.normalize(
                    packet_pts=packet.pts,
                    packet_dts=packet.dts,
                    stream_type=stype,
                    in_time_base=in_tb,
                    out_time_base=out_tb,
                    epoch_shift=self._epoch_shift_seconds,
                )

                original_duration = packet.duration
                if packet.duration is not None and packet.duration > 0:
                    packet.duration = round(packet.duration * in_tb / out_tb)

                packet.pts = pts
                packet.dts = dts
                packet.time_base = out_stream.time_base
                packet.stream = out_stream
                self._output_container.mux(packet)
                self._packets_written += 1

                # Maintain maximum output timeline from actual normalized packet end
                dur = round(original_duration * in_tb / out_tb) if original_duration and original_duration > 0 else 0
                pkt_end_seconds = max(
                    Fraction(pts + dur, 1) * out_tb,
                    Fraction(dts + dur, 1) * out_tb,
                )
                if pkt_end_seconds > self._max_timeline_seconds:
                    self._max_timeline_seconds = pkt_end_seconds
        except Exception:
            self.close()
            raise
        finally:
            source.close()

    def finish(self) -> dict[str, Any]:
        """Finalize the container, apply rotation matrix, and rename atomically."""
        if self._closed:
            return {"packets_written": self._packets_written, "status": "already_closed"}

        try:
            if self._output_container:
                self._output_container.close()
                self._output_container = None

            if self._packets_written == 0 or not self.tmp_path.is_file() or self.tmp_path.stat().st_size == 0:
                self.close()
                raise EmptyRecordingError(
                    f"Recording target '{self.target_path}' produced no packets or empty output"
                )

            # Apply orientation matrix if needed
            self._apply_orientation_and_rename()

            return {
                "packets_written": self._packets_written,
                "target_path": str(self.target_path),
                "size_bytes": self.target_path.stat().st_size if self.target_path.is_file() else 0,
            }
        finally:
            self.close()

    def _apply_orientation_and_rename(self) -> None:
        """Apply orientation box if required, then atomically promote to final path."""
        if self.orientation not in (1, None):
            transform_tmp = self.tmp_path.with_suffix(".transform.tmp")
            try:
                from homeassistant.components.stream.fmp4utils import read_init, transform_init

                with open(self.tmp_path, "rb") as in_file, open(transform_tmp, "wb") as out_file:
                    init_data = transform_init(read_init(in_file), self.orientation)
                    out_file.write(init_data)
                    in_file.seek(len(init_data))
                    while chunk := in_file.read(65536):
                        out_file.write(chunk)

                # Atomically promote transform tmp to final target
                os.replace(str(transform_tmp), str(self.target_path))
                self.tmp_path.unlink(missing_ok=True)
                return
            except Exception as exc:
                transform_tmp.unlink(missing_ok=True)
                _LOGGER.error(
                    "Failed to apply MP4 orientation transform (%s); aborting promotion",
                    type(exc).__name__,
                )
                raise StreamMuxError(f"Orientation transform failed: {type(exc).__name__}") from exc

        # Direct atomic replacement when no transform needed
        os.replace(str(self.tmp_path), str(self.target_path))

    def close(self) -> None:
        """Ensure all resources are closed and partial files unlinked."""
        self._closed = True
        if self._output_container is not None:
            try:
                self._output_container.close()
            except Exception:
                pass
            self._output_container = None

        if self.tmp_path.is_file():
            try:
                self.tmp_path.unlink(missing_ok=True)
            except Exception:
                pass
        transform_tmp = self.tmp_path.with_suffix(".transform.tmp")
        if transform_tmp.is_file():
            try:
                transform_tmp.unlink(missing_ok=True)
            except Exception:
                pass
