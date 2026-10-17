"""Media validation, probing, and atomic filesystem promotion for Event Recording.

Strict boundary: Zero Home Assistant imports, zero AI imports.
Fail-closed security: Enforces path containment within allowed media roots,
rejects URL fetches, and validates media assets with injected probes.
"""

from __future__ import annotations

import errno
import hashlib
import json
import math
import os
import re
import shutil
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .validation import ValidationError


class MediaImportError(Exception):
    """Base exception for media import and validation failures."""


class ProbeUnavailableError(MediaImportError):
    """Raised when an external probe (e.g. ffprobe) is not installed or configured."""


class InvalidMediaError(MediaImportError):
    """Raised when media data fails container, stream, or codec validation."""


class PathTraversalSecurityError(MediaImportError, ValidationError):
    """Raised when an import path violates containment boundaries or attempts URL fetch."""


class MediaTypeMismatchError(MediaImportError):
    """Raised when probe result does not match expected media kind or MIME type."""


@dataclass(frozen=True)
class MediaProbeResult:
    """Standardized result of media container and stream inspection."""

    kind: str  # "video" | "image"
    mime_type: str
    duration: float | None = None
    width: int | None = None
    height: int | None = None


class MediaProbe(Protocol):
    """Protocol for injected media container and stream probes."""

    def probe(self, path: Path) -> MediaProbeResult:
        """Inspect media file and return verified container/stream metadata."""
        ...


class FFmpegProbe(MediaProbe):
    """Real media probe utilizing the ffprobe binary.

    Fails closed if the binary is missing, if streams are corrupt or missing,
    or if video duration and dimensions are non-positive or non-finite.
    """

    def __init__(self, ffprobe_binary: str | None = None) -> None:
        self._binary = ffprobe_binary or shutil.which("ffprobe")

    @property
    def is_available(self) -> bool:
        """Check if ffprobe binary is available."""
        return self._binary is not None

    def probe(self, path: Path) -> MediaProbeResult:
        """Probe media file using ffprobe JSON report."""
        if not self._binary:
            raise ProbeUnavailableError("ffprobe executable not found on system")

        cmd = [
            self._binary,
            "-v",
            "error",
            "-show_entries",
            "format=duration,format_name:stream=codec_type,codec_name,width,height",
            "-of",
            "json",
            str(path),
        ]
        try:
            res = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        except Exception as err:
            raise InvalidMediaError(f"Failed to execute ffprobe: {err}") from err

        if res.returncode != 0:
            raise InvalidMediaError(f"ffprobe validation failed: {res.stderr.strip()}")

        try:
            data = json.loads(res.stdout)
        except json.JSONDecodeError as err:
            raise InvalidMediaError("ffprobe returned invalid JSON output") from err

        format_info = data.get("format", {})
        streams = data.get("streams", [])

        if not streams:
            raise InvalidMediaError("No streams found in media container")

        # Check for video stream first
        for st in streams:
            if st.get("codec_type") == "video":
                codec = st.get("codec_name", "").lower()
                dur_raw = format_info.get("duration") or st.get("duration")
                width = st.get("width")
                height = st.get("height")

                # Verify dimensions are positive integers
                if not isinstance(width, int) or width <= 0 or not isinstance(height, int) or height <= 0:
                    raise InvalidMediaError(f"Invalid or missing media dimensions: {width}x{height}")

                # Check for still image codec encoded as video stream without duration
                is_zero_dur = False
                if dur_raw is not None:
                    try:
                        dur_float = float(dur_raw)
                        is_zero_dur = dur_float <= 0.0
                    except (ValueError, TypeError):
                        is_zero_dur = True

                fmt_name = format_info.get("format_name", "").lower()
                still_container = any(fmt in fmt_name for fmt in ("image2", "jpeg_pipe", "png_pipe", "webp_pipe", "bmp_pipe"))
                if codec in ("mjpeg", "png", "bmp", "webp") and (still_container or dur_raw is None or is_zero_dur):
                    mime = "image/jpeg" if codec == "mjpeg" else f"image/{codec}"
                    return MediaProbeResult(
                        kind="image",
                        mime_type=mime,
                        duration=None,
                        width=width,
                        height=height,
                    )

                # Validate positive finite video duration
                if dur_raw is None:
                    raise InvalidMediaError("Missing video duration in container")
                try:
                    duration = float(dur_raw)
                except (ValueError, TypeError) as err:
                    raise InvalidMediaError(f"Invalid duration format: {dur_raw}") from err

                if not math.isfinite(duration) or duration <= 0.0:
                    raise InvalidMediaError(f"Video duration must be positive and finite, got {duration}")

                fmt_name = format_info.get("format_name", "").lower()
                if any(x in fmt_name for x in ("mp4", "quicktime", "mov")):
                    mime = "video/mp4"
                elif any(x in fmt_name for x in ("webm", "matroska")):
                    mime = "video/webm"
                else:
                    raise InvalidMediaError(f"Unsupported video container format: {fmt_name}")

                return MediaProbeResult(
                    kind="video",
                    mime_type=mime,
                    duration=duration,
                    width=width,
                    height=height,
                )

        # Check for image formats
        fmt_name = format_info.get("format_name", "").lower()
        if any(x in fmt_name for x in ("image2", "jpeg", "png", "webp")):
            first_stream = streams[0]
            width = first_stream.get("width")
            height = first_stream.get("height")
            if not isinstance(width, int) or width <= 0 or not isinstance(height, int) or height <= 0:
                raise InvalidMediaError(f"Invalid or missing image dimensions: {width}x{height}")

            codec = first_stream.get("codec_name", "").lower()
            if codec in ("mjpeg", "jpeg") or "jpeg" in fmt_name:
                mime = "image/jpeg"
            elif codec == "png" or "png" in fmt_name:
                mime = "image/png"
            elif codec == "webp" or "webp" in fmt_name:
                mime = "image/webp"
            else:
                raise InvalidMediaError(f"Unsupported image format: {fmt_name}")

            return MediaProbeResult(
                kind="image",
                mime_type=mime,
                duration=None,
                width=width,
                height=height,
            )

        raise InvalidMediaError("No recognized video or image streams found")


def verify_media_path(path: str | Path, allowed_roots: Sequence[str | Path]) -> Path:
    """Validate containment of media file strictly within allowed root paths.

    Rejects remote URLs, symlink directory escapes, and non-existent files.
    Enforces containment check BEFORE invoking file.is_file/stat to prevent unauthorized file probing.
    """
    path_str = str(path).strip()
    lower_str = path_str.lower()
    if (
        lower_str.startswith("http:")
        or lower_str.startswith("https:")
        or lower_str.startswith("rtsp:")
        or lower_str.startswith("file:")
        or lower_str.startswith("ftp:")
    ):
        raise PathTraversalSecurityError("Remote URL fetches are strictly forbidden")

    try:
        resolved_path = Path(path).resolve()
    except Exception as err:
        raise PathTraversalSecurityError(f"Invalid path expression: {err}") from err

    contained = False
    for root in allowed_roots:
        try:
            resolved_root = Path(root).resolve()
            if resolved_path.is_relative_to(resolved_root):
                contained = True
                break
        except Exception:
            continue

    if not contained:
        raise PathTraversalSecurityError(
            "Source path is not within any permitted media root"
        )

    if not resolved_path.is_file():
        raise FileNotFoundError(f"Source media file not found: {resolved_path}")

    return resolved_path


def stage_and_promote_media(
    src_path: Path | str,
    owned_media_root: Path,
    event_id: str,
    media_id: str,
    extension: str,
    adopt_staging: bool = False,
) -> tuple[str, int, str]:
    """Atomically stage and promote media asset into owned root.

    When adopt_staging is False (default for external imports), creates a duplicate
    copy in .staging before atomic rename into destination, preserving the source untouched.

    When adopt_staging is True (for internal closed/settled capture outputs):
    - Validates that src_path is strictly within owned .staging and not a symlink.
    - Computes SHA-256 and size in streaming chunks off-loop without writing duplicate bytes.
    - Atomically moves src_path into the final destination directory via os.replace.
    - Handles cross-device renames safely by copying to a temporary destination on the target
      filesystem before atomic replace.

    Returns (root_relative_file_name, size_bytes, sha256_hex).
    """
    # Reject control or path components in event_id or media_id
    for name, val in (("event_id", event_id), ("media_id", media_id)):
        if not isinstance(val, str) or not val.strip():
            raise PathTraversalSecurityError(f"{name} cannot be empty")
        clean = val.strip()
        if any(c in clean for c in "/\\..\0\r\n\t") or not re.match(r"^[a-zA-Z0-9_-]+$", clean):
            raise PathTraversalSecurityError(f"Invalid characters or traversal attempt in {name}")

    clean_ext = extension if extension.startswith(".") else f".{extension}"
    owned_root = owned_media_root.resolve()

    target_dir = (owned_root / event_id).resolve()
    if not target_dir.is_relative_to(owned_root):
        raise PathTraversalSecurityError(f"Target directory escapes owned root: {target_dir}")

    target_dir.mkdir(parents=True, exist_ok=True)
    final_file_name = f"{media_id}{clean_ext}"
    final_path = (target_dir / final_file_name).resolve()
    if not final_path.is_relative_to(owned_root):
        raise PathTraversalSecurityError(f"Target file escapes owned root: {final_path}")

    staging_dir = owned_media_root / ".staging"
    if staging_dir.is_symlink():
        raise PathTraversalSecurityError("Cannot adopt media from symlinked staging directory")
    resolved_staging = staging_dir.resolve()
    if not resolved_staging.is_relative_to(owned_root):
        raise PathTraversalSecurityError("Staging directory escapes owned root")
    if Path(owned_media_root).is_symlink() and not owned_root.is_relative_to(Path(owned_media_root).parent.resolve()):
        raise PathTraversalSecurityError("Owned media root symlink escapes intended root")
    src_path = Path(src_path)

    if adopt_staging:
        # Enforce containment, non-symlink, and regular file invariant
        if src_path.is_symlink():
            raise PathTraversalSecurityError("Cannot adopt symlinked staging file")
        resolved_src = src_path.resolve()
        if resolved_src.parent != resolved_staging or resolved_src == resolved_staging:
            raise PathTraversalSecurityError(
                "Cannot adopt media outside owned .staging directory"
            )
        if not resolved_src.is_file():
            raise FileNotFoundError("Staging file not found for adoption")

        # Stream hash calculation directly from settled staging file (zero duplicate write)
        hasher = hashlib.sha256()
        size_bytes = 0
        with open(resolved_src, "rb") as f_in:
            while chunk := f_in.read(65536):
                hasher.update(chunk)
                size_bytes += len(chunk)

        # Atomic move into final path (same-filesystem zero-copy rename)
        try:
            os.replace(resolved_src, final_path)
        except OSError as err:
            is_cross_dev = (
                getattr(err, "errno", None) == errno.EXDEV
                or getattr(err, "winerror", None) == 17
            )
            if is_cross_dev:
                temp_dest = target_dir / f"{media_id}.tmp"
                try:
                    shutil.copyfile(resolved_src, temp_dest)
                    os.replace(temp_dest, final_path)
                    try:
                        resolved_src.unlink()
                    except OSError:
                        final_path.unlink(missing_ok=True)
                        raise
                finally:
                    if temp_dest.exists():
                        try:
                            temp_dest.unlink(missing_ok=True)
                        except Exception:
                            pass
            else:
                raise

        rel_path = f"{event_id}/{final_file_name}"
        return rel_path, size_bytes, hasher.hexdigest()

    # External import path (adopt_staging=False): preserve source untouched
    resolved_staging.mkdir(parents=True, exist_ok=True)
    temp_target = resolved_staging / f"{event_id}_{media_id}.tmp"

    hasher = hashlib.sha256()
    size_bytes = 0

    try:
        with open(src_path, "rb") as f_in, open(temp_target, "wb") as f_out:
            while chunk := f_in.read(65536):
                hasher.update(chunk)
                f_out.write(chunk)
                size_bytes += len(chunk)

        os.replace(temp_target, final_path)
    finally:
        if temp_target.exists():
            try:
                temp_target.unlink(missing_ok=True)
            except Exception:
                pass

    rel_path = f"{event_id}/{final_file_name}"
    return rel_path, size_bytes, hasher.hexdigest()


def unlink_promoted_media(owned_media_root: Path, rel_file_name: str) -> bool:
    """Safely remove a promoted media file under owned media root with containment check."""
    try:
        owned_root = owned_media_root.resolve()
        target_path = (owned_root / rel_file_name).resolve()
        if target_path.is_relative_to(owned_root) and target_path.is_file():
            target_path.unlink()
            return True
    except Exception:
        pass
    return False
