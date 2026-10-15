"""Small local video previews, independent of any event analyzer.

Derived files are disposable: source media stays authoritative and is never changed.
Generation runs off-loop, uses local inputs only, and has a bounded runtime.
"""

from __future__ import annotations

import asyncio
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any


def thumbnail_path(root: Path, event: Any) -> Path:
    """Resolve a revision-specific derivative, rejecting traversal and symlinks."""
    resolved = root.resolve()
    target = resolved / "thumbnails" / f"{event.event_id}_r{event.media_revision}_thumb.jpg"
    if not target.resolve().is_relative_to(resolved) or target.is_symlink():
        raise ValueError("Invalid thumbnail path")
    return target


def generate_thumbnail(root: Path, event: Any, binary: str | None) -> bool:
    """Create one representative JPEG from the middle of a finalized clip."""
    target = thumbnail_path(root, event)
    if target.is_file():
        return True
    binary = binary or shutil.which("ffmpeg")
    if not binary:
        return False
    videos = [m for m in event.media if m.available and m.kind == "video"]
    if not videos:
        return False
    # Prefer a middle segment over the first pre-roll frame.
    item = videos[len(videos) // 2]
    source = (root.resolve() / item.file_name).resolve()
    if not source.is_relative_to(root.resolve()) or not source.is_file():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(dir=target.parent, suffix=".jpg", delete=False) as output:
            temporary = Path(output.name)
        duration = max(0.0, float(item.duration or 0))
        result = subprocess.run(
            [binary, "-nostdin", "-v", "error", "-threads", "1", "-protocol_whitelist", "file",
             "-ss", str(round(duration * .35, 3)), "-i", str(source), "-an", "-sn",
             "-vf", "thumbnail=20,scale=400:-2", "-frames:v", "1", "-threads", "1",
             "-q:v", "5", "-y", str(temporary)],
            capture_output=True, timeout=10, check=False,
        )
        if result.returncode or not temporary.is_file() or temporary.stat().st_size == 0:
            return False
        os.replace(temporary, target)
        return True
    except (OSError, ValueError, subprocess.SubprocessError):
        return False
    finally:
        if temporary:
            temporary.unlink(missing_ok=True)


async def async_ensure_thumbnail(hass: Any, store: Any, event: Any) -> None:
    """Bound concurrent extraction; failures leave the card's icon fallback usable."""
    from .api import get_api
    from .const import DOMAIN

    if not any(m.available and m.kind == "video" for m in event.media):
        return
    api = get_api(hass)
    binary = api.ffmpeg_binary if api else None
    semaphore = hass.data[DOMAIN].setdefault("thumbnail_semaphore", asyncio.Semaphore(2))
    async with semaphore:
        await hass.async_add_executor_job(generate_thumbnail, store.media_root, event, binary)
