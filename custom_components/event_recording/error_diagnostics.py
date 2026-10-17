"""Bounded structural diagnostics without exception messages or dynamic names.

Only fixed exception labels, fixed module tags and numeric line locations leave
this formatter. Unknown types/frames lose detail deliberately. No filesystem or
source inspection takes place in the Home Assistant event loop.
"""

from __future__ import annotations

import sys
from collections import deque
from types import TracebackType

DEFAULT_MAX_FRAMES = 4
DEFAULT_MAX_DEPTH = 3
DEFAULT_MAX_CHARS = 256

_BUILTIN_TYPES = (
    (PermissionError, "PermissionError"),
    (FileNotFoundError, "FileNotFoundError"),
    (ConnectionRefusedError, "ConnectionRefusedError"),
    (ConnectionResetError, "ConnectionResetError"),
    (ConnectionAbortedError, "ConnectionAbortedError"),
    (BrokenPipeError, "BrokenPipeError"),
    (TimeoutError, "TimeoutError"),
    (ConnectionError, "ConnectionError"),
    (OSError, "OSError"),
    (ValueError, "ValueError"),
    (TypeError, "TypeError"),
    (KeyError, "KeyError"),
    (AttributeError, "AttributeError"),
    (RuntimeError, "RuntimeError"),
    (AssertionError, "AssertionError"),
    (ImportError, "ImportError"),
    (Exception, "Exception"),
)
# Resolve only fixed keys in modules already loaded by HA. Never emit arbitrary
# class metadata, including forged module/name/qualname strings.
_KNOWN_TYPES = (
    ("homeassistant.exceptions", "HomeAssistantError", "ha.HomeAssistantError"),
    ("homeassistant.exceptions", "ServiceValidationError", "ha.ServiceValidationError"),
    ("homeassistant.exceptions", "Unauthorized", "ha.Unauthorized"),
    ("aiohttp.client_exceptions", "ClientConnectorError", "aiohttp.ClientConnectorError"),
    ("aiohttp.client_exceptions", "ClientConnectionError", "aiohttp.ClientConnectionError"),
    ("aiohttp.client_exceptions", "ClientResponseError", "aiohttp.ClientResponseError"),
    ("av.error", "InvalidDataError", "av.InvalidDataError"),
    ("av.error", "FFmpegError", "av.FFmpegError"),
)
_FRAME_TAGS = {
    "homeassistant.components.camera": "ha.camera",
    "homeassistant.components.stream": "ha.stream",
    "homeassistant.components.stream.core": "ha.stream.core",
    "homeassistant.components.stream.worker": "ha.stream.worker",
    "homeassistant.core": "ha.core",
    "homeassistant.helpers.service": "ha.service",
    "custom_components.event_recording.recorder": "recorder",
    "custom_components.event_recording.capture": "capture",
    "asyncio.tasks": "asyncio.tasks",
}


def _type_label(exc: BaseException) -> str:
    cls = type(exc)
    for candidate, label in _BUILTIN_TYPES:
        if cls is candidate:
            return label
    for module_name, class_name, label in _KNOWN_TYPES:
        module = sys.modules.get(module_name)
        if module is not None and vars(module).get(class_name) is cls:
            return label
    return "Exception"


def _frames(tb: TracebackType | None, count: int) -> list[str]:
    result: deque[str] = deque(maxlen=count)
    for _ in range(40):
        if tb is None or count == 0:
            break
        module_name = tb.tb_frame.f_globals.get("__name__")
        tag = _FRAME_TAGS.get(module_name, "other") if type(module_name) is str else "other"
        result.append(f"{tag}:{tb.tb_lineno}")
        tb = tb.tb_next
    return list(result)


def _bound(value: int, default: int, maximum: int) -> int:
    return max(0, min(value, maximum)) if type(value) is int else default


def format_safe_exception_diagnostics(
    exc: BaseException | None,
    *,
    max_frames: int = DEFAULT_MAX_FRAMES,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_chars: int = DEFAULT_MAX_CHARS,
) -> str:
    """Return fixed labels/locations, with at most 3 types and 4 frames by default.

    Hard caps: 1024 characters, 4 exception levels, 8 frames, 40 traceback visits.
    Raw messages, args, notes, locals, filenames and function names are omitted.
    All paths respect the output budget; small budgets may omit type details.
    """
    budget = _bound(max_chars, DEFAULT_MAX_CHARS, 1024)
    frame_count = _bound(max_frames, DEFAULT_MAX_FRAMES, 8)
    depth = _bound(max_depth, DEFAULT_MAX_DEPTH, 4)
    text = "diagnostics_unavailable"
    try:
        if budget == 0 or depth == 0:
            return ""
        if not isinstance(exc, BaseException):
            text = "none"
        else:
            labels: list[str] = []
            seen: set[int] = set()
            deepest_tb = None
            for _ in range(depth):
                if exc is None:
                    break
                if id(exc) in seen:
                    labels.append("(circular_chain)")
                    break
                seen.add(id(exc))
                labels.append(_type_label(exc))
                if exc.__traceback__ is not None:
                    deepest_tb = exc.__traceback__
                next_exc = exc.__cause__
                if next_exc is None and not exc.__suppress_context__:
                    next_exc = exc.__context__
                exc = next_exc
            text = "types=" + " caused_by ".join(labels)
            locations = _frames(deepest_tb, frame_count)
            if locations:
                text += " | frames=[" + " -> ".join(locations) + "]"
    except Exception:
        # Do not expose details from a formatting failure either.
        pass
    if len(text) <= budget:
        return text
    return text[: budget - 3] + "..." if budget > 3 else text[:budget]
