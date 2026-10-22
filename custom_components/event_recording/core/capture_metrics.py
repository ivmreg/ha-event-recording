"""Bounded producer duration measurements, separate from timeline verification."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any

MAX_CAPTURE_REPORTS = 64
_SECONDS = (
    "requested_live_seconds", "captured_live_seconds", "live_shortfall_seconds",
    "requested_preroll_seconds", "obtained_preroll_seconds", "probed_duration_seconds",
)
_REASONS = frozenset({
    "STREAM_CAPTURE_FAILED", "STREAM_CAPTURE_TIMEOUT", "STREAM_SEQUENCE_GAP",
    "STREAM_PROVIDER_LOST", "PREROLL_UNAVAILABLE", "BUFFER_MEMORY_LIMIT",
})


def normalize_capture_metrics(value: Any) -> dict[str, Any]:
    """Drop unknown/private keys and invalid values without rejecting valid media."""
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, Any] = {}
    if value.get("provider") in ("home_assistant", "go2rtc"):
        result["provider"] = value["provider"]
    for key in _SECONDS:
        number = value.get(key)
        if (
            isinstance(number, (int, float)) and not isinstance(number, bool)
            and 0 <= number <= 86400 and math.isfinite(number)
        ):
            result[key] = round(float(number), 4)
    if isinstance(value.get("partial_error"), str) and value["partial_error"] in _REASONS:
        result["partial_error"] = value["partial_error"]
    return result


def chunk_capture_metrics(
    result: Any, *, provider: str, requested_live_seconds: float,
    requested_preroll_seconds: float, partial_error: str | None,
) -> dict[str, Any]:
    """Retain measured adapter output; missing measurements remain absent."""
    result = result if isinstance(result, Mapping) else {}
    return normalize_capture_metrics({
        "provider": provider,
        "requested_live_seconds": requested_live_seconds,
        "requested_preroll_seconds": requested_preroll_seconds,
        "captured_live_seconds": result.get(
            "captured_live_seconds", result.get("captured_live_duration")
        ),
        "live_shortfall_seconds": result.get("duration_shortfall"),
        "obtained_preroll_seconds": result.get("obtained_preroll_seconds"),
        "partial_error": partial_error,
    })


def retained_capture_metrics(
    media: Iterable[Any], previous_coverage: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Keep the latest 64 available-media reports, with explicit truncation.

    Reports live in existing coverage JSON. Recomputing trusted interval coverage
    must neither discard these measurements nor treat them as trusted clocks.
    """
    previous = previous_coverage if isinstance(previous_coverage, Mapping) else {}
    reports = previous.get("capture_metrics")
    reports = reports if isinstance(reports, Mapping) else {}
    retained = {}
    for row in media:
        try:
            media_id, available = row["id"], row["available"]
        except (KeyError, IndexError, TypeError):
            continue
        if available and isinstance(media_id, str) and media_id in reports:
            report = normalize_capture_metrics(reports[media_id])
            if report:
                retained[media_id] = report
    truncated = previous.get("capture_metrics_truncated") is True or len(retained) > MAX_CAPTURE_REPORTS
    retained = dict(list(retained.items())[-MAX_CAPTURE_REPORTS:])
    if not retained and not truncated:
        return {}
    return {"capture_metrics": retained, "capture_metrics_truncated": truncated}
