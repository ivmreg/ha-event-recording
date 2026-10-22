"""Evidence-based coverage computation for recorded events.

Coverage is derived from observed media intervals, never from a sticky
"uncertain" flag. The module is deliberately Home Assistant-free so the same
rules can be unit-tested without an HA runtime.

Definitions
-----------
requested window
    ``events.start_at`` .. ``events.desired_capture_end``. This is the logical
    interval the recorder was asked to observe. It is not a claim about media.

observed interval
    Per media item, ``capture_start`` .. ``capture_end``. Only producers that
    measured a clock may populate it. Missing intervals are unknown, not gaps.

covered seconds
    Length of the union of observed intervals after clipping to the requested
    window. Overlapping lookback chunks therefore count once.

gaps
    ``requested_length - covered_seconds``. ``None`` when any contributing
    media has no trustworthy interval; a missing measurement is not zero.

verification
    ``verified``  trusted clocks, trusted continuity, gaps within tolerance
    ``partial``   trusted clocks, but a measured gap or broken continuity
    ``unverified`` at least one video item cannot be placed on the timeline
    ``instant``   image-only observation (a still observes one instant)
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any

from .capture_metrics import retained_capture_metrics
from .models import ensure_utc, format_iso

COVERAGE_VERSION = 2

#: A verified recording may end a fraction of a second short of the requested
#: boundary because capture stops on completed stream segments. Larger gaps are
#: reported as partial with a measured value.
DEFAULT_GAP_TOLERANCE_SECONDS = 0.5

TRUSTED_TIME_QUALITIES = frozenset({"host_recorded", "provider_reported"})
TRUSTED_CLOCK_SOURCES = frozenset(
    {"ha_stream", "go2rtc", "frigate_nvr", "ntp", "ptp", "wall_clock"}
)
TRUSTED_CONTINUITIES = frozenset(
    {"verified_segments", "contiguous", "continuous", "provider_verified"}
)
BROKEN_CONTINUITIES = frozenset({"broken", "provider_partial"})
VALID_CONTINUITIES = frozenset(
    {
        *TRUSTED_CONTINUITIES,
        *BROKEN_CONTINUITIES,
        "unverified",
        "instant",
    }
)


def _get(row: Mapping[str, Any] | Any, key: str, default: Any = None) -> Any:
    """Read one field from a sqlite3.Row, dict, or model-like object."""
    try:
        value = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if value is None else value


def _as_utc(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return ensure_utc(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return ensure_utc(datetime.fromisoformat(text))
        except ValueError:
            return None
    return None


def _merge_intervals(
    intervals: Iterable[tuple[datetime, datetime]],
) -> list[tuple[datetime, datetime]]:
    ordered = sorted(intervals, key=lambda item: item[0])
    merged: list[tuple[datetime, datetime]] = []
    for start, end in ordered:
        if end <= start:
            continue
        if merged and start <= merged[-1][1]:
            previous_start, previous_end = merged[-1]
            if end > previous_end:
                merged[-1] = (previous_start, end)
        else:
            merged.append((start, end))
    return merged


def _seconds(start: datetime, end: datetime) -> float:
    return max(0.0, (end - start).total_seconds())


def compute_coverage(
    media: Iterable[Mapping[str, Any] | Any],
    *,
    requested_start: datetime | None,
    requested_end: datetime | None,
    gap_tolerance_seconds: float = DEFAULT_GAP_TOLERANCE_SECONDS,
    previous_coverage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Recompute timeline evidence and preserve bounded producer measurements."""
    rows = list(media)
    coverage = _compute_coverage(
        rows, requested_start=requested_start, requested_end=requested_end,
        gap_tolerance_seconds=gap_tolerance_seconds,
    )
    req_start, req_end = _as_utc(requested_start), _as_utc(requested_end)
    coverage["requested_seconds"] = (
        round(_seconds(req_start, req_end), 4)
        if req_start is not None and req_end is not None and req_end >= req_start else None
    )
    coverage.update(retained_capture_metrics(rows, previous_coverage))
    return coverage


def _compute_coverage(
    media: Iterable[Mapping[str, Any] | Any],
    *,
    requested_start: datetime | None,
    requested_end: datetime | None,
    gap_tolerance_seconds: float = DEFAULT_GAP_TOLERANCE_SECONDS,
) -> dict[str, Any]:
    """Derive an event coverage payload from the current media set.

    The returned mapping is JSON-serialisable and intentionally includes
    ``actual_duration``/``is_uncertain``/``gaps`` for backward compatibility.
    """
    rows = [row for row in media if bool(_get(row, "available", True))]
    video = [row for row in rows if _get(row, "kind") == "video"]
    images = [row for row in rows if _get(row, "kind") == "image"]

    actual_duration = round(
        sum(float(_get(row, "duration", 0.0) or 0.0) for row in video), 4
    )

    if not video:
        if images:
            # A still is an observation of one instant. It is honest to call it
            # complete for that instant, but it must never claim interval cover.
            stamp = None
            for row in images:
                stamp = _as_utc(_get(row, "capture_start")) or _as_utc(
                    _get(row, "created_at")
                )
                if stamp is not None:
                    break
            return {
                "actual_duration": 0.0,
                "covered_seconds": 0.0,
                "gaps": 0.0,
                "gap_seconds": 0.0,
                "overlap_seconds": 0.0,
                "is_uncertain": False,
                "verification": "instant",
                "reason": None,
                "observed_start": format_iso(stamp),
                "observed_end": format_iso(stamp),
                "clock_sources": sorted(
                    {
                        str(_get(row, "clock_source"))
                        for row in images
                        if _get(row, "clock_source")
                    }
                ),
                "continuity": "instant",
                "coverage_version": COVERAGE_VERSION,
            }
        return {
            "actual_duration": actual_duration,
            "covered_seconds": 0.0,
            "gaps": None,
            "gap_seconds": None,
            "overlap_seconds": 0.0,
            "is_uncertain": True,
            "verification": "unverified",
            "reason": "no_media",
            "observed_start": None,
            "observed_end": None,
            "clock_sources": [],
            "continuity": "unverified",
            "coverage_version": COVERAGE_VERSION,
        }

    clipped: list[tuple[datetime, datetime]] = []
    unknown_reason: str | None = None
    continuities: set[str] = set()
    clock_sources: set[str] = set()

    for row in video:
        continuity = str(_get(row, "continuity", "unverified")).lower()
        quality = str(_get(row, "time_quality", "unknown")).lower()
        clock = str(_get(row, "clock_source", "unknown"))
        continuities.add(continuity)
        clock_sources.add(clock)

        start = _as_utc(_get(row, "capture_start"))
        end = _as_utc(_get(row, "capture_end"))
        if start is None or end is None or end <= start:
            unknown_reason = unknown_reason or "unknown_timing"
            continue
        if quality not in TRUSTED_TIME_QUALITIES:
            unknown_reason = unknown_reason or "untrusted_clock"
            continue
        if clock.lower() not in TRUSTED_CLOCK_SOURCES:
            unknown_reason = unknown_reason or "unknown_clock_source"
            continue
        if continuity in BROKEN_CONTINUITIES:
            unknown_reason = unknown_reason or "continuity_broken"
            continue
        if continuity not in TRUSTED_CONTINUITIES:
            unknown_reason = unknown_reason or "unverified_continuity"
            continue
        clipped.append((start, end))

    req_start = _as_utc(requested_start)
    req_end = _as_utc(requested_end)

    if unknown_reason is not None:
        return {
            "actual_duration": actual_duration,
            "covered_seconds": None,
            "gaps": None,
            "gap_seconds": None,
            "overlap_seconds": None,
            "is_uncertain": True,
            "verification": "unverified",
            "reason": unknown_reason,
            "observed_start": None,
            "observed_end": None,
            "clock_sources": sorted(clock_sources),
            "continuity": _aggregate_continuity(continuities),
            "coverage_version": COVERAGE_VERSION,
        }

    if req_start is None or req_end is None or req_end <= req_start:
        # Without a requested window the best honest statement is "we saw
        # exactly these intervals"; no absence claim beyond them.
        merged_observed = _merge_intervals(clipped)
        observed_start = merged_observed[0][0] if merged_observed else None
        observed_end = merged_observed[-1][1] if merged_observed else None
        return {
            "actual_duration": actual_duration,
            "covered_seconds": round(
                sum(_seconds(s, e) for s, e in merged_observed), 4
            ),
            "gaps": None,
            "gap_seconds": None,
            "overlap_seconds": 0.0,
            "is_uncertain": True,
            "verification": "unverified",
            "reason": "unknown_requested_window",
            "observed_start": format_iso(observed_start),
            "observed_end": format_iso(observed_end),
            "clock_sources": sorted(clock_sources),
            "continuity": _aggregate_continuity(continuities),
            "coverage_version": COVERAGE_VERSION,
        }

    within_window = [
        (max(start, req_start), min(end, req_end))
        for start, end in clipped
        if min(end, req_end) > max(start, req_start)
    ]
    merged = _merge_intervals(within_window)

    covered = round(sum(_seconds(s, e) for s, e in merged), 4)
    requested_length = round(_seconds(req_start, req_end), 4)
    gaps = round(max(0.0, requested_length - covered), 4)
    overlap = round(
        max(0.0, sum(_seconds(s, e) for s, e in within_window) - covered), 4
    )
    continuity = _aggregate_continuity(continuities)

    if gaps > gap_tolerance_seconds:
        verification = "partial"
        reason = "coverage_gap"
    else:
        verification = "verified"
        reason = None

    observed_start = merged[0][0] if merged else None
    observed_end = merged[-1][1] if merged else None

    return {
        "actual_duration": actual_duration,
        "covered_seconds": covered,
        "gaps": gaps,
        "gap_seconds": gaps,
        "overlap_seconds": overlap,
        "is_uncertain": verification != "verified",
        "verification": verification,
        "reason": reason,
        "observed_start": format_iso(observed_start),
        "observed_end": format_iso(observed_end),
        "clock_sources": sorted(clock_sources),
        "continuity": continuity,
        "coverage_version": COVERAGE_VERSION,
    }


def _aggregate_continuity(continuities: set[str]) -> str:
    if not continuities:
        return "unverified"
    if any(value in BROKEN_CONTINUITIES for value in continuities):
        return "broken"
    if all(value in TRUSTED_CONTINUITIES for value in continuities):
        return "verified_segments"
    return "unverified"
