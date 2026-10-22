"""Conservative negative filtering; plan completion never proves whole-video absence."""

from __future__ import annotations

import json
from typing import Any


def _count(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and 0 <= value <= 2**53 - 1
        and value == int(value)
    )


def can_hide_no_activity(event: dict[str, Any]) -> bool:
    """Only fully accounted JPEG observations qualify under the current policy.

    Unknown/legacy metadata and video negatives remain visible. This qualifies
    observation of recorded stills, not absence outside those images or truth
    of the model's interpretation. PNG/WebP can be animated; MIME alone cannot
    certify that a single extracted frame observed those complete inputs.
    """
    annotations = event.get("annotations")
    if not isinstance(annotations, dict):
        return False
    annotation = annotations.get("event_vision")
    if not isinstance(annotation, dict):
        return False
    revision = event.get("media_revision")
    annotation_revision = annotation.get(
        "media_revision", annotation.get("expected_media_revision")
    )
    if (
        not _count(revision)
        or revision == 0
        or annotation_revision != revision
        or not _count(annotation_revision)
    ):
        return False
    if annotation.get("status") != "complete" or annotation.get("activity") is not False:
        return False
    if annotation.get("error_code") not in (None, ""):
        return False
    if event.get("capture_state") != "ready" or event.get("capture_error") not in (None, ""):
        return False
    capture = event.get("coverage")
    if not isinstance(capture, dict) or capture.get("is_uncertain") is not False:
        return False
    if not _count(capture.get("gaps")) or capture["gaps"] != 0:
        return False
    if "uncertainty" in capture and capture["uncertainty"] is not False:
        return False
    if "gap_seconds" in capture and (
        not _count(capture["gap_seconds"]) or capture["gap_seconds"] != 0
    ):
        return False
    media = event.get("media")
    if not isinstance(media, list) or not media:
        return False
    ids = []
    for item in media:
        if not isinstance(item, dict) or item.get("available") is not True:
            return False
        if item.get("kind") != "image" or item.get("mime_type") != "image/jpeg":
            return False
        identity = item.get("id")
        if not isinstance(identity, str) or not identity:
            return False
        ids.append(identity)
    if len(set(ids)) != len(ids):
        return False
    coverage = annotation.get("coverage")
    if not isinstance(coverage, dict):
        return False
    if (
        coverage.get("observation_state") != "complete"
        or coverage.get("is_partial") is not False
        or coverage.get("source_capture_state") != "ready"
    ):
        return False
    expected = len(ids)
    for key in (
        "planned_frames",
        "extracted_frames",
        "planned_clips",
        "sampled_clips",
        "available_clips",
    ):
        if not _count(coverage.get(key)) or coverage[key] != expected:
            return False
    for key in ("missing_clips", "unselected_clips"):
        if not _count(coverage.get(key)) or coverage[key] != 0:
            return False
    frames = annotation.get("frames")
    if not isinstance(frames, list) or len(frames) != expected:
        return False
    observed = []
    for frame in frames:
        if not isinstance(frame, dict) or not _count(frame.get("offset")) or frame["offset"] != 0:
            return False
        observed.append(frame.get("media_id"))
    return all(isinstance(identity, str) for identity in observed) and set(observed) == set(ids)


def negative_observation_sql(state, error, capture_json, revision, payload_json, media_json) -> int:
    """SQLite WHERE adapter; status/revision freshness are also fenced in SQL."""
    try:
        payload = json.loads(payload_json)
        if not isinstance(payload, dict):
            return 0
        return int(
            can_hide_no_activity(
                {
                    "capture_state": state,
                    "capture_error": error,
                    "coverage": json.loads(capture_json),
                    "media_revision": revision,
                    "media": json.loads(media_json),
                    "annotations": {
                        "event_vision": {
                            **payload,
                            "status": "complete",
                            "media_revision": revision,
                        }
                    },
                }
            )
        )
    except (ValueError, TypeError, OverflowError):
        return 0
