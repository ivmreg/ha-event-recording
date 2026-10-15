"""Pure helpers for stored per-camera recording defaults."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

CAMERA_PROFILES = "camera_profiles"


def default_camera_profile(
    camera_entity: str,
    *,
    name: str | None = None,
    chunk_seconds: int = 10,
    max_window_seconds: int = 300,
) -> dict[str, Any]:
    """Build defaults for a camera entity."""
    object_id = camera_entity.partition(".")[2] if "." in camera_entity else camera_entity
    source_id = re.sub(r"[^a-zA-Z0-9_-]+", "_", object_id).strip("_")[:128] or "camera"
    label = (name or " ".join(part.capitalize() for part in re.split(r"[_-]+", object_id))).strip()
    return {
        "source_id": source_id,
        "label": label[:256] or camera_entity,
        "window_seconds": 15,
        "lookback_seconds": 0,
        "chunk_seconds": chunk_seconds,
        "max_window_seconds": max_window_seconds,
    }


def get_camera_profiles(entry: Any) -> dict[str, dict[str, Any]]:
    """Return a defensive copy of configured camera profiles.

    An explicit empty options value overrides any legacy data value.
    """
    options = getattr(entry, "options", {}) or {}
    data = getattr(entry, "data", {}) or {}
    profiles = options.get(CAMERA_PROFILES, data.get(CAMERA_PROFILES, {}))
    if not isinstance(profiles, Mapping):
        return {}
    return {
        str(entity_id): dict(profile)
        for entity_id, profile in profiles.items()
        if isinstance(entity_id, str) and isinstance(profile, Mapping)
    }


def validate_camera_profile(camera_entity: str, profile: Mapping[str, Any]) -> dict[str, str]:
    """Return field-level translation keys for an invalid profile."""
    errors: dict[str, str] = {}
    if (
        not isinstance(camera_entity, str)
        or not camera_entity.startswith("camera.")
        or not camera_entity.partition(".")[2]
    ):
        errors["camera_entity"] = "invalid_camera_entity"

    for field, maximum, minimum in (
        ("window_seconds", 300, 1),
        ("lookback_seconds", 60, 0),
        ("chunk_seconds", 15, 1),
        ("max_window_seconds", 300, 1),
    ):
        value = profile.get(field)
        if type(value) is not int or not minimum <= value <= maximum:
            errors[field] = f"invalid_{field}"

    if "window_seconds" not in errors and "max_window_seconds" not in errors:
        if profile["max_window_seconds"] < profile["window_seconds"]:
            errors["max_window_seconds"] = "max_window_below_window"

    for field, max_length in (("source_id", 128), ("label", 256)):
        value = profile.get(field)
        if not isinstance(value, str) or not value.strip() or len(value) > max_length:
            errors[field] = f"invalid_{field}"

    return errors
