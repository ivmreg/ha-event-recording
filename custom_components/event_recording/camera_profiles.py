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
        "recording_mode": "extend",
        "ownership_group": None,
        "hold": False,
        "tail_window_seconds": 0.0,
        "correlation_group": None,
        "zone": None,
        "adjacent_zones": [],
        "source_clock_offset_seconds": 0.0,
        "source_time_uncertainty_seconds": 0.0,
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

    recording_mode = profile.get("recording_mode", "extend")
    if recording_mode not in ("extend", "native_fixed", "native_snapshot", "buffered"):
        errors["recording_mode"] = "invalid_recording_mode"

    if recording_mode == "buffered":
        base_url = profile.get("frigate_base_url")
        cam_name = profile.get("frigate_camera_name")
        if (
            not isinstance(base_url, str)
            or not (base_url.startswith("http://") or base_url.startswith("https://"))
        ):
            errors["frigate_base_url"] = "invalid_frigate_base_url"
        if not isinstance(cam_name, str) or not re.match(r"^[a-zA-Z0-9_-]+$", cam_name):
            errors["frigate_camera_name"] = "invalid_frigate_camera_name"
        export_delay = profile.get("export_delay_seconds", 5)
        if type(export_delay) is not int or not 0 <= export_delay <= 60:
            errors["export_delay_seconds"] = "invalid_export_delay_seconds"

    ownership_group = profile.get("ownership_group")
    if ownership_group is not None and ownership_group != "":
        if (
            not isinstance(ownership_group, str)
            or len(ownership_group) > 64
            or any(ord(c) < 32 or ord(c) == 127 for c in ownership_group)
        ):
            errors["ownership_group"] = "invalid_ownership_group"

    hold = profile.get("hold")
    if hold is not None and not isinstance(hold, bool):
        errors["hold"] = "invalid_hold"

    tail = profile.get("tail_window_seconds")
    if tail is not None:
        if type(tail) not in (int, float) or isinstance(tail, bool) or not (0.0 <= tail <= 300.0):
            errors["tail_window_seconds"] = "invalid_tail_window_seconds"

    corr_group = profile.get("correlation_group")
    if corr_group is not None and corr_group != "":
        if (
            not isinstance(corr_group, str)
            or len(corr_group) > 64
            or any(ord(c) < 32 or ord(c) == 127 for c in corr_group)
        ):
            errors["correlation_group"] = "invalid_correlation_group"

    zone = profile.get("zone")
    if zone is not None and zone != "":
        if (
            not isinstance(zone, str)
            or len(zone) > 64
            or any(ord(c) < 32 or ord(c) == 127 for c in zone)
        ):
            errors["zone"] = "invalid_zone"

    adj_zones = profile.get("adjacent_zones")
    if adj_zones is not None:
        if not isinstance(adj_zones, (list, tuple)) or len(adj_zones) > 16:
            errors["adjacent_zones"] = "invalid_adjacent_zones"
        else:
            for az in adj_zones:
                if (
                    not isinstance(az, str)
                    or len(az) > 64
                    or any(ord(c) < 32 or ord(c) == 127 for c in az)
                ):
                    errors["adjacent_zones"] = "invalid_adjacent_zones"
                    break

    offset = profile.get("source_clock_offset_seconds")
    if offset is not None:
        if type(offset) not in (int, float) or isinstance(offset, bool) or not (-3600.0 <= offset <= 3600.0):
            errors["source_clock_offset_seconds"] = "invalid_source_clock_offset_seconds"

    uncertainty = profile.get("source_time_uncertainty_seconds")
    if uncertainty is not None:
        if type(uncertainty) not in (int, float) or isinstance(uncertainty, bool) or not (0.0 <= uncertainty <= 300.0):
            errors["source_time_uncertainty_seconds"] = "invalid_source_time_uncertainty_seconds"

    return errors
