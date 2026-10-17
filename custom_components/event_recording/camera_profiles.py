"""Pure helpers for stored per-camera recording defaults."""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any

from .core.validation import (
    SPEC_CHUNK_SECONDS,
    SPEC_EXPORT_DELAY_SECONDS,
    SPEC_FRIGATE_CAMERA_NAME,
    SPEC_GROUP_OR_ZONE,
    SPEC_LABEL,
    SPEC_LOOKBACK_SECONDS,
    SPEC_MAX_WINDOW_SECONDS,
    SPEC_SOURCE_CLOCK_OFFSET_SECONDS,
    SPEC_SOURCE_ID,
    SPEC_SOURCE_TIME_UNCERTAINTY_SECONDS,
    SPEC_TAIL_WINDOW_SECONDS,
    SPEC_WINDOW_SECONDS,
    ValidationError,
)

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

    for field, num_spec in (
        ("window_seconds", SPEC_WINDOW_SECONDS),
        ("lookback_seconds", SPEC_LOOKBACK_SECONDS),
        ("chunk_seconds", SPEC_CHUNK_SECONDS),
        ("max_window_seconds", SPEC_MAX_WINDOW_SECONDS),
    ):
        value = profile.get(field)
        if type(value) is not int:
            errors[field] = f"invalid_{field}"
            continue
        try:
            num_spec.validate(value)
        except ValidationError:
            errors[field] = f"invalid_{field}"

    if "window_seconds" not in errors and "max_window_seconds" not in errors:
        if profile["max_window_seconds"] < profile["window_seconds"]:
            errors["max_window_seconds"] = "max_window_below_window"

    for field, str_spec in (("source_id", SPEC_SOURCE_ID), ("label", SPEC_LABEL)):
        try:
            str_spec.validate(profile.get(field))
        except ValidationError:
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
        try:
            SPEC_FRIGATE_CAMERA_NAME.validate(cam_name)
        except ValidationError:
            errors["frigate_camera_name"] = "invalid_frigate_camera_name"
        export_delay = profile.get("export_delay_seconds", 5)
        if type(export_delay) is not int:
            errors["export_delay_seconds"] = "invalid_export_delay_seconds"
        else:
            try:
                SPEC_EXPORT_DELAY_SECONDS.validate(export_delay)
            except ValidationError:
                errors["export_delay_seconds"] = "invalid_export_delay_seconds"

    for group_field in ("ownership_group", "correlation_group", "zone"):
        raw_grp = profile.get(group_field)
        if raw_grp is not None and raw_grp != "":
            try:
                SPEC_GROUP_OR_ZONE.validate(raw_grp)
            except ValidationError:
                errors[group_field] = f"invalid_{group_field}"

    hold = profile.get("hold")
    if hold is not None and not isinstance(hold, bool):
        errors["hold"] = "invalid_hold"

    adj_zones = profile.get("adjacent_zones")
    if adj_zones is not None:
        if not isinstance(adj_zones, (list, tuple)) or len(adj_zones) > 16:
            errors["adjacent_zones"] = "invalid_adjacent_zones"
        else:
            for az in adj_zones:
                try:
                    SPEC_GROUP_OR_ZONE.validate(az)
                except ValidationError:
                    errors["adjacent_zones"] = "invalid_adjacent_zones"
                    break

    for float_field, float_spec in (
        ("tail_window_seconds", SPEC_TAIL_WINDOW_SECONDS),
        ("source_clock_offset_seconds", SPEC_SOURCE_CLOCK_OFFSET_SECONDS),
        ("source_time_uncertainty_seconds", SPEC_SOURCE_TIME_UNCERTAINTY_SECONDS),
    ):
        f_val = profile.get(float_field)
        if f_val is not None:
            try:
                float_spec.validate(f_val)
            except ValidationError:
                errors[float_field] = f"invalid_{float_field}"

    return errors
