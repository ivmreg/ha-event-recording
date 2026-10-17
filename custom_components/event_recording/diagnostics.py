"""Diagnostics support for Event Recording.

Provides safe diagnostic metrics including slot utilization, queue timeouts,
storage accounting, and quarantined resources with zero paths, credentials, or raw entity names.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant

from .camera_profiles import get_camera_profiles
from .const import (
    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    CONF_CHUNK_SECONDS,
    CONF_ENABLE_AUTO_ASSOCIATION,
    CONF_MAX_INCIDENT_SPAN_SECONDS,
    CONF_MAX_PARALLEL_RESOURCES,
    CONF_MAX_QUEUE_DEPTH,
    CONF_MIN_DISK_FREE_MB,
    CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
    CONF_RETENTION_DAYS,
    CONF_RETENTION_SIZE_MB,
    DATA_COORDINATOR,
    DATA_REGISTRY,
    DATA_STORE,
    DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    DEFAULT_CHUNK_SECONDS,
    DEFAULT_ENABLE_AUTO_ASSOCIATION,
    DEFAULT_MAX_INCIDENT_SPAN_SECONDS,
    DEFAULT_MAX_PARALLEL_RESOURCES,
    DEFAULT_MAX_QUEUE_DEPTH,
    DEFAULT_MIN_DISK_FREE_MB,
    DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_RETENTION_SIZE_MB,
    DOMAIN,
)


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return diagnostics for a config entry with zero secrets, raw paths, or entity IDs."""
    domain_data = hass.data.get(DOMAIN, {})
    coordinator = domain_data.get(DATA_COORDINATOR)
    store = domain_data.get(DATA_STORE)
    registry = domain_data.get(DATA_REGISTRY)

    options = entry.options
    data = entry.data

    def _get_setting(key: str, default: Any) -> Any:
        return options.get(key, data.get(key, default))

    config_diagnostics = {
        CONF_RETENTION_SIZE_MB: _get_setting(CONF_RETENTION_SIZE_MB, DEFAULT_RETENTION_SIZE_MB),
        CONF_RETENTION_DAYS: _get_setting(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
        CONF_CHUNK_SECONDS: _get_setting(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS),
        CONF_MAX_PARALLEL_RESOURCES: _get_setting(
            CONF_MAX_PARALLEL_RESOURCES, DEFAULT_MAX_PARALLEL_RESOURCES
        ),
        CONF_MAX_QUEUE_DEPTH: _get_setting(CONF_MAX_QUEUE_DEPTH, DEFAULT_MAX_QUEUE_DEPTH),
        CONF_QUEUE_WAIT_TIMEOUT_SECONDS: _get_setting(
            CONF_QUEUE_WAIT_TIMEOUT_SECONDS, DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS
        ),
        CONF_MIN_DISK_FREE_MB: _get_setting(CONF_MIN_DISK_FREE_MB, DEFAULT_MIN_DISK_FREE_MB),
        CONF_ENABLE_AUTO_ASSOCIATION: bool(
            _get_setting(CONF_ENABLE_AUTO_ASSOCIATION, DEFAULT_ENABLE_AUTO_ASSOCIATION)
        ),
        CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS: float(
            _get_setting(
                CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
            )
        ),
        CONF_MAX_INCIDENT_SPAN_SECONDS: float(
            _get_setting(
                CONF_MAX_INCIDENT_SPAN_SECONDS, DEFAULT_MAX_INCIDENT_SPAN_SECONDS
            )
        ),
    }

    camera_map: dict[str, str] = {}
    source_map: dict[str, str] = {}
    group_map: dict[str, str] = {}
    zone_map: dict[str, str] = {}

    def _anonymize_camera(entity_id: str) -> str:
        if entity_id not in camera_map:
            camera_map[entity_id] = f"camera_{len(camera_map) + 1}"
        return camera_map[entity_id]

    def _anonymize_source(source_id: str | None) -> str | None:
        if not source_id:
            return None
        if source_id not in source_map:
            source_map[source_id] = f"source_{len(source_map) + 1}"
        return source_map[source_id]

    def _anonymize_group(group: str | None) -> str | None:
        if not group:
            return None
        if group not in group_map:
            group_map[group] = f"group_{len(group_map) + 1}"
        return group_map[group]

    def _anonymize_zone(zone: str | None) -> str | None:
        if not zone:
            return None
        if zone not in zone_map:
            zone_map[zone] = f"zone_{len(zone_map) + 1}"
        return zone_map[zone]

    profiles = get_camera_profiles(entry)
    safe_profiles: dict[str, dict[str, Any]] = {}
    for entity_id, prof in profiles.items():
        anon_cam = _anonymize_camera(entity_id)
        safe_profiles[anon_cam] = {
            "source_id": _anonymize_source(prof.get("source_id")),
            "recording_mode": prof.get("recording_mode", "extend"),
            "ownership_group": _anonymize_group(prof.get("ownership_group")),
            "has_buffered_provider": bool(prof.get("frigate_base_url")),
            "window_seconds": prof.get("window_seconds"),
            "lookback_seconds": prof.get("lookback_seconds"),
            "chunk_seconds": prof.get("chunk_seconds"),
            "max_window_seconds": prof.get("max_window_seconds"),
            "export_delay_seconds": prof.get("export_delay_seconds"),
            "correlation_group": _anonymize_group(prof.get("correlation_group")),
            "zone": _anonymize_zone(prof.get("zone")),
            "adjacent_zones": [
                _anonymize_zone(z)
                for z in (prof.get("adjacent_zones") or [])
                if _anonymize_zone(z) is not None
            ],
            "source_clock_offset_seconds": float(prof.get("source_clock_offset_seconds", 0.0)),
            "source_time_uncertainty_seconds": float(
                prof.get("source_time_uncertainty_seconds", 0.0)
            ),
        }

    slots_info: dict[str, Any] = {}
    counters_info: dict[str, Any] = {}
    if coordinator is not None:
        slot_mgr = getattr(coordinator, "_slot_manager", None)
        if slot_mgr is not None:
            slots_info = {
                "capacity": slot_mgr.capacity,
                "max_queue_depth": getattr(slot_mgr, "max_queue_depth", 32),
                "active_slots": slot_mgr.active_slots,
                "waiting_tasks": slot_mgr.waiting_tasks,
            }
        counters_info = {
            "expired_captures_count": getattr(coordinator, "expired_captures_count", 0),
            "queue_timeouts_count": getattr(coordinator, "queue_timeouts_count", 0),
            "queue_overflow_count": getattr(coordinator, "queue_overflow_count", 0),
            "queue_overflows_count": getattr(coordinator, "queue_overflow_count", 0),
        }

    registry_info: dict[str, Any] = {}
    if registry is not None:
        raw_active = registry.active_resources
        raw_quarantined = registry.quarantined_resources
        registry_info = {
            "active_resources": [
                _anonymize_camera(r) if r.startswith("camera.") else f"resource_{idx+1}"
                for idx, r in enumerate(raw_active)
            ],
            "quarantined_resources": [
                _anonymize_camera(r) if r.startswith("camera.") else f"resource_{idx+1}"
                for idx, r in enumerate(raw_quarantined)
            ],
            "has_unsettled": registry.has_unsettled,
        }

    storage_stats: dict[str, Any] = {}
    if store is not None:
        try:
            storage_stats = await store.get_storage_stats()
            ledger = getattr(store, "ledger", None)
            if ledger is not None:
                storage_stats.update(ledger.stats())
        except Exception:
            storage_stats = {"error": "unavailable"}

    backup_status: dict[str, Any] = {}
    if store is not None and hasattr(store, "async_get_backup_status"):
        try:
            backup_status = await store.async_get_backup_status()
        except Exception:
            backup_status = {"error": "unavailable"}

    return {
        "config": config_diagnostics,
        "camera_profiles": safe_profiles,
        "slots": slots_info,
        "counters": counters_info,
        "registry": registry_info,
        "storage": storage_stats,
        "backup": backup_status,
    }
