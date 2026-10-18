"""Service handlers for the Event Recording integration."""

from __future__ import annotations

import logging
import uuid
from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol

from homeassistant.core import HomeAssistant, ServiceCall, SupportsResponse, callback
from homeassistant.exceptions import HomeAssistantError, Unauthorized
from homeassistant.helpers import config_validation as cv

from .camera_profiles import get_camera_profiles
from .const import (
    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    CONF_ENABLE_AUTO_ASSOCIATION,
    CONF_MAX_INCIDENT_SPAN_SECONDS,
    DATA_COORDINATOR,
    DATA_STORE,
    DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    DEFAULT_ENABLE_AUTO_ASSOCIATION,
    DEFAULT_MAX_INCIDENT_SPAN_SECONDS,
    DOMAIN,
    SERVICE_ATTACH_MEDIA,
    SERVICE_FINALIZE,
    SERVICE_LOG_EVENT,
    SERVICE_RECORD,
    SERVICE_RECORD_EVENTS,
)
from .core.models import utc_now, validate_correlation_id
from .core.store import JournalStore

_LOGGER = logging.getLogger(__name__)


def _build_auto_association_config(
    coordinator: Any | None,
    profile_defaults: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """Build auto_association_config combining global integration options and camera profile."""
    if not coordinator or not getattr(coordinator, "entry", None):
        return None
    entry = coordinator.entry
    options = getattr(entry, "options", {}) or {}
    data = getattr(entry, "data", {}) or {}

    enabled = bool(
        options.get(
            CONF_ENABLE_AUTO_ASSOCIATION,
            data.get(CONF_ENABLE_AUTO_ASSOCIATION, DEFAULT_ENABLE_AUTO_ASSOCIATION),
        )
    )
    if not enabled:
        return {"enabled": False}

    import math

    raw_tol = options.get(
        CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
        data.get(
            CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
            DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
        ),
    )
    raw_span = options.get(
        CONF_MAX_INCIDENT_SPAN_SECONDS,
        data.get(
            CONF_MAX_INCIDENT_SPAN_SECONDS,
            DEFAULT_MAX_INCIDENT_SPAN_SECONDS,
        ),
    )
    tolerance = (
        float(raw_tol)
        if isinstance(raw_tol, (int, float)) and math.isfinite(raw_tol)
        else DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS
    )
    max_span = (
        float(raw_span)
        if isinstance(raw_span, (int, float)) and math.isfinite(raw_span)
        else DEFAULT_MAX_INCIDENT_SPAN_SECONDS
    )

    config: dict[str, Any] = {
        "enabled": True,
        "global_tolerance_seconds": max(1.0, min(60.0, tolerance)),
        "max_incident_span_seconds": max(10.0, min(3600.0, max_span)),
    }

    if profile_defaults:
        if profile_defaults.get("correlation_group"):
            config["correlation_group"] = str(profile_defaults["correlation_group"])
        if profile_defaults.get("zone"):
            config["zone"] = str(profile_defaults["zone"])
        if profile_defaults.get("adjacent_zones"):
            config["adjacent_zones"] = list(profile_defaults["adjacent_zones"])
        if "source_clock_offset_seconds" in profile_defaults:
            try:
                config["source_clock_offset_seconds"] = float(
                    profile_defaults["source_clock_offset_seconds"]
                )
            except (TypeError, ValueError):
                pass
        if "source_time_uncertainty_seconds" in profile_defaults:
            try:
                config["source_time_uncertainty_seconds"] = float(
                    profile_defaults["source_time_uncertainty_seconds"]
                )
            except (TypeError, ValueError):
                pass

    return config

SCHEMA_LOG_EVENT = vol.Schema(
    {
        vol.Required("source_id"): cv.string,
        vol.Required("kind"): cv.string,
        vol.Optional("occurred_at"): cv.string,
        vol.Optional("idempotency_key"): cv.string,
        vol.Optional("correlation_id"): cv.string,
        vol.Optional("label"): vol.Any(cv.string, None),
        vol.Optional("payload"): vol.Any(dict, None),
        vol.Optional("access_entities"): vol.Any(cv.ensure_list, None),
    }
)

SCHEMA_RECORD = vol.Schema(
    {
        vol.Optional("source_id"): cv.string,
        vol.Required("kind"): cv.string,
        vol.Optional("resource_key"): cv.string,
        vol.Optional("camera_entity"): cv.entity_id,
        vol.Required("operation"): vol.In(["start_or_extend", "end_after"]),
        vol.Optional("window_seconds"): vol.All(vol.Coerce(float), vol.Range(min=1.0, max=300.0)),
        vol.Optional("occurred_at"): cv.string,
        vol.Optional("idempotency_key"): cv.string,
        vol.Optional("correlation_id"): cv.string,
        vol.Optional("lookback_seconds"): vol.All(
            vol.Coerce(float), vol.Range(min=0.0, max=60.0)
        ),
        vol.Optional("hold"): cv.boolean,
        vol.Optional("tail_window_seconds"): vol.All(
            vol.Coerce(float), vol.Range(min=0.0, max=300.0)
        ),
        vol.Optional("recorder_config"): vol.Any(dict, None),
        vol.Optional("label"): vol.Any(cv.string, None),
        vol.Optional("payload"): vol.Any(dict, None),
        vol.Optional("access_entities"): vol.Any(cv.ensure_list, None),
    }
)

SCHEMA_RECORD_EVENTS = vol.Schema(
    {
        vol.Required("events"): vol.All(cv.ensure_list, vol.Length(min=1, max=16)),
        vol.Optional("correlation_id"): cv.string,
        vol.Optional("occurred_at"): cv.string,
        vol.Optional("kind"): cv.string,
        vol.Optional("label"): vol.Any(cv.string, None),
        vol.Optional("payload"): vol.Any(dict, None),
    }
)

SCHEMA_ATTACH_MEDIA = vol.Schema(
    {
        vol.Required("event_id"): cv.string,
        vol.Required("media_locator"): cv.string,
        vol.Optional("kind"): vol.Any(vol.In(["video", "image"]), None),
        vol.Optional("mime_type"): vol.Any(cv.string, None),
        vol.Optional("finalize", default=True): cv.boolean,
    }
)

SCHEMA_FINALIZE = vol.Schema(
    {
        vol.Required("event_id"): cv.string,
    }
)


async def _check_human_permission(
    hass: HomeAssistant,
    call: ServiceCall,
    entities: list[str] | tuple[str, ...],
    permission_type: str = "control",
) -> None:
    """Validate permissions asynchronously when called by a human user."""
    user_id = call.context.user_id
    if user_id is None:
        # System automation context -> allowed
        return

    user = await hass.auth.async_get_user(user_id)
    if not user or not getattr(user, "is_active", True):
        raise Unauthorized(context=call.context)

    if user.is_admin:
        return

    if not entities:
        raise Unauthorized(context=call.context)

    for ent in entities:
        if not user.permissions.check_entity(ent, permission_type):
            raise Unauthorized(context=call.context)


@callback
def async_register_services(hass: HomeAssistant) -> None:
    """Register Event Recording services once."""
    if hass.services.has_service(DOMAIN, SERVICE_LOG_EVENT):
        return

    async def handle_log_event(call: ServiceCall) -> dict[str, Any]:
        domain_data = hass.data.get(DOMAIN, {})
        store: JournalStore | None = domain_data.get(DATA_STORE)
        if not store:
            raise HomeAssistantError("Event Recording integration is not initialized")

        data = call.data
        source_id = data["source_id"]
        kind = data["kind"]
        occurred_at = data.get("occurred_at") or utc_now()
        idempotency_key = data.get("idempotency_key") or uuid.uuid4().hex
        correlation_id = data.get("correlation_id")
        label = data.get("label")
        payload = data.get("payload") or {}
        access_entities = tuple(data.get("access_entities") or ())

        _LOGGER.debug(
            "service event_recording.log_event called for source %s (kind=%s, label=%s)",
            source_id,
            kind,
            label,
        )

        # Security check for human callers on explicit access entities
        if access_entities and call.context.user_id is not None:
            await _check_human_permission(hass, call, access_entities, "control")

        coordinator = domain_data.get(DATA_COORDINATOR)
        profile_defaults = None
        if coordinator:
            profiles = get_camera_profiles(coordinator.entry)
            profile_defaults = profiles.get(source_id)

        auto_association_config = _build_auto_association_config(coordinator, profile_defaults)

        event, inserted = await store.log_event(
            source_id=source_id,
            kind=kind,
            occurred_at=occurred_at,
            idempotency_key=idempotency_key,
            label=label,
            payload=payload,
            access_entities=access_entities,
            owner_user_id=call.context.user_id,
            correlation_id=correlation_id,
            auto_association_config=auto_association_config,
        )

        return {"event_id": event.event_id, "inserted": inserted}

    async def _admit_record_payload(
        data: dict[str, Any], context: Any
    ) -> dict[str, Any]:
        domain_data = hass.data.get(DOMAIN, {})
        store: JournalStore | None = domain_data.get(DATA_STORE)
        coordinator = domain_data.get(DATA_COORDINATOR)
        if not store:
            raise HomeAssistantError("Event Recording integration is not initialized")

        source_id = data.get("source_id")
        kind = data.get("kind")
        if not kind:
            raise HomeAssistantError("kind is required for recording")
        resource_key = data.get("resource_key")
        shortcut_camera = data.get("camera_entity")
        if shortcut_camera:
            if not shortcut_camera.startswith("camera."):
                raise HomeAssistantError("camera_entity must identify a Home Assistant camera")
            if resource_key and resource_key != shortcut_camera:
                raise HomeAssistantError(
                    "resource_key must match camera_entity when using a camera profile"
                )
            resource_key = shortcut_camera
        if not resource_key:
            raise HomeAssistantError("resource_key is required unless camera_entity is provided")
        operation = data.get("operation", "start_or_extend")
        window_seconds = data.get("window_seconds")
        occurred_at = data.get("occurred_at") or utc_now()
        idempotency_key = data.get("idempotency_key") or uuid.uuid4().hex
        correlation_id = data.get("correlation_id")
        lookback_seconds = data.get("lookback_seconds")

        _LOGGER.debug(
            "service event_recording.record called: resource_key=%s, op=%s, window=%s, lookback=%s",
            resource_key,
            operation,
            window_seconds,
            lookback_seconds,
        )
        action_hold: bool | None = data.get("hold") if "hold" in data else None
        action_tail: float | None = (
            float(data["tail_window_seconds"]) if "tail_window_seconds" in data else None
        )
        explicit_recorder_config = dict(data.get("recorder_config") or {})
        # This key is reserved for the integration's durable per-session snapshot.
        explicit_recorder_config.pop("_event_recording_profile", None)
        recorder_config = dict(explicit_recorder_config)
        label = data.get("label")
        payload = data.get("payload") or {}
        raw_access = list(data.get("access_entities") or ())
        profile_defaults: dict[str, Any] | None = None

        # Check adapter configuration
        is_camera = (
            recorder_config.get("adapter") == "camera"
            or "camera_entity" in recorder_config
            or resource_key.startswith("camera.")
        )
        if is_camera:
            camera_entity = resource_key
            profiles = get_camera_profiles(coordinator.entry) if coordinator else {}
            profile = profiles.get(camera_entity)
            if shortcut_camera and profile is None:
                raise HomeAssistantError("No Event Recording profile is configured for this camera")
            if profile is not None:
                profile_defaults = dict(profile)
                if source_id is None:
                    source_id = profile_defaults.get("source_id")
                if label is None and "label" not in data:
                    label = profile_defaults.get("label")
                if window_seconds is None:
                    window_seconds = profile_defaults.get("window_seconds", 15)
                if lookback_seconds is None:
                    lookback_seconds = profile_defaults.get("lookback_seconds", 0)
            recorder_config["adapter"] = "camera"
            cam_ent = recorder_config.get("camera_entity")
            if cam_ent and cam_ent != resource_key:
                raise HomeAssistantError(
                    f"resource_key '{resource_key}' must match camera_entity '{cam_ent}' for camera adapter"
                )
            recorder_config["camera_entity"] = resource_key

            # Always protect camera recordings with the canonical camera entity ACL.
            if resource_key not in raw_access:
                raw_access.append(resource_key)

            if context.user_id is not None:
                user = await hass.auth.async_get_user(context.user_id)
                if not user or not getattr(user, "is_active", True):
                    raise Unauthorized(context=context)
                if not user.is_admin:
                    if not user.permissions.check_entity(resource_key, "control"):
                        raise Unauthorized(context=context)

        if action_hold is not None:
            hold = bool(action_hold)
        elif (
            profile_defaults is not None
            and "hold" in profile_defaults
            and profile_defaults["hold"] is not None
        ):
            hold = bool(profile_defaults["hold"])
        else:
            hold = False

        if action_tail is not None:
            tail_window_seconds = float(action_tail)
        elif (
            profile_defaults is not None
            and "tail_window_seconds" in profile_defaults
            and profile_defaults["tail_window_seconds"] is not None
        ):
            tail_window_seconds = float(profile_defaults["tail_window_seconds"])
        else:
            tail_window_seconds = 0.0

        if source_id is None:
            raise HomeAssistantError(
                "source_id is required unless a configured camera profile provides it"
            )
        if window_seconds is None:
            raise HomeAssistantError(
                "window_seconds is required unless a configured camera profile provides it"
            )
        window_seconds = float(window_seconds)
        lookback_seconds = float(lookback_seconds if lookback_seconds is not None else 0.0)

        access_entities = tuple(raw_access)
        if access_entities and context.user_id is not None:
            user = await hass.auth.async_get_user(context.user_id)
            if not user or not getattr(user, "is_active", True):
                raise Unauthorized(context=context)
            if not user.is_admin:
                for ent in access_entities:
                    if not user.permissions.check_entity(ent, "control"):
                        raise Unauthorized(context=context)

        admit = coordinator.admit_record_intent if coordinator else store.admit_record_intent
        extra_admission: dict[str, Any] = {}
        if coordinator:
            extra_admission = {
                "_profile_defaults": profile_defaults,
                "_explicit_fields": frozenset(data),
                "_explicit_recorder_config": explicit_recorder_config,
            }
        auto_association_config = _build_auto_association_config(coordinator, profile_defaults)
        event, inserted, intent_id = await admit(
            source_id=source_id,
            kind=kind,
            resource_key=resource_key,
            operation=operation,
            window_seconds=window_seconds,
            occurred_at=occurred_at,
            idempotency_key=idempotency_key,
            lookback_seconds=lookback_seconds,
            hold=hold,
            recorder_config=recorder_config,
            label=label,
            payload=payload,
            access_entities=access_entities,
            owner_user_id=context.user_id,
            tail_window_seconds=tail_window_seconds,
            correlation_id=correlation_id,
            auto_association_config=auto_association_config,
            **extra_admission,
        )

        if coordinator:
            coordinator.notify_new_intent()

        return {
            "event_id": event.event_id,
            "inserted": inserted,
            "intent_id": intent_id,
        }

    async def handle_record(call: ServiceCall) -> dict[str, Any]:
        return await _admit_record_payload(call.data, call.context)

    async def handle_record_events(call: ServiceCall) -> dict[str, Any]:
        data = call.data
        shared_correlation_id = data.get("correlation_id") or f"corr_{uuid.uuid4().hex[:12]}"
        validate_correlation_id(shared_correlation_id)
        _LOGGER.debug(
            "service event_recording.record_events called: %d events, correlation_id=%s",
            len(data.get("events", [])),
            shared_correlation_id,
        )

        top_occurred_at = data.get("occurred_at")
        top_kind = data.get("kind")
        top_label = data.get("label")
        top_payload = data.get("payload")

        results: list[dict[str, Any]] = []
        for raw_item in data["events"]:
            item_data = dict(raw_item)
            if "kind" not in item_data and top_kind:
                item_data["kind"] = top_kind
            if "occurred_at" not in item_data and top_occurred_at:
                item_data["occurred_at"] = top_occurred_at
            if "label" not in item_data and top_label:
                item_data["label"] = top_label
            if "payload" not in item_data and top_payload:
                item_data["payload"] = dict(top_payload)

            item_data["correlation_id"] = shared_correlation_id
            if "idempotency_key" not in item_data:
                item_data["idempotency_key"] = uuid.uuid4().hex

            try:
                res = await _admit_record_payload(item_data, call.context)
                results.append(
                    {
                        "event_id": res["event_id"],
                        "inserted": res["inserted"],
                        "intent_id": res["intent_id"],
                        "resource_key": item_data.get("resource_key") or item_data.get("camera_entity"),
                        "status": "ok",
                    }
                )
            except Exception as err:
                _LOGGER.warning("record_events item failed: %s", err)
                results.append(
                    {
                        "resource_key": item_data.get("resource_key") or item_data.get("camera_entity"),
                        "status": "error",
                        "error": str(err),
                    }
                )

        return {
            "correlation_id": shared_correlation_id,
            "results": results,
        }

    async def handle_attach_media(call: ServiceCall) -> dict[str, Any]:
        domain_data = hass.data.get(DOMAIN, {})
        store: JournalStore | None = domain_data.get(DATA_STORE)
        if not store:
            raise HomeAssistantError("Event Recording integration is not initialized")

        # Imported filesystem media attach should be ADMIN/SYSTEM only
        if call.context.user_id is not None:
            user = await hass.auth.async_get_user(call.context.user_id)
            if not user or not getattr(user, "is_active", True) or not user.is_admin:
                raise Unauthorized(context=call.context)

        data = call.data
        event_id = data["event_id"]
        media_locator = data["media_locator"]
        kind = data.get("kind")
        mime_type = data.get("mime_type")
        finalize = bool(data.get("finalize", True))

        _LOGGER.debug(
            "service event_recording.attach_media called for event %s: kind=%s, mime=%s, finalize=%s",
            event_id,
            kind,
            mime_type,
            finalize,
        )

        event = await store.get_event(event_id)
        if not event:
            raise HomeAssistantError(f"Event '{event_id}' not found")

        event, media_item = await store.attach_and_promote_media(
            event_id=event_id,
            src_path=media_locator,
            kind=kind,
            mime_type=mime_type,
            finalize=finalize,
        )

        return {"event_id": event.event_id, "media_id": media_item.id}

    async def handle_finalize(call: ServiceCall) -> dict[str, Any]:
        domain_data = hass.data.get(DOMAIN, {})
        store: JournalStore | None = domain_data.get(DATA_STORE)
        if not store:
            raise HomeAssistantError("Event Recording integration is not initialized")

        event_id = call.data["event_id"]
        _LOGGER.debug("service event_recording.finalize called for event %s", event_id)
        event = await store.get_event(event_id)
        if not event:
            raise HomeAssistantError(f"Event '{event_id}' not found")

        # Enforce active valid user and event owner/Admin OR explicit ALL entities control
        if call.context.user_id is not None:
            user = await hass.auth.async_get_user(call.context.user_id)
            if not user or not getattr(user, "is_active", True):
                raise Unauthorized(context=call.context)
            if not user.is_admin:
                if event.owner_user_id and event.owner_user_id == user.id:
                    pass
                elif event.access_entities:
                    await _check_human_permission(hass, call, event.access_entities, "control")
                else:
                    raise Unauthorized(context=call.context)

        event = await store.finalize_event(event_id)
        return {"event_id": event.event_id, "capture_state": event.capture_state}

    hass.services.async_register(
        DOMAIN,
        SERVICE_LOG_EVENT,
        handle_log_event,
        schema=SCHEMA_LOG_EVENT,
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_RECORD,
        handle_record,
        schema=SCHEMA_RECORD,
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_RECORD_EVENTS,
        handle_record_events,
        schema=SCHEMA_RECORD_EVENTS,
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_ATTACH_MEDIA,
        handle_attach_media,
        schema=SCHEMA_ATTACH_MEDIA,
        supports_response=SupportsResponse.OPTIONAL,
    )
    hass.services.async_register(
        DOMAIN,
        SERVICE_FINALIZE,
        handle_finalize,
        schema=SCHEMA_FINALIZE,
        supports_response=SupportsResponse.OPTIONAL,
    )
