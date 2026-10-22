"""WebSocket API endpoints for Event Recording."""

from __future__ import annotations

import logging
import math
import re
from datetime import timedelta
from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol

from homeassistant.components import websocket_api
from homeassistant.components.http.auth import async_sign_path
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import config_validation as cv

from .api import get_api
from .const import (
    DATA_STORE,
    DOMAIN,
    WS_TYPE_EVENT,
    WS_TYPE_EVENTS,
    WS_TYPE_INCIDENT,
    WS_TYPE_LIST_ACTIVITY,
    WS_TYPE_MEDIA,
    WS_TYPE_PROCESS,
    WS_TYPE_SUBSCRIBE,
)
from .core.models import RecordedEvent, format_iso, to_public_dto, to_public_incident_dto, utc_now
from .core.notifications import RevisionDispatcher
from .core.store import JournalStore
from .core.visibility import can_hide_no_activity

_LOGGER = logging.getLogger(__name__)


def is_user_authorized_for_event(user: Any, event: RecordedEvent) -> bool:
    """Check whether a user has read access to an event based on ACLs and access entities."""
    if not user or not getattr(user, "is_active", True):
        return False
    if getattr(user, "is_admin", False):
        return True
    if event.access_entities:
        permissions = getattr(user, "permissions", None)
        if not permissions:
            return False
        return all(permissions.check_entity(ent, "read") for ent in event.access_entities)
    if event.owner_user_id and event.owner_user_id == getattr(user, "id", None):
        return True
    return False


def make_event_cursor(event: RecordedEvent) -> str:
    """Format keyset cursor matching JournalStore descending order."""
    return f"{format_iso(event.start_at)}|{event.event_id}"


def _validate_annotation_exclusion(value: Any) -> dict[str, Any]:
    """Validate a generic top-level annotation field equality filter."""
    if not isinstance(value, dict) or set(value) != {"namespace", "field", "equals"}:
        raise vol.Invalid("Expected namespace, field, and equals")

    namespace = value["namespace"]
    field = value["field"]
    expected = value["equals"]
    if not isinstance(namespace, str) or not namespace.strip() or len(namespace) > 128:
        raise vol.Invalid("namespace must be a non-empty string of at most 128 characters")
    if not isinstance(field, str) or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,63}", field):
        raise vol.Invalid("field must be a top-level identifier of at most 64 characters")
    if expected is not None and type(expected) not in (bool, str, int, float):
        raise vol.Invalid("equals must be a JSON scalar")
    if type(expected) is float and not math.isfinite(expected):
        raise vol.Invalid("equals must be finite")
    return value


@callback
def async_register_websocket_api(hass: HomeAssistant) -> None:
    """Register Event Recording WebSocket commands."""
    websocket_api.async_register_command(hass, ws_handle_events)
    websocket_api.async_register_command(hass, ws_handle_event)
    websocket_api.async_register_command(hass, ws_handle_subscribe)
    websocket_api.async_register_command(hass, ws_handle_media)
    websocket_api.async_register_command(hass, ws_handle_process)
    websocket_api.async_register_command(hass, ws_handle_incident)
    websocket_api.async_register_command(hass, ws_handle_list_activity)


# -----------------------------------------------------------------------------
# 1. event_recording/events
# -----------------------------------------------------------------------------
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_TYPE_EVENTS,
        vol.Optional("source_id"): str,
        vol.Optional("kind"): str,
        vol.Optional("exclude_current_complete_annotation"): _validate_annotation_exclusion,
        vol.Optional("exclude_journal_only"): bool,
        vol.Optional("limit", default=50): vol.All(vol.Coerce(int), vol.Range(min=1, max=100)),
        vol.Optional("before_cursor"): str,
        vol.Optional("cursor"): str,
    }
)
@websocket_api.async_response
async def ws_handle_events(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Handle listing events with keyset pagination and server-side authorization filtering."""
    if (
        not connection.user
        or connection.user.system_generated
        or not getattr(connection.user, "is_active", True)
    ):
        connection.send_error(msg["id"], "unauthorized", "Active user authentication required")
        return

    domain_data = hass.data.get(DOMAIN, {})
    store: JournalStore | None = domain_data.get(DATA_STORE)
    if not store:
        connection.send_error(msg["id"], "not_found", "Event Recording is not initialized")
        return

    api = get_api(hass)
    limit = max(1, min(int(msg.get("limit", 50)), 100))
    cursor = msg.get("before_cursor") or msg.get("cursor")
    _LOGGER.debug(
        "WS command event_recording/events: limit=%d, cursor=%s, filters=%s",
        limit,
        cursor,
        {
            k: v
            for k, v in msg.items()
            if k not in ("type", "id", "limit", "cursor", "before_cursor")
        },
    )

    filters: dict[str, Any] = {}
    if "source_id" in msg:
        filters["source_id"] = msg["source_id"]
    if "kind" in msg:
        filters["kind"] = msg["kind"]
    if "exclude_current_complete_annotation" in msg:
        filters["exclude_current_complete_annotation"] = msg["exclude_current_complete_annotation"]
    if msg.get("exclude_journal_only") is True:
        filters["exclude_journal_only"] = True

    authorized_events: list[RecordedEvent] = []
    current_cursor = cursor
    next_cursor: str | None = None
    max_scan_batches = 5
    batch_size = max(limit * 2, 20)

    for _ in range(max_scan_batches):
        batch, batch_next_cur = await store.list_events(
            filters=filters,
            limit=batch_size,
            cursor=current_cursor,
        )
        if not batch:
            next_cursor = None
            break

        stopped_early = False
        for ev in batch:
            if is_user_authorized_for_event(connection.user, ev):
                authorized_events.append(ev)
                if len(authorized_events) >= limit:
                    # Cursor must be the last SCANNED record actually consumed
                    if ev is batch[-1] and batch_next_cur is None:
                        next_cursor = None
                    else:
                        next_cursor = make_event_cursor(ev)
                    stopped_early = True
                    break

        if stopped_early:
            break

        # Consumed full batch without reaching limit
        if batch_next_cur is not None:
            current_cursor = batch_next_cur
            next_cursor = batch_next_cur
        else:
            next_cursor = None
            break

    consumers = api.get_registered_consumers() if api else []

    connection.send_result(
        msg["id"],
        {
            "events": [to_public_dto(ev) for ev in authorized_events[:limit]],
            "next_cursor": next_cursor,
            "capabilities": {
                "consumers": consumers,
            },
        },
    )


# -----------------------------------------------------------------------------
# 2. event_recording/event
# -----------------------------------------------------------------------------
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_TYPE_EVENT,
        vol.Required("event_id"): str,
    }
)
@websocket_api.async_response
async def ws_handle_event(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Handle retrieving a single event by event_id."""
    if (
        not connection.user
        or connection.user.system_generated
        or not getattr(connection.user, "is_active", True)
    ):
        connection.send_error(msg["id"], "unauthorized", "Active user authentication required")
        return

    domain_data = hass.data.get(DOMAIN, {})
    store: JournalStore | None = domain_data.get(DATA_STORE)
    if not store:
        connection.send_error(msg["id"], "not_found", "Event Recording is not initialized")
        return

    event_id = msg["event_id"]
    _LOGGER.debug("WS command event_recording/event query for event_id=%s", event_id)
    event = await store.get_event(event_id)
    if not event:
        connection.send_error(msg["id"], "not_found", f"Event '{event_id}' not found")
        return

    if not is_user_authorized_for_event(connection.user, event):
        connection.send_error(msg["id"], "unauthorized", "Access denied")
        return

    # Check incident membership: authorize EVERY member before summary/metadata
    event_incidents = await store.list_event_incidents(event_id)
    authorized_incidents: list[dict[str, Any]] = []
    has_unauthorized_member = False
    for inc in event_incidents:
        members = await store.get_incident_events(inc.incident_id)
        if all(is_user_authorized_for_event(connection.user, m) for m in members):
            authorized_incidents.append(to_public_incident_dto(inc))
        else:
            has_unauthorized_member = True

    event_dto = to_public_dto(
        event,
        omit_correlation=has_unauthorized_member and not authorized_incidents,
    )
    if authorized_incidents:
        event_dto["incidents"] = authorized_incidents

    connection.send_result(msg["id"], {"event": event_dto})


# -----------------------------------------------------------------------------
# 3. event_recording/subscribe
# -----------------------------------------------------------------------------
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_TYPE_SUBSCRIBE,
    }
)
@websocket_api.async_response
async def ws_handle_subscribe(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Handle event revision subscriptions with continuous per-event permission checks."""
    if (
        not connection.user
        or connection.user.system_generated
        or not getattr(connection.user, "is_active", True)
    ):
        connection.send_error(msg["id"], "unauthorized", "Active user authentication required")
        return

    domain_data = hass.data.get(DOMAIN, {})
    store: JournalStore | None = domain_data.get(DATA_STORE)
    if not store or not store.is_open:
        connection.send_error(msg["id"], "not_found", "Event Recording is not initialized")
        return

    async def _on_event_update(event_id: str, revision: int) -> None:
        # Safe check against stale/closed store or unloaded integration
        current_store: JournalStore | None = hass.data.get(DOMAIN, {}).get(DATA_STORE)
        if not current_store or not current_store.is_open:
            return

        # Revalidate live user status from auth subsystem
        live_user = await hass.auth.async_get_user(connection.user.id)
        if not live_user or not getattr(live_user, "is_active", True):
            return

        ev = await current_store.get_event_access(event_id)
        if ev and is_user_authorized_for_event(live_user, ev):
            connection.send_message(
                websocket_api.event_message(
                    msg["id"],
                    {
                        "event_id": event_id,
                        "revision": revision,
                    },
                )
            )

    async def _catch_up() -> None:
        connection.send_message(websocket_api.event_message(msg["id"], {"refresh": True}))

    dispatcher = RevisionDispatcher(_on_event_update, _catch_up)
    unsubscribe = store.subscribe(dispatcher.notify)

    def unsub() -> None:
        unsubscribe()
        dispatcher.close()

    connection.subscriptions[msg["id"]] = unsub
    connection.send_result(msg["id"])


# -----------------------------------------------------------------------------
# 4. event_recording/media
# -----------------------------------------------------------------------------
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_TYPE_MEDIA,
        vol.Required("event_id"): str,
        vol.Optional("media_id"): str,
        vol.Optional("thumbnail_id"): str,
        vol.Optional("expires_in", default=300): vol.All(
            vol.Coerce(int), vol.Range(min=30, max=300)
        ),
    }
)
@websocket_api.async_response
async def ws_handle_media(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Resolve opaque media_id or thumbnail_id to a time-limited signed URL."""
    if (
        not connection.user
        or connection.user.system_generated
        or not getattr(connection.user, "is_active", True)
    ):
        connection.send_error(msg["id"], "unauthorized", "Active user authentication required")
        return

    refresh_token_id = getattr(connection, "refresh_token_id", None)
    if not refresh_token_id:
        connection.send_error(
            msg["id"], "unauthorized", "Valid user refresh token required for signed media"
        )
        return

    event_id = msg["event_id"]
    media_id = msg.get("media_id")
    thumbnail_id = msg.get("thumbnail_id")

    # Strict XOR: exactly one of media_id or thumbnail_id must be provided
    if (media_id is None) == (thumbnail_id is None):
        connection.send_error(
            msg["id"],
            "invalid_format",
            "Exactly one of media_id or thumbnail_id must be provided",
        )
        return

    domain_data = hass.data.get(DOMAIN, {})
    store: JournalStore | None = domain_data.get(DATA_STORE)
    if not store:
        connection.send_error(msg["id"], "not_found", "Event Recording is not initialized")
        return

    event = await store.get_event(event_id)
    if not event:
        connection.send_error(msg["id"], "not_found", f"Event '{event_id}' not found")
        return

    if not is_user_authorized_for_event(connection.user, event):
        connection.send_error(msg["id"], "unauthorized", "Access denied")
        return

    expires_in = max(30, min(int(msg.get("expires_in", 300)), 300))
    media_root = store.media_root

    if thumbnail_id == "thumbnail":
        from .thumbnails import async_ensure_thumbnail

        await async_ensure_thumbnail(hass, store, event)

    def _verify_media_target() -> tuple[str | None, str | None, int]:
        resolved_root = media_root.resolve()
        if thumbnail_id:
            if thumbnail_id != "thumbnail":
                return None, None, 400

            from .thumbnails import thumbnail_path

            thumb_file = thumbnail_path(resolved_root, event).resolve()
            if not thumb_file.is_relative_to(resolved_root):
                return None, None, 403

            if thumb_file.is_file() and any(m.available for m in event.media):
                return "thumbnail", "image/jpeg", 200

            img_item = next((m for m in event.media if m.kind == "image" and m.available), None)
            if not img_item:
                return None, None, 404

            target = (resolved_root / img_item.file_name).resolve()
            if not target.is_relative_to(resolved_root):
                return None, None, 403
            if not target.is_file():
                return None, None, 404
            return img_item.id, img_item.mime_type, 200

        assert media_id is not None
        matching = next((m for m in event.media if m.id == media_id), None)
        if not matching or not matching.available:
            return None, None, 404

        target = (resolved_root / matching.file_name).resolve()
        if not target.is_relative_to(resolved_root):
            return None, None, 403
        if not target.is_file():
            return None, None, 404
        return media_id, matching.mime_type, 200

    subpath, mime_type, status_code = await hass.async_add_executor_job(_verify_media_target)

    if status_code == 400:
        connection.send_error(
            msg["id"],
            "not_found",
            f"Invalid thumbnail identifier '{thumbnail_id}'. Use 'thumbnail'.",
        )
        return
    if status_code == 403:
        connection.send_error(msg["id"], "unauthorized", "Media path containment violation")
        return
    if status_code == 404 or subpath is None or mime_type is None:
        target_name = "Thumbnail" if thumbnail_id else f"Media item '{media_id}'"
        connection.send_error(msg["id"], "not_found", f"{target_name} not found or unavailable")
        return

    base_path = f"/api/event_recording/media/{event_id}/{subpath}"
    signed_path = async_sign_path(
        hass,
        base_path,
        timedelta(seconds=expires_in),
        refresh_token_id=refresh_token_id,
    )

    expiry_dt = utc_now() + timedelta(seconds=expires_in)
    connection.send_result(
        msg["id"],
        {
            "url": signed_path,
            "expires_at": format_iso(expiry_dt),
            "mime_type": mime_type,
        },
    )


# -----------------------------------------------------------------------------
# 5. event_recording/process
# -----------------------------------------------------------------------------
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_TYPE_PROCESS,
        vol.Required("event_id"): str,
        vol.Required("consumer"): str,
        vol.Optional("retry", default=True): cv.boolean,
    }
)
@websocket_api.async_response
async def ws_handle_process(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Trigger retry of an external analysis consumer. Admin ONLY."""
    if not connection.user or not connection.user.is_admin:
        connection.send_error(msg["id"], "unauthorized", "Admin privileges required")
        return

    api = get_api(hass)
    if not api:
        connection.send_error(msg["id"], "not_found", "Event Recording is not initialized")
        return

    event_id = msg["event_id"]
    consumer_id = msg["consumer"]

    event = await api.get_event(event_id)
    if not event:
        connection.send_error(msg["id"], "not_found", f"Event '{event_id}' not found")
        return

    accepted = await api.trigger_consumer_retry(consumer_id, event_id)
    connection.send_result(msg["id"], {"accepted": bool(accepted)})


# -----------------------------------------------------------------------------
# 6. event_recording/incident
# -----------------------------------------------------------------------------
@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_TYPE_INCIDENT,
        vol.Required("incident_id"): str,
    }
)
@websocket_api.async_response
async def ws_handle_incident(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Handle retrieving a single incident with strict all-or-nothing member authorization."""
    if (
        not connection.user
        or connection.user.system_generated
        or not getattr(connection.user, "is_active", True)
    ):
        connection.send_error(msg["id"], "unauthorized", "Active user authentication required")
        return

    domain_data = hass.data.get(DOMAIN, {})
    store: JournalStore | None = domain_data.get(DATA_STORE)
    if not store:
        connection.send_error(msg["id"], "not_found", "Event Recording is not initialized")
        return

    incident_id = msg["incident_id"]
    incident = await store.get_incident(incident_id)
    if not incident:
        connection.send_error(msg["id"], "not_found", f"Incident '{incident_id}' not found")
        return

    members = await store.get_incident_events(incident_id)
    # Authorize EVERY member before returning incident details. Safest omit if any member hidden!
    for m in members:
        if not is_user_authorized_for_event(connection.user, m):
            connection.send_error(msg["id"], "unauthorized", "Access denied")
            return

    connection.send_result(
        msg["id"],
        {
            "incident": to_public_incident_dto(incident),
            "events": [to_public_dto(m) for m in members],
        },
    )


# -----------------------------------------------------------------------------
# 7. event_recording/list_activity
# -----------------------------------------------------------------------------
def _matches_activity_exclusion(
    ann_dict: dict[str, Any],
    exclusion: dict[str, Any],
    event_media_revision: int | None = None,
    event: RecordedEvent | None = None,
) -> bool:
    """Return True if annotation payload equals the excluded value and media revision is fresh."""
    ns = exclusion["namespace"]
    field = exclusion["field"]
    expected = exclusion["equals"]
    if ns == "event_vision" and field == "activity" and expected is False:
        return event is not None and can_hide_no_activity(to_public_dto(event))
    ann = ann_dict.get(ns)
    if not isinstance(ann, dict):
        return False
    if ann.get("status") != "complete":
        return False
    # Verify annotation.media_revision == event.media_revision so stale false doesn't hide new media
    if event_media_revision is not None:
        ann_media_rev = ann.get("media_revision", ann.get("expected_media_revision"))
        if ann_media_rev is not None and ann_media_rev != event_media_revision:
            return False
    payload = ann.get("payload")
    if not isinstance(payload, dict):
        payload = {
            k: v for k, v in ann.items() if k not in ("worker_id", "reserved_by", "reserved_until")
        }
    if field not in payload:
        return False
    actual = payload[field]
    if isinstance(actual, bool) or isinstance(expected, bool):
        return type(actual) is type(expected) and actual == expected
    return actual == expected


@websocket_api.websocket_command(
    {
        vol.Required("type"): WS_TYPE_LIST_ACTIVITY,
        vol.Optional("source_id"): str,
        vol.Optional("kind"): str,
        vol.Optional("exclude_current_complete_annotation"): _validate_annotation_exclusion,
        vol.Optional("exclude_journal_only"): bool,
        vol.Optional("group_by_incident", default=False): bool,
        vol.Optional("limit", default=50): vol.All(vol.Coerce(int), vol.Range(min=1, max=100)),
        vol.Optional("before_cursor"): str,
        vol.Optional("cursor"): str,
    }
)
@websocket_api.async_response
async def ws_handle_list_activity(
    hass: HomeAssistant,
    connection: websocket_api.ActiveConnection,
    msg: dict[str, Any],
) -> None:
    """Handle listing activity feed with optional incident grouping and keyset pagination."""
    if (
        not connection.user
        or connection.user.system_generated
        or not getattr(connection.user, "is_active", True)
    ):
        connection.send_error(msg["id"], "unauthorized", "Active user authentication required")
        return

    domain_data = hass.data.get(DOMAIN, {})
    store: JournalStore | None = domain_data.get(DATA_STORE)
    if not store:
        connection.send_error(msg["id"], "not_found", "Event Recording is not initialized")
        return

    api = get_api(hass)
    limit = max(1, min(int(msg.get("limit", 50)), 100))
    cursor = msg.get("before_cursor") or msg.get("cursor")
    group_by_incident = bool(msg.get("group_by_incident", False))

    source_id_filter = msg.get("source_id")
    kind_filter = msg.get("kind")
    exclude_journal_only = msg.get("exclude_journal_only") is True
    annotation_exclusion = msg.get("exclude_current_complete_annotation")

    def _is_journal_only(ev: RecordedEvent) -> bool:
        # Match list_events and the card: missing media and capture failures are
        # useful warnings, rather than successful journal-only captures.
        return not ev.media and not ev.capture_error and ev.capture_state in ("ready", "expired")

    def _event_passes_filters(ev: RecordedEvent) -> bool:
        if source_id_filter and ev.source_id != source_id_filter:
            return False
        if kind_filter and ev.kind != kind_filter:
            return False
        if exclude_journal_only and _is_journal_only(ev):
            return False
        if annotation_exclusion and _matches_activity_exclusion(
            ev.annotations, annotation_exclusion, ev.media_revision, event=ev
        ):
            return False
        return True

    def _incident_passes_filters(members: list[RecordedEvent]) -> bool:
        if source_id_filter and not any(m.source_id == source_id_filter for m in members):
            return False
        if kind_filter and not any(m.kind == kind_filter for m in members):
            return False
        if exclude_journal_only and members and all(_is_journal_only(m) for m in members):
            return False
        if annotation_exclusion:
            # A group hides only when every member independently qualifies.
            # Unknown observations or capture warnings keep the group visible.
            if len(members) > 0 and all(
                _matches_activity_exclusion(
                    m.annotations, annotation_exclusion, m.media_revision, event=m
                )
                for m in members
            ):
                return False
        return True

    output_items: list[dict[str, Any]] = []
    current_cursor = cursor
    next_cursor: str | None = None
    max_scan_batches = 500  # Unbounded semantic progress (safety bound against infinite loop)
    batch_size = max(limit * 2, 50)
    incident_authorized_cache: dict[str, bool] = {}

    for _ in range(max_scan_batches):
        batch, batch_next_cur = await store.list_activity(
            group_by_incident=group_by_incident,
            limit=batch_size,
            cursor=current_cursor,
        )
        if not batch:
            next_cursor = None
            break

        stopped_early = False
        for item in batch:
            itype = item["type"]
            item_accepted = False

            if itype == "incident":
                inc = item["incident"]
                members = item["members"]
                iid = inc.incident_id
                authorized_members = [
                    m for m in members if is_user_authorized_for_event(connection.user, m)
                ]
                is_authorized = len(authorized_members) == len(members)
                incident_authorized_cache[iid] = is_authorized

                if is_authorized:
                    if _incident_passes_filters(members):
                        output_items.append(
                            {
                                "type": "incident",
                                "incident": to_public_incident_dto(inc),
                                "events": [to_public_dto(m) for m in members],
                            }
                        )
                        item_accepted = True
                # If NOT authorized, omit the whole incident!
                # Its visible members will be projected as independent events at their own immutable start_at anchors.

            else:
                ev = item["event"]
                if is_user_authorized_for_event(connection.user, ev):
                    # Check if this event belongs to an authorized incident
                    inc_ids = item.get("incident_ids") or []
                    belongs_to_authorized_incident = False
                    if group_by_incident and inc_ids:
                        for inc_id in inc_ids:
                            if inc_id not in incident_authorized_cache:
                                inc_members = await store.get_incident_events(inc_id)
                                incident_authorized_cache[inc_id] = all(
                                    is_user_authorized_for_event(connection.user, m)
                                    for m in inc_members
                                )
                            if incident_authorized_cache[inc_id]:
                                belongs_to_authorized_incident = True
                                break

                    # If the event belongs to an authorized incident, it is matched to that incident;
                    # do NOT emit it as an independent event.
                    # If it does NOT belong to any authorized incident (unmatched visible event or incident omitted due to hidden other member),
                    # emit it as an independent event with omit_correlation=True.
                    if not belongs_to_authorized_incident:
                        if _event_passes_filters(ev):
                            output_items.append(
                                {
                                    "type": "event",
                                    "event": to_public_dto(ev, omit_correlation=group_by_incident),
                                }
                            )
                            item_accepted = True

            if item_accepted and len(output_items) >= limit:
                if item is batch[-1] and batch_next_cur is None:
                    next_cursor = None
                else:
                    next_cursor = f"{item['sort_time']}|{item['type']}|{item['item_id']}"
                stopped_early = True
                break

        if stopped_early:
            break

        if batch_next_cur is not None:
            current_cursor = batch_next_cur
            next_cursor = batch_next_cur
        else:
            next_cursor = None
            break

    consumers = api.get_registered_consumers() if api else []

    connection.send_result(
        msg["id"],
        {
            "items": output_items[:limit],
            "next_cursor": next_cursor,
            "capabilities": {
                "consumers": consumers,
            },
        },
    )
