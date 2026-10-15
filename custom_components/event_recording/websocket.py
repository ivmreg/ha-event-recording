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
    WS_TYPE_MEDIA,
    WS_TYPE_PROCESS,
    WS_TYPE_SUBSCRIBE,
)
from .core.models import RecordedEvent, format_iso, to_public_dto, utc_now
from .core.store import JournalStore

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

    filters: dict[str, Any] = {}
    if "source_id" in msg:
        filters["source_id"] = msg["source_id"]
    if "kind" in msg:
        filters["kind"] = msg["kind"]
    if "exclude_current_complete_annotation" in msg:
        filters["exclude_current_complete_annotation"] = msg[
            "exclude_current_complete_annotation"
        ]
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
    event = await store.get_event(event_id)
    if not event:
        connection.send_error(msg["id"], "not_found", f"Event '{event_id}' not found")
        return

    if not is_user_authorized_for_event(connection.user, event):
        connection.send_error(msg["id"], "unauthorized", "Access denied")
        return

    connection.send_result(msg["id"], {"event": to_public_dto(event)})


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

        ev = await current_store.get_event(event_id)
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

    def _sync_cb(event_id: str, revision: int) -> None:
        hass.async_create_task(_on_event_update(event_id, revision))

    unsub = store.subscribe(_sync_cb)
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
