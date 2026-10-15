"""Authenticated HTTP streaming view for Event Recording media assets."""

from __future__ import annotations

import logging
from pathlib import Path

from aiohttp import web
from homeassistant.components.http import HomeAssistantView
from homeassistant.components.http.const import KEY_HASS_USER
from homeassistant.core import HomeAssistant

from .const import DATA_STORE, DOMAIN
from .core.store import JournalStore
from .websocket import is_user_authorized_for_event

_LOGGER = logging.getLogger(__name__)


class EventRecordingMediaView(HomeAssistantView):
    """View to stream recorded media files and thumbnails with Range support and strict authorization."""

    url = "/api/event_recording/media/{event_id}/{media_id}"
    name = "api:event_recording:media"
    requires_auth = True

    def __init__(self, hass: HomeAssistant) -> None:
        """Initialize the media view."""
        self.hass = hass

    async def get(self, request: web.Request, event_id: str, media_id: str) -> web.StreamResponse:
        """Stream media file with Range support and strict entity authorization."""
        domain_data = self.hass.data.get(DOMAIN, {})
        store: JournalStore | None = domain_data.get(DATA_STORE)
        if not store:
            return web.Response(status=404, text="Event Recording not initialized")

        # 1. User authentication validation via actual KEY_HASS_USER
        user = request.get(KEY_HASS_USER)
        if not user or not getattr(user, "is_active", True):
            return web.Response(status=401, text="Unauthorized: User required")

        # Revalidate live user status from auth subsystem
        live_user = await self.hass.auth.async_get_user(user.id)
        if not live_user or not getattr(live_user, "is_active", True):
            return web.Response(status=401, text="Unauthorized: User inactive or revoked")

        # 2. Event existence and authorization
        event = await store.get_event(event_id)
        if not event:
            return web.Response(status=404, text=f"Event '{event_id}' not found")

        if not is_user_authorized_for_event(live_user, event):
            return web.Response(status=403, text="Forbidden: Access denied for event entities")

        media_root = store.media_root

        # 3. Path resolution & containment check performed off-loop
        def _resolve_target_media() -> tuple[Path | None, str | None, int]:
            resolved_root = media_root.resolve()
            if media_id == "thumbnail":
                from .thumbnails import thumbnail_path

                thumb_path = thumbnail_path(resolved_root, event).resolve()
                if not thumb_path.is_relative_to(resolved_root):
                    return None, None, 403
                if thumb_path.is_file() and any(m.available for m in event.media):
                    return thumb_path, "image/jpeg", 200

                img_item = next((m for m in event.media if m.kind == "image" and m.available), None)
                if not img_item:
                    return None, None, 404
                target_file = (resolved_root / img_item.file_name).resolve()
                if not target_file.is_relative_to(resolved_root):
                    return None, None, 403
                if not target_file.is_file():
                    return None, None, 404
                return target_file, img_item.mime_type, 200

            matching = next((m for m in event.media if m.id == media_id), None)
            if not matching or not matching.available:
                return None, None, 404

            target_file = (resolved_root / matching.file_name).resolve()
            if not target_file.is_relative_to(resolved_root):
                return None, None, 403
            if not target_file.is_file():
                return None, None, 404
            return target_file, matching.mime_type, 200

        target_path, content_type, status_code = await self.hass.async_add_executor_job(
            _resolve_target_media
        )

        if status_code == 403:
            return web.Response(status=403, text="Access denied: Path containment violation")
        if status_code == 404 or target_path is None or content_type is None:
            return web.Response(status=404, text="Media asset not found or unavailable")

        # Return FileResponse with private, no-store for clean revocation support
        return web.FileResponse(
            path=target_path,
            headers={
                "Accept-Ranges": "bytes",
                "Cache-Control": "private, no-store",
                "Content-Type": content_type,
            },
        )
