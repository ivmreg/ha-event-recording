"""Event Recording Integration.

Neutral durable event journal and media store for Home Assistant.
Zero AI imports, zero Gemini dependencies, zero API keys.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import Event, HomeAssistant

from .const import (
    CARD_URL_PATH,
    CONF_ENABLE_DEBUG_LOGGING,
    CONF_MEDIA_ROOT,
    DATA_API,
    DATA_COORDINATOR,
    DATA_REGISTRY,
    DATA_SETUP_DONE,
    DATA_STORE,
    DEFAULT_ENABLE_DEBUG_LOGGING,
    DEFAULT_MEDIA_DIR,
    DOMAIN,
)
from .logging_util import configure_integration_logging

_LOGGER = logging.getLogger(__name__)


__all__ = [
    "DOMAIN",
    "async_reload_entry",
    "async_setup",
    "async_setup_entry",
    "async_unload_entry",
    "configure_integration_logging",
]


async def async_setup(hass: HomeAssistant, config: dict[str, Any]) -> bool:
    """Set up the Event Recording component once globally."""
    from homeassistant.components.http import StaticPathConfig

    from .media import EventRecordingMediaView
    from .recorder import RecorderRegistry
    from .services import async_register_services
    from .websocket import async_register_websocket_api

    hass.data.setdefault(DOMAIN, {})

    if hass.data[DOMAIN].get(DATA_SETUP_DONE):
        return True

    # Initialize shared recorder registry surviving entry reload
    if DATA_REGISTRY not in hass.data[DOMAIN]:
        hass.data[DOMAIN][DATA_REGISTRY] = RecorderRegistry()

    registered_items: list[str] = []

    try:
        # 1. Register packaged frontend static JS bundle if present
        card_file = Path(__file__).parent / "frontend" / "event-card.js"
        card_exists = await hass.async_add_executor_job(card_file.is_file)
        if card_exists:
            try:
                await hass.http.async_register_static_paths(
                    [StaticPathConfig(CARD_URL_PATH, str(card_file), cache_headers=True)]
                )
                registered_items.append("static_path")
            except Exception as err:
                _LOGGER.debug("Notice on card static path registration: %s", err)

        # 2. Register services and WebSocket commands
        async_register_services(hass)
        registered_items.append("services")

        async_register_websocket_api(hass)
        registered_items.append("websocket")

        # 3. Register authenticated media streaming view
        hass.http.register_view(EventRecordingMediaView(hass))
        registered_items.append("media_view")

        hass.data[DOMAIN][DATA_SETUP_DONE] = True
        return True

    except Exception as err:
        _LOGGER.error("Failed to initialize Event Recording integration: %s", err)
        # Rollback partial setup
        if "services" in registered_items:
            for s_name in ("log_event", "record", "record_events", "attach_media", "finalize"):
                hass.services.async_remove(DOMAIN, s_name)
        return False


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Event Recording from a config entry."""
    import shutil

    from homeassistant.const import EVENT_HOMEASSISTANT_STOP

    from .api import RecordingAPI
    from .coordinator import RecordingCoordinator
    from .core.media import FFmpegProbe
    from .core.store import JournalStore

    data = entry.data
    options = entry.options

    def get_val(key: str, default: Any) -> Any:
        return options.get(key, data.get(key, default))

    configure_integration_logging(
        bool(get_val(CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING))
    )

    raw_media_dir = str(get_val(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR)).strip()
    from .config_flow import validate_media_root

    root_err = await hass.async_add_executor_job(validate_media_root, hass, raw_media_dir)
    if root_err:
        _LOGGER.error(
            "Event Recording media directory '%s' is rejected: %s", raw_media_dir, root_err
        )
        return False

    if Path(raw_media_dir).is_absolute():
        media_root = Path(raw_media_dir)
    else:
        media_root = Path(hass.config.path(raw_media_dir))

    db_path = Path(hass.config.path("event_recording.db"))

    # Off-loop probe configuration detection
    ffprobe_bin = await hass.async_add_executor_job(shutil.which, "ffprobe")
    probe = FFmpegProbe(ffprobe_bin) if ffprobe_bin else None

    # Allowed import roots include owned private root, configured media_dirs, and media fallback
    allowed_roots: list[Path] = [media_root]
    media_dirs = getattr(hass.config, "media_dirs", {}) or {}
    for m_dir in media_dirs.values():
        try:
                allowed_roots.append(Path(m_dir))
        except Exception:
            pass
    try:
        allowed_roots.append(Path(hass.config.path("media")))
    except Exception:
        pass

    store = JournalStore(
        db_path=db_path,
        media_root=media_root,
        allowed_media_roots=allowed_roots,
        probe=probe,
    )

    await store.open()

    registry = hass.data[DOMAIN][DATA_REGISTRY]
    coordinator = RecordingCoordinator(hass, store, entry, registry)

    try:
        await coordinator.start()
    except Exception as err:
        _LOGGER.error("Failed to start RecordingCoordinator: %s", err)
        await store.close()
        raise

    api = RecordingAPI(hass, store)

    hass.data[DOMAIN][DATA_STORE] = store
    hass.data[DOMAIN][DATA_COORDINATOR] = coordinator
    hass.data[DOMAIN][DATA_API] = api

    # Graceful stop on HA shutdown
    async def _async_on_ha_stop(event: Event) -> None:
        await coordinator.stop()
        await store.close()

    entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, _async_on_ha_stop))
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload an Event Recording config entry."""
    from .coordinator import RecordingCoordinator
    from .core.store import JournalStore

    domain_data = hass.data.get(DOMAIN, {})

    coordinator: RecordingCoordinator | None = domain_data.get(DATA_COORDINATOR)
    if coordinator:
        await coordinator.stop()

    store: JournalStore | None = domain_data.get(DATA_STORE)
    if store:
        await store.close()

    domain_data.pop(DATA_STORE, None)
    domain_data.pop(DATA_COORDINATOR, None)
    domain_data.pop(DATA_API, None)

    return True


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Apply options immediately, reloading only when the media root changes."""
    from .const import CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR

    domain_data = hass.data.get(DOMAIN, {})
    coordinator = domain_data.get(DATA_COORDINATOR)
    if coordinator is None:
        await hass.config_entries.async_reload(entry.entry_id)
        return

    raw_root = str(
        entry.options.get(CONF_MEDIA_ROOT, entry.data.get(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR))
    ).strip()
    configured_root = Path(raw_root)
    effective_root = (
        configured_root if configured_root.is_absolute() else Path(hass.config.path(raw_root))
    )
    try:
        root_changed = effective_root.resolve() != coordinator.store.media_root.resolve()
    except (OSError, RuntimeError):
        root_changed = effective_root != coordinator.store.media_root

    if root_changed:
        await hass.config_entries.async_reload(entry.entry_id)
        return

    # Entry options are read dynamically by retention properties and profile lookup.
    coordinator.entry = entry
    configure_integration_logging(
        bool(
            entry.options.get(
                CONF_ENABLE_DEBUG_LOGGING,
                entry.data.get(CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING),
            )
        )
    )
    await coordinator.set_max_parallel_resources(coordinator.max_parallel_resources)
