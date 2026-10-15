"""Config flow for Event Recording integration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult

from .const import (
    CONF_CHUNK_SECONDS,
    CONF_MEDIA_ROOT,
    CONF_RETENTION_DAYS,
    CONF_RETENTION_SIZE_MB,
    DEFAULT_CHUNK_SECONDS,
    DEFAULT_MEDIA_DIR,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_RETENTION_SIZE_MB,
    DOMAIN,
)


def validate_media_root(hass: Any, raw_media: str) -> str | None:
    """Validate media_root is not inside www or configured media_dirs.

    Returns error key ('www_forbidden', 'media_dir_exposed') if forbidden, or None if valid.
    """
    raw = str(raw_media).strip()
    if not raw:
        return "invalid_media_root"
    if "www" in raw.replace("\\", "/").lower().strip("/").split("/"):
        return "www_forbidden"
    try:
        config_root = Path(hass.config.path()).resolve()
        candidate = Path(raw)
        resolved = (candidate if candidate.is_absolute() else config_root / candidate).resolve()
        if resolved == config_root or resolved in config_root.parents:
            return "invalid_media_root"
        www_root = (config_root / "www").resolve()
        if resolved.is_relative_to(www_root) or www_root.is_relative_to(resolved):
            return "www_forbidden"
        for directory in (getattr(hass.config, "media_dirs", {}) or {}).values():
            exposed = Path(directory).resolve()
            if resolved.is_relative_to(exposed) or exposed.is_relative_to(resolved):
                return "media_dir_exposed"
        return None
    except (OSError, ValueError, RuntimeError):
        return "invalid_media_root"


class EventRecordingConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Event Recording."""

    VERSION = 1
    single_instance_allowed = True

    async def async_step_user(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Handle the initial setup step."""
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        errors: dict[str, str] = {}

        if user_input is not None:
            # 1. Reject media_root inside www or configured media_dirs
            raw_media = str(user_input.get(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR)).strip()
            err = await self.hass.async_add_executor_job(validate_media_root, self.hass, raw_media)
            if err:
                errors[CONF_MEDIA_ROOT] = err

            # 2. Validate chunk_seconds <= 15
            chunk_seconds = user_input.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS)
            if chunk_seconds > 15 or chunk_seconds < 1:
                errors[CONF_CHUNK_SECONDS] = "invalid_chunk_seconds"

            if not errors:
                return self.async_create_entry(
                    title="Event Recording",
                    data=user_input,
                )

        schema = vol.Schema(
            {
                vol.Required(CONF_MEDIA_ROOT, default=DEFAULT_MEDIA_DIR): str,
                vol.Required(CONF_RETENTION_SIZE_MB, default=DEFAULT_RETENTION_SIZE_MB): vol.All(
                    vol.Coerce(int), vol.Range(min=50)
                ),
                vol.Required(CONF_RETENTION_DAYS, default=DEFAULT_RETENTION_DAYS): vol.All(
                    vol.Coerce(int), vol.Range(min=1)
                ),
                vol.Required(CONF_CHUNK_SECONDS, default=DEFAULT_CHUNK_SECONDS): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=15)
                ),
            }
        )

        return self.async_show_form(
            step_id="user",
            data_schema=schema,
            errors=errors,
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Get the options flow for this handler."""
        return EventRecordingOptionsFlow(config_entry)


class EventRecordingOptionsFlow(config_entries.OptionsFlow):
    """Handle options flow for Event Recording."""

    def __init__(self, config_entry: config_entries.ConfigEntry | None = None) -> None:
        """Initialize options flow."""
        self._own_entry = config_entry

    @property
    def config_entry(self) -> config_entries.ConfigEntry:
        """Return the config entry, using own entry or framework property."""
        if getattr(self, "_own_entry", None) is not None:
            return self._own_entry
        return super().config_entry

    async def async_step_init(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Manage the options."""
        errors: dict[str, str] = {}

        if user_input is not None:
            if CONF_MEDIA_ROOT in user_input:
                raw_media = str(user_input.get(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR)).strip()
                err = await self.hass.async_add_executor_job(validate_media_root, self.hass, raw_media)
                if err:
                    errors[CONF_MEDIA_ROOT] = err

            chunk_seconds = user_input.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS)
            if chunk_seconds > 15 or chunk_seconds < 1:
                errors[CONF_CHUNK_SECONDS] = "invalid_chunk_seconds"

            if not errors:
                return self.async_create_entry(title="", data=user_input)

        entry = self.config_entry
        current_size = entry.options.get(
            CONF_RETENTION_SIZE_MB,
            entry.data.get(CONF_RETENTION_SIZE_MB, DEFAULT_RETENTION_SIZE_MB),
        )
        current_days = entry.options.get(
            CONF_RETENTION_DAYS,
            entry.data.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
        )
        current_chunk = entry.options.get(
            CONF_CHUNK_SECONDS,
            entry.data.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS),
        )

        schema = vol.Schema(
            {
                vol.Required(CONF_RETENTION_SIZE_MB, default=current_size): vol.All(
                    vol.Coerce(int), vol.Range(min=50)
                ),
                vol.Required(CONF_RETENTION_DAYS, default=current_days): vol.All(
                    vol.Coerce(int), vol.Range(min=1)
                ),
                vol.Required(CONF_CHUNK_SECONDS, default=current_chunk): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=15)
                ),
            }
        )

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
        )
