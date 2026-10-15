"""Config flow for Event Recording integration."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import entity_registry, selector

from .camera_profiles import CAMERA_PROFILES, default_camera_profile, validate_camera_profile
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


class _IntegerNumberSelector(selector.NumberSelector):
    """Native number selector that rejects booleans before float coercion."""

    def __call__(self, data: Any) -> float:
        if isinstance(data, bool):
            raise vol.Invalid("Boolean values are not numbers")
        return super().__call__(data)


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
        """Show the options menu."""
        return self.async_show_menu(step_id="init", menu_options=["recording_settings", "cameras"])

    async def async_step_recording_settings(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage global storage and recording defaults."""
        errors: dict[str, str] = {}

        if user_input is not None:
            if CONF_MEDIA_ROOT in user_input:
                raw_media = str(user_input.get(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR)).strip()
                err = await self.hass.async_add_executor_job(
                    validate_media_root, self.hass, raw_media
                )
                if err:
                    errors[CONF_MEDIA_ROOT] = err

            chunk_seconds = user_input.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS)
            if chunk_seconds > 15 or chunk_seconds < 1:
                errors[CONF_CHUNK_SECONDS] = "invalid_chunk_seconds"

            if not errors:
                return self.async_create_entry(
                    title="", data={**self._current_options(), **user_input}
                )

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
            step_id="recording_settings",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_cameras(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Show camera profile actions."""
        return self.async_show_menu(
            step_id="cameras",
            menu_options=["camera_add", "camera_edit", "camera_remove"],
        )

    async def async_step_camera_add(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Choose a camera for the new profile."""
        errors: dict[str, str] = {}
        if user_input is not None:
            entity_id = str(user_input.get("camera_entity", ""))
            errors.update(await self._validate_camera_entity(entity_id))
            if entity_id in self._profiles():
                errors["camera_entity"] = "camera_profile_exists"
            if not errors:
                self._add_camera_entity = entity_id
                return await self.async_step_camera_add_profile()

        return self.async_show_form(
            step_id="camera_add",
            data_schema=self._camera_entity_schema(),
            errors=errors,
        )

    async def async_step_camera_add_profile(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Set defaults for the selected camera."""
        entity_id = getattr(self, "_add_camera_entity", None)
        if not entity_id:
            return self.async_abort(reason="camera_profile_missing")
        defaults = default_camera_profile(
            entity_id,
            name=self._camera_name(entity_id),
            chunk_seconds=self._global_chunk_seconds(),
            max_window_seconds=self._global_max_window_seconds(),
        )
        errors: dict[str, str] = {}
        if user_input is not None:
            profile = self._profile_from_input(user_input, defaults)
            errors.update(validate_camera_profile(entity_id, profile))
            if not errors:
                profiles = self._profiles()
                if entity_id in profiles:
                    return self.async_abort(reason="camera_profile_missing")
                profiles[entity_id] = profile
                return self._save_profiles(profiles)
        return self.async_show_form(
            step_id="camera_add_profile",
            data_schema=self._profile_schema(
                {**defaults, **(user_input or {})}, include_entity=False
            ),
            errors=errors,
            description_placeholders={"camera_name": self._camera_name(entity_id)},
        )

    async def async_step_camera_edit(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Select an existing profile to edit."""
        profiles = self._profiles()
        if not profiles:
            return self.async_abort(reason="no_camera_profiles")
        if user_input is not None:
            entity_id = str(user_input.get("camera_entity", ""))
            if entity_id not in profiles:
                return self.async_show_form(
                    step_id="camera_edit",
                    data_schema=self._profile_selector_schema(profiles, entity_id),
                    errors={"camera_entity": "camera_profile_missing"},
                )
            self._edit_camera_entity = entity_id
            return await self.async_step_camera_edit_profile()
        return self.async_show_form(
            step_id="camera_edit", data_schema=self._profile_selector_schema(profiles)
        )

    async def async_step_camera_edit_profile(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Edit one stored camera profile."""
        profiles = self._profiles()
        original_entity = getattr(self, "_edit_camera_entity", None)
        if original_entity not in profiles:
            return self.async_abort(reason="camera_profile_missing")
        old_profile = profiles[original_entity]
        current = {**default_camera_profile(original_entity), **old_profile}
        errors: dict[str, str] = {}
        if user_input is not None:
            new_entity = str(user_input.get("camera_entity", original_entity))
            errors.update(await self._validate_camera_entity(new_entity))
            if new_entity != original_entity and new_entity in profiles:
                errors["camera_entity"] = "camera_profile_exists"
            profile = self._profile_from_input(user_input, current)
            errors.update(validate_camera_profile(new_entity, profile))
            if not errors:
                profiles.pop(original_entity)
                profiles[new_entity] = profile
                return self._save_profiles(profiles)
        displayed = {**current, **(user_input or {})}
        return self.async_show_form(
            step_id="camera_edit_profile",
            data_schema=self._profile_schema(
                displayed,
                include_entity=True,
                entity_default=(user_input or {}).get("camera_entity", original_entity),
            ),
            errors=errors,
        )

    async def async_step_camera_remove(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Select a profile to remove."""
        profiles = self._profiles()
        if not profiles:
            return self.async_abort(reason="no_camera_profiles")
        if user_input is not None:
            entity_id = str(user_input.get("camera_entity", ""))
            if entity_id not in profiles:
                return self.async_show_form(
                    step_id="camera_remove",
                    data_schema=self._profile_selector_schema(profiles, entity_id),
                    errors={"camera_entity": "camera_profile_missing"},
                )
            self._remove_camera_entity = entity_id
            return await self.async_step_camera_remove_confirm()
        return self.async_show_form(
            step_id="camera_remove", data_schema=self._profile_selector_schema(profiles)
        )

    async def async_step_camera_remove_confirm(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Confirm removal of a profile without touching recordings."""
        entity_id = getattr(self, "_remove_camera_entity", None)
        profiles = self._profiles()
        if entity_id not in profiles:
            return self.async_abort(reason="camera_profile_missing")
        if user_input is not None:
            if user_input.get("confirm"):
                profiles.pop(entity_id)
                return self._save_profiles(profiles)
            return self.async_abort(reason="remove_cancelled")
        return self.async_show_form(
            step_id="camera_remove_confirm",
            data_schema=vol.Schema({vol.Required("confirm", default=False): bool}),
            description_placeholders={
                "camera_name": self._camera_name(entity_id),
                "camera_entity": entity_id,
            },
        )

    def _current_options(self) -> dict[str, Any]:
        """Copy current options so saving one section preserves all other settings."""
        return dict(self.config_entry.options)

    def _profiles(self) -> dict[str, dict[str, Any]]:
        """Return profiles with legacy entry data as fallback."""
        from .camera_profiles import get_camera_profiles

        return get_camera_profiles(self.config_entry)

    def _save_profiles(self, profiles: dict[str, dict[str, Any]]) -> FlowResult:
        options = self._current_options()
        options[CAMERA_PROFILES] = profiles
        return self.async_create_entry(title="", data=options)

    def _global_chunk_seconds(self) -> int:
        return int(
            self.config_entry.options.get(
                CONF_CHUNK_SECONDS,
                self.config_entry.data.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS),
            )
        )

    def _global_max_window_seconds(self) -> int:
        from .const import CONF_MAX_WINDOW_SECONDS, DEFAULT_MAX_WINDOW_SECONDS

        return int(
            self.config_entry.options.get(
                CONF_MAX_WINDOW_SECONDS,
                self.config_entry.data.get(CONF_MAX_WINDOW_SECONDS, DEFAULT_MAX_WINDOW_SECONDS),
            )
        )

    @staticmethod
    def _camera_entity_schema() -> Any:
        return vol.Schema(
            {
                vol.Required("camera_entity"): selector.EntitySelector(
                    selector.EntitySelectorConfig(domain="camera")
                )
            }
        )

    @staticmethod
    def _profile_schema(
        profile: dict[str, Any], *, include_entity: bool, entity_default: str | None = None
    ) -> Any:
        schema: dict[Any, Any] = {}
        if include_entity:
            entity_field = vol.Required("camera_entity")
            if entity_default:
                entity_field = vol.Required("camera_entity", default=entity_default)
            schema[entity_field] = selector.EntitySelector(
                selector.EntitySelectorConfig(domain="camera")
            )
        schema.update(
            {
                vol.Required("source_id", default=profile.get("source_id", "camera")): str,
                vol.Required("label", default=profile.get("label", "Camera")): str,
                vol.Required(
                    "window_seconds", default=profile.get("window_seconds", 15)
                ): _IntegerNumberSelector(
                    selector.NumberSelectorConfig(
                        min=1, max=300, step=1, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                vol.Required(
                    "lookback_seconds", default=profile.get("lookback_seconds", 0)
                ): _IntegerNumberSelector(
                    selector.NumberSelectorConfig(
                        min=0, max=60, step=1, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                vol.Required(
                    "chunk_seconds", default=profile.get("chunk_seconds", 10)
                ): _IntegerNumberSelector(
                    selector.NumberSelectorConfig(
                        min=1, max=15, step=1, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                vol.Required(
                    "max_window_seconds", default=profile.get("max_window_seconds", 300)
                ): _IntegerNumberSelector(
                    selector.NumberSelectorConfig(
                        min=1, max=300, step=1, mode=selector.NumberSelectorMode.BOX
                    )
                ),
            }
        )
        return vol.Schema(schema)

    def _profile_selector_schema(
        self, profiles: dict[str, dict[str, Any]], default: str | None = None
    ) -> Any:
        choices = [
            {"value": entity_id, "label": f"{self._camera_name(entity_id)} ({entity_id})"}
            for entity_id in sorted(profiles)
        ]
        entity_field = vol.Required("camera_entity")
        if default in profiles:
            entity_field = vol.Required("camera_entity", default=default)
        return vol.Schema(
            {
                entity_field: selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=choices, mode=selector.SelectSelectorMode.DROPDOWN
                    )
                )
            }
        )

    @staticmethod
    def _profile_from_input(user_input: dict[str, Any], defaults: dict[str, Any]) -> dict[str, Any]:
        profile = {
            key: user_input.get(key, defaults.get(key))
            for key in (
                "source_id",
                "label",
                "window_seconds",
                "lookback_seconds",
                "chunk_seconds",
                "max_window_seconds",
            )
        }
        for field in (
            "window_seconds",
            "lookback_seconds",
            "chunk_seconds",
            "max_window_seconds",
        ):
            value = profile[field]
            if isinstance(value, float) and math.isfinite(value) and value.is_integer():
                profile[field] = int(value)
        return profile

    async def _validate_camera_entity(self, entity_id: str) -> dict[str, str]:
        if not entity_id.startswith("camera."):
            return {"camera_entity": "invalid_camera_entity"}
        if self.hass.states.get(entity_id) is not None:
            return {}
        registry = entity_registry.async_get(self.hass)
        if registry.async_get(entity_id) is not None:
            return {}
        return {"camera_entity": "camera_entity_not_found"}

    def _camera_name(self, entity_id: str) -> str:
        state = self.hass.states.get(entity_id)
        if state is not None and state.name:
            return state.name
        registry = entity_registry.async_get(self.hass)
        entry = registry.async_get(entity_id)
        if entry is not None and (entry.name or entry.original_name):
            return entry.name or entry.original_name or entity_id
        return " ".join(part.capitalize() for part in entity_id.partition(".")[2].split("_"))
