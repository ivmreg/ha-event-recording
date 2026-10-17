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
    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    CONF_CHUNK_SECONDS,
    CONF_ENABLE_AUTO_ASSOCIATION,
    CONF_EXPORT_DELAY_SECONDS,
    CONF_FRIGATE_AUTH_TOKEN,
    CONF_FRIGATE_BASE_URL,
    CONF_FRIGATE_CAMERA_NAME,
    CONF_MAX_INCIDENT_SPAN_SECONDS,
    CONF_MAX_PARALLEL_RESOURCES,
    CONF_MAX_QUEUE_DEPTH,
    CONF_MAX_WINDOW_SECONDS,
    CONF_MEDIA_ROOT,
    CONF_MIN_DISK_FREE_MB,
    CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
    CONF_RETENTION_DAYS,
    CONF_RETENTION_SIZE_MB,
    DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    DEFAULT_CHUNK_SECONDS,
    DEFAULT_ENABLE_AUTO_ASSOCIATION,
    DEFAULT_EXPORT_DELAY_SECONDS,
    DEFAULT_MAX_INCIDENT_SPAN_SECONDS,
    DEFAULT_MAX_PARALLEL_RESOURCES,
    DEFAULT_MAX_QUEUE_DEPTH,
    DEFAULT_MAX_WINDOW_SECONDS,
    DEFAULT_MEDIA_DIR,
    DEFAULT_MIN_DISK_FREE_MB,
    DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS,
    DEFAULT_RETENTION_DAYS,
    DEFAULT_RETENTION_SIZE_MB,
    DEFAULT_WATCHDOG_PADDING_SECONDS,
    DOMAIN,
    MODE_BUFFERED,
    MODE_EXTEND,
    MODE_NATIVE_FIXED,
    MODE_NATIVE_SNAPSHOT,
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
        return self.async_show_menu(
            step_id="init",
            menu_options=["recording_settings", "storage_settings", "cameras", "effective_config"],
        )

    async def async_step_recording_settings(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage global recording concurrency and queue limits."""
        errors: dict[str, str] = {}

        if user_input is not None:
            chunk_seconds = user_input.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS)
            if chunk_seconds > 15 or chunk_seconds < 1:
                errors[CONF_CHUNK_SECONDS] = "invalid_chunk_seconds"
            max_parallel = user_input.get(
                CONF_MAX_PARALLEL_RESOURCES, DEFAULT_MAX_PARALLEL_RESOURCES
            )
            if max_parallel < 1 or max_parallel > 8:
                errors[CONF_MAX_PARALLEL_RESOURCES] = "invalid_max_parallel"
            max_queue = user_input.get(CONF_MAX_QUEUE_DEPTH, DEFAULT_MAX_QUEUE_DEPTH)
            if max_queue < 1 or max_queue > 256:
                errors[CONF_MAX_QUEUE_DEPTH] = "invalid_max_queue_depth"
            queue_timeout = user_input.get(
                CONF_QUEUE_WAIT_TIMEOUT_SECONDS, DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS
            )
            if queue_timeout < 1 or queue_timeout > 300:
                errors[CONF_QUEUE_WAIT_TIMEOUT_SECONDS] = "invalid_queue_timeout"

            if CONF_ENABLE_AUTO_ASSOCIATION in user_input:
                user_input[CONF_ENABLE_AUTO_ASSOCIATION] = bool(
                    user_input[CONF_ENABLE_AUTO_ASSOCIATION]
                )
            if CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS in user_input:
                raw_tol = user_input[CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS]
                if not isinstance(raw_tol, (int, float)) or not (1.0 <= float(raw_tol) <= 60.0):
                    errors[CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS] = "invalid_tolerance"
            if CONF_MAX_INCIDENT_SPAN_SECONDS in user_input:
                raw_span = user_input[CONF_MAX_INCIDENT_SPAN_SECONDS]
                if not isinstance(raw_span, (int, float)) or not (10.0 <= float(raw_span) <= 3600.0):
                    errors[CONF_MAX_INCIDENT_SPAN_SECONDS] = "invalid_max_incident_span"

            if not errors:
                return self.async_create_entry(
                    title="", data={**self._current_options(), **user_input}
                )

        entry = self.config_entry
        current_chunk = entry.options.get(
            CONF_CHUNK_SECONDS,
            entry.data.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS),
        )
        current_parallel = entry.options.get(
            CONF_MAX_PARALLEL_RESOURCES,
            entry.data.get(CONF_MAX_PARALLEL_RESOURCES, DEFAULT_MAX_PARALLEL_RESOURCES),
        )
        current_queue_depth = entry.options.get(
            CONF_MAX_QUEUE_DEPTH,
            entry.data.get(CONF_MAX_QUEUE_DEPTH, DEFAULT_MAX_QUEUE_DEPTH),
        )
        current_queue_timeout = entry.options.get(
            CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
            entry.data.get(
                CONF_QUEUE_WAIT_TIMEOUT_SECONDS, DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS
            ),
        )
        current_enable_auto = entry.options.get(
            CONF_ENABLE_AUTO_ASSOCIATION,
            entry.data.get(CONF_ENABLE_AUTO_ASSOCIATION, DEFAULT_ENABLE_AUTO_ASSOCIATION),
        )
        current_auto_tol = entry.options.get(
            CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
            entry.data.get(
                CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
            ),
        )
        current_max_span = entry.options.get(
            CONF_MAX_INCIDENT_SPAN_SECONDS,
            entry.data.get(
                CONF_MAX_INCIDENT_SPAN_SECONDS,
                DEFAULT_MAX_INCIDENT_SPAN_SECONDS,
            ),
        )

        schema = vol.Schema(
            {
                vol.Required(CONF_CHUNK_SECONDS, default=current_chunk): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=15)
                ),
                vol.Required(CONF_MAX_PARALLEL_RESOURCES, default=current_parallel): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=8)
                ),
                vol.Required(CONF_MAX_QUEUE_DEPTH, default=current_queue_depth): vol.All(
                    vol.Coerce(int), vol.Range(min=1, max=256)
                ),
                vol.Required(
                    CONF_QUEUE_WAIT_TIMEOUT_SECONDS, default=current_queue_timeout
                ): vol.All(vol.Coerce(int), vol.Range(min=1, max=300)),
                vol.Required(CONF_ENABLE_AUTO_ASSOCIATION, default=bool(current_enable_auto)): bool,
                vol.Required(
                    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS, default=float(current_auto_tol)
                ): vol.All(vol.Coerce(float), vol.Range(min=1.0, max=60.0)),
                vol.Required(
                    CONF_MAX_INCIDENT_SPAN_SECONDS, default=float(current_max_span)
                ): vol.All(vol.Coerce(float), vol.Range(min=10.0, max=3600.0)),
            }
        )

        return self.async_show_form(
            step_id="recording_settings",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_storage_settings(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Manage global storage retention and disk floor settings."""
        errors: dict[str, str] = {}

        if user_input is not None:
            min_disk = user_input.get(CONF_MIN_DISK_FREE_MB, DEFAULT_MIN_DISK_FREE_MB)
            if min_disk < 50:
                errors[CONF_MIN_DISK_FREE_MB] = "min_disk_free_too_low"
            retention_size = user_input.get(
                CONF_RETENTION_SIZE_MB, DEFAULT_RETENTION_SIZE_MB
            )
            if retention_size < 50:
                errors[CONF_RETENTION_SIZE_MB] = "retention_size_too_low"
            retention_days = user_input.get(
                CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS
            )
            if retention_days < 1:
                errors[CONF_RETENTION_DAYS] = "retention_days_too_low"

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
        current_min_disk = entry.options.get(
            CONF_MIN_DISK_FREE_MB,
            entry.data.get(CONF_MIN_DISK_FREE_MB, DEFAULT_MIN_DISK_FREE_MB),
        )

        schema = vol.Schema(
            {
                vol.Required(CONF_RETENTION_SIZE_MB, default=current_size): vol.All(
                    vol.Coerce(int), vol.Range(min=50)
                ),
                vol.Required(CONF_RETENTION_DAYS, default=current_days): vol.All(
                    vol.Coerce(int), vol.Range(min=1)
                ),
                vol.Required(
                    CONF_MIN_DISK_FREE_MB, default=max(50, current_min_disk)
                ): vol.All(vol.Coerce(int), vol.Range(min=50, max=10240)),
            }
        )

        return self.async_show_form(
            step_id="storage_settings",
            data_schema=schema,
            errors=errors,
        )

    async def async_step_effective_config(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Display read-only effective configuration and operational formulas."""
        if user_input is not None:
            return self.async_create_entry(title="", data=self._current_options())

        entry = self.config_entry
        config_root = Path(self.hass.config.path()).resolve()
        raw_media = str(
            entry.options.get(
                CONF_MEDIA_ROOT,
                entry.data.get(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR),
            )
        )
        resolved_media = (
            Path(raw_media) if Path(raw_media).is_absolute() else (config_root / raw_media)
        ).resolve()

        chunk_seconds = int(
            entry.options.get(
                CONF_CHUNK_SECONDS,
                entry.data.get(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS),
            )
        )
        max_parallel = int(
            entry.options.get(
                CONF_MAX_PARALLEL_RESOURCES,
                entry.data.get(
                    CONF_MAX_PARALLEL_RESOURCES, DEFAULT_MAX_PARALLEL_RESOURCES
                ),
            )
        )
        max_queue = int(
            entry.options.get(
                CONF_MAX_QUEUE_DEPTH,
                entry.data.get(CONF_MAX_QUEUE_DEPTH, DEFAULT_MAX_QUEUE_DEPTH),
            )
        )
        queue_timeout = int(
            entry.options.get(
                CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
                entry.data.get(
                    CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
                    DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS,
                ),
            )
        )
        retention_size = int(
            entry.options.get(
                CONF_RETENTION_SIZE_MB,
                entry.data.get(CONF_RETENTION_SIZE_MB, DEFAULT_RETENTION_SIZE_MB),
            )
        )
        retention_days = int(
            entry.options.get(
                CONF_RETENTION_DAYS,
                entry.data.get(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS),
            )
        )
        min_disk = int(
            entry.options.get(
                CONF_MIN_DISK_FREE_MB,
                entry.data.get(CONF_MIN_DISK_FREE_MB, DEFAULT_MIN_DISK_FREE_MB),
            )
        )
        profiles = self._profiles()

        enable_auto = bool(
            entry.options.get(
                CONF_ENABLE_AUTO_ASSOCIATION,
                entry.data.get(CONF_ENABLE_AUTO_ASSOCIATION, DEFAULT_ENABLE_AUTO_ASSOCIATION),
            )
        )
        tolerance = float(
            entry.options.get(
                CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                entry.data.get(
                    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                    DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                ),
            )
        )
        max_span = float(
            entry.options.get(
                CONF_MAX_INCIDENT_SPAN_SECONDS,
                entry.data.get(
                    CONF_MAX_INCIDENT_SPAN_SECONDS,
                    DEFAULT_MAX_INCIDENT_SPAN_SECONDS,
                ),
            )
        )
        auto_assoc_str = (
            f"enabled (tolerance: {tolerance:.1f}s, max_span: {max_span:.1f}s)"
            if enable_auto
            else f"disabled (default) (tolerance: {tolerance:.1f}s, max_span: {max_span:.1f}s)"
        )

        description_placeholders = {
            "media_root": str(resolved_media),
            "precedence": "action > camera_profile > entry_defaults",
            "rate_estimate": "1.0 MiB/s (8 Mbps nominal stream)",
            "watchdog_formula": f"chunk_seconds + lookback_seconds + {DEFAULT_WATCHDOG_PADDING_SECONDS:.1f}s",
            "hard_cap_behavior": "stop (fail-closed, writers quarantined)",
            "chunk_seconds": str(chunk_seconds),
            "max_parallel": str(max_parallel),
            "max_queue_depth": str(max_queue),
            "queue_timeout": str(queue_timeout),
            "retention_size_mb": str(retention_size),
            "retention_days": str(retention_days),
            "min_disk_free_mb": str(min_disk),
            "profiles_count": str(len(profiles)),
            "auto_association": auto_assoc_str,
        }

        return self.async_show_form(
            step_id="effective_config",
            data_schema=vol.Schema({}),
            description_placeholders=description_placeholders,
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
        mode_choices = [
            {"value": MODE_EXTEND, "label": "Extend (continuous chunked)"},
            {"value": MODE_NATIVE_FIXED, "label": "Native Fixed Clip"},
            {"value": MODE_NATIVE_SNAPSHOT, "label": "Native Snapshot"},
            {"value": MODE_BUFFERED, "label": "Buffered Provider Export (Frigate NVR)"},
        ]
        schema.update(
            {
                vol.Required("source_id", default=profile.get("source_id", "camera")): str,
                vol.Required("label", default=profile.get("label", "Camera")): str,
                vol.Required(
                    "recording_mode", default=profile.get("recording_mode", MODE_EXTEND)
                ): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=mode_choices, mode=selector.SelectSelectorMode.DROPDOWN
                    )
                ),
                vol.Optional(
                    "ownership_group", default=profile.get("ownership_group") or ""
                ): str,
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
                vol.Optional(
                    CONF_FRIGATE_BASE_URL, default=profile.get(CONF_FRIGATE_BASE_URL) or ""
                ): str,
                vol.Optional(
                    CONF_FRIGATE_CAMERA_NAME,
                    default=profile.get(CONF_FRIGATE_CAMERA_NAME) or "",
                ): str,
                vol.Optional(
                    CONF_FRIGATE_AUTH_TOKEN, default=profile.get(CONF_FRIGATE_AUTH_TOKEN) or ""
                ): str,
                vol.Optional(
                    CONF_EXPORT_DELAY_SECONDS,
                    default=profile.get(
                        CONF_EXPORT_DELAY_SECONDS, DEFAULT_EXPORT_DELAY_SECONDS
                    ),
                ): _IntegerNumberSelector(
                    selector.NumberSelectorConfig(
                        min=0, max=60, step=1, mode=selector.NumberSelectorMode.BOX
                    )
                ),
                vol.Optional(
                    "correlation_group", default=profile.get("correlation_group") or ""
                ): str,
                vol.Optional(
                    "zone", default=profile.get("zone") or ""
                ): str,
                vol.Optional(
                    "adjacent_zones",
                    default=", ".join(profile.get("adjacent_zones") or [])
                    if isinstance(profile.get("adjacent_zones"), (list, tuple))
                    else (profile.get("adjacent_zones") or ""),
                ): str,
                vol.Optional(
                    "source_clock_offset_seconds",
                    default=float(profile.get("source_clock_offset_seconds", 0.0)),
                ): vol.All(vol.Coerce(float), vol.Range(min=-3600.0, max=3600.0)),
                vol.Optional(
                    "source_time_uncertainty_seconds",
                    default=float(profile.get("source_time_uncertainty_seconds", 0.0)),
                ): vol.All(vol.Coerce(float), vol.Range(min=0.0, max=300.0)),
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
    def _profile_from_input(
        user_input: dict[str, Any], defaults: dict[str, Any]
    ) -> dict[str, Any]:
        profile = {
            key: user_input.get(key, defaults.get(key))
            for key in (
                "source_id",
                "label",
                "recording_mode",
                "window_seconds",
                "lookback_seconds",
                "chunk_seconds",
                "max_window_seconds",
            )
        }
        raw_group = user_input.get("ownership_group", defaults.get("ownership_group"))
        if isinstance(raw_group, str):
            stripped = raw_group.strip()
            profile["ownership_group"] = stripped if stripped else None
        else:
            profile["ownership_group"] = raw_group

        for field in (
            "window_seconds",
            "lookback_seconds",
            "chunk_seconds",
            "max_window_seconds",
        ):
            value = profile[field]
            if isinstance(value, float) and math.isfinite(value) and value.is_integer():
                profile[field] = int(value)

        raw_url = user_input.get(CONF_FRIGATE_BASE_URL, defaults.get(CONF_FRIGATE_BASE_URL))
        profile[CONF_FRIGATE_BASE_URL] = (
            raw_url.strip() if isinstance(raw_url, str) and raw_url.strip() else None
        )

        raw_cam = user_input.get(
            CONF_FRIGATE_CAMERA_NAME, defaults.get(CONF_FRIGATE_CAMERA_NAME)
        )
        profile[CONF_FRIGATE_CAMERA_NAME] = (
            raw_cam.strip() if isinstance(raw_cam, str) and raw_cam.strip() else None
        )

        raw_token = user_input.get(
            CONF_FRIGATE_AUTH_TOKEN, defaults.get(CONF_FRIGATE_AUTH_TOKEN)
        )
        profile[CONF_FRIGATE_AUTH_TOKEN] = (
            raw_token.strip() if isinstance(raw_token, str) and raw_token.strip() else None
        )

        export_delay = user_input.get(
            CONF_EXPORT_DELAY_SECONDS,
            defaults.get(CONF_EXPORT_DELAY_SECONDS, DEFAULT_EXPORT_DELAY_SECONDS),
        )
        if (
            isinstance(export_delay, float)
            and math.isfinite(export_delay)
            and export_delay.is_integer()
        ):
            export_delay = int(export_delay)
        profile[CONF_EXPORT_DELAY_SECONDS] = (
            export_delay if isinstance(export_delay, int) else DEFAULT_EXPORT_DELAY_SECONDS
        )

        profile["hard_cap_behavior"] = "stop"

        raw_corr = user_input.get("correlation_group", defaults.get("correlation_group"))
        profile["correlation_group"] = (
            raw_corr.strip() if isinstance(raw_corr, str) and raw_corr.strip() else None
        )

        raw_zone = user_input.get("zone", defaults.get("zone"))
        profile["zone"] = (
            raw_zone.strip() if isinstance(raw_zone, str) and raw_zone.strip() else None
        )

        raw_adj = user_input.get("adjacent_zones", defaults.get("adjacent_zones"))
        if isinstance(raw_adj, str):
            profile["adjacent_zones"] = [z.strip() for z in raw_adj.split(",") if z.strip()]
        elif isinstance(raw_adj, (list, tuple)):
            profile["adjacent_zones"] = [str(z).strip() for z in raw_adj if str(z).strip()]
        else:
            profile["adjacent_zones"] = []

        raw_offset = user_input.get(
            "source_clock_offset_seconds", defaults.get("source_clock_offset_seconds", 0.0)
        )
        try:
            profile["source_clock_offset_seconds"] = max(-3600.0, min(3600.0, float(raw_offset)))
        except (TypeError, ValueError):
            profile["source_clock_offset_seconds"] = 0.0

        raw_unc = user_input.get(
            "source_time_uncertainty_seconds", defaults.get("source_time_uncertainty_seconds", 0.0)
        )
        try:
            profile["source_time_uncertainty_seconds"] = max(0.0, min(300.0, float(raw_unc)))
        except (TypeError, ValueError):
            profile["source_time_uncertainty_seconds"] = 0.0

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
