"""Config flow for Event Recording integration."""

from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol

from homeassistant import config_entries, data_entry_flow
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers import entity_registry, selector

from .camera_profiles import (
    CAMERA_PROFILES,
    default_camera_profile,
    validate_camera_profile,
    validate_go2rtc_base_url,
)
from .const import (
    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    CONF_CHUNK_SECONDS,
    CONF_ENABLE_AUTO_ASSOCIATION,
    CONF_ENABLE_DEBUG_LOGGING,
    CONF_EXPORT_DELAY_SECONDS,
    CONF_FRIGATE_AUTH_TOKEN,
    CONF_FRIGATE_BASE_URL,
    CONF_FRIGATE_CAMERA_NAME,
    CONF_GO2RTC_BASE_URL,
    CONF_GO2RTC_PASSWORD,
    CONF_GO2RTC_STREAM,
    CONF_GO2RTC_USERNAME,
    CONF_MAX_INCIDENT_SPAN_SECONDS,
    CONF_MAX_PARALLEL_RESOURCES,
    CONF_MAX_QUEUE_DEPTH,
    CONF_MAX_WINDOW_SECONDS,
    CONF_MEDIA_ROOT,
    CONF_MIN_DISK_FREE_MB,
    CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
    CONF_RECORDING_PROVIDER,
    CONF_RETENTION_DAYS,
    CONF_RETENTION_SIZE_MB,
    DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    DEFAULT_CHUNK_SECONDS,
    DEFAULT_ENABLE_AUTO_ASSOCIATION,
    DEFAULT_ENABLE_DEBUG_LOGGING,
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
    PROVIDER_GO2RTC,
    PROVIDER_HOME_ASSISTANT,
)
from .core.validation import (
    SPEC_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
    SPEC_CHUNK_SECONDS,
    SPEC_EXPORT_DELAY_SECONDS,
    SPEC_LOOKBACK_SECONDS,
    SPEC_MAX_INCIDENT_SPAN_SECONDS,
    SPEC_MAX_PARALLEL_RESOURCES,
    SPEC_MAX_QUEUE_DEPTH,
    SPEC_MAX_WINDOW_SECONDS,
    SPEC_MIN_DISK_FREE_MB,
    SPEC_QUEUE_WAIT_TIMEOUT_SECONDS,
    SPEC_RETENTION_DAYS,
    SPEC_RETENTION_SIZE_MB,
    SPEC_SOURCE_CLOCK_OFFSET_SECONDS,
    SPEC_SOURCE_TIME_UNCERTAINTY_SECONDS,
    SPEC_WINDOW_SECONDS,
    ValidationError,
)
from .validation import number_spec_selector

# Native Home Assistant form section ids. Sections are presentation only: all
# values are stored as flat option keys, never nested under these ids.
SECTION_CONNECTION = "connection"
SECTION_ADVANCED = "advanced"
SECTION_CORRELATION = "correlation"
SECTION_TIMING = "timing"
SECTION_PROVIDER = "provider"

_GLOBAL_SECTION_IDS = frozenset({SECTION_CONNECTION, SECTION_ADVANCED, SECTION_CORRELATION})
_CAMERA_SECTION_IDS = frozenset({SECTION_TIMING, SECTION_PROVIDER, SECTION_CORRELATION})

# Map submitted fields to the section that renders them, so a validation error
# can re-open the section that holds the invalid field.
_GLOBAL_FIELD_SECTIONS = {
    CONF_GO2RTC_BASE_URL: SECTION_CONNECTION,
    CONF_GO2RTC_USERNAME: SECTION_CONNECTION,
    CONF_GO2RTC_PASSWORD: SECTION_CONNECTION,
    "clear_go2rtc_password": SECTION_CONNECTION,
    CONF_MAX_PARALLEL_RESOURCES: SECTION_ADVANCED,
    CONF_MAX_QUEUE_DEPTH: SECTION_ADVANCED,
    CONF_QUEUE_WAIT_TIMEOUT_SECONDS: SECTION_ADVANCED,
    CONF_ENABLE_DEBUG_LOGGING: SECTION_ADVANCED,
    CONF_ENABLE_AUTO_ASSOCIATION: SECTION_CORRELATION,
    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS: SECTION_CORRELATION,
    CONF_MAX_INCIDENT_SPAN_SECONDS: SECTION_CORRELATION,
}

_CAMERA_FIELD_SECTIONS = {
    "lookback_seconds": SECTION_TIMING,
    "chunk_seconds": SECTION_TIMING,
    CONF_GO2RTC_STREAM: SECTION_PROVIDER,
    CONF_FRIGATE_BASE_URL: SECTION_PROVIDER,
    CONF_FRIGATE_CAMERA_NAME: SECTION_PROVIDER,
    CONF_FRIGATE_AUTH_TOKEN: SECTION_PROVIDER,
    "clear_frigate_auth_token": SECTION_PROVIDER,
    CONF_EXPORT_DELAY_SECONDS: SECTION_PROVIDER,
    "ownership_group": SECTION_CORRELATION,
    "correlation_group": SECTION_CORRELATION,
    "zone": SECTION_CORRELATION,
    "adjacent_zones": SECTION_CORRELATION,
    "source_clock_offset_seconds": SECTION_CORRELATION,
    "source_time_uncertainty_seconds": SECTION_CORRELATION,
}

_CAMERA_FORM_FIELDS = frozenset(
    {
        "camera_entity",
        "source_id",
        "label",
        "recording_mode",
        CONF_RECORDING_PROVIDER,
        "window_seconds",
        "max_window_seconds",
        *_CAMERA_FIELD_SECTIONS,
    }
)


def _form_number(value: Any, default: Any) -> Any:
    """Return a form default without discarding an attempted invalid value."""
    if value is None or value == "" or isinstance(value, bool):
        return default
    if isinstance(value, float) and not math.isfinite(value):
        return default
    return value


def _flatten_sections(
    user_input: Mapping[str, Any], section_ids: frozenset[str]
) -> dict[str, Any]:
    """Flatten one level of native form sections into plain option keys."""
    flat: dict[str, Any] = {}
    for key, value in user_input.items():
        if key in section_ids and isinstance(value, Mapping):
            flat.update(value)
        else:
            flat[key] = value
    return flat


def _sections_with_errors(
    errors: Mapping[str, str], field_sections: Mapping[str, str]
) -> set[str]:
    """Return the sections that should start expanded for the given errors."""
    return {
        field_sections[field]
        for field in errors
        if field in field_sections
    }


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

            # 2. Validate numeric parameters against shared declarative specs
            for field, spec, err_key in (
                (CONF_RETENTION_SIZE_MB, SPEC_RETENTION_SIZE_MB, "retention_size_too_low"),
                (CONF_RETENTION_DAYS, SPEC_RETENTION_DAYS, "retention_days_too_low"),
                (CONF_CHUNK_SECONDS, SPEC_CHUNK_SECONDS, "invalid_chunk_seconds"),
            ):
                try:
                    user_input[field] = int(spec.validate(user_input.get(field, spec.default)))
                except (ValidationError, ValueError, TypeError):
                    errors[field] = err_key

            if not errors:
                return self.async_create_entry(
                    title="Event Recording",
                    data=user_input,
                )

        # Re-render the form with the attempted values (never a credential echo).
        displayed = dict(user_input) if user_input is not None else {}
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_MEDIA_ROOT,
                    default=str(displayed.get(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR)),
                ): str,
                vol.Required(
                    CONF_RETENTION_SIZE_MB,
                    default=_form_number(
                        displayed.get(CONF_RETENTION_SIZE_MB),
                        DEFAULT_RETENTION_SIZE_MB,
                    ),
                ): number_spec_selector(SPEC_RETENTION_SIZE_MB),
                vol.Required(
                    CONF_RETENTION_DAYS,
                    default=_form_number(
                        displayed.get(CONF_RETENTION_DAYS), DEFAULT_RETENTION_DAYS
                    ),
                ): number_spec_selector(SPEC_RETENTION_DAYS),
                vol.Required(
                    CONF_CHUNK_SECONDS,
                    default=_form_number(
                        displayed.get(CONF_CHUNK_SECONDS), DEFAULT_CHUNK_SECONDS
                    ),
                ): number_spec_selector(SPEC_CHUNK_SECONDS),
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
        """Show the options menu with distinct actions available directly."""
        return self.async_show_menu(
            step_id="init",
            menu_options=[
                "settings",
                "camera_add",
                "camera_edit",
                "camera_remove",
                "effective_config",
            ],
        )

    async def async_step_settings(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Review and save every global recording setting in a single form."""
        current = self._global_current()
        errors: dict[str, str] = {}
        submitted: dict[str, Any] = {}

        if user_input is not None:
            submitted = _flatten_sections(user_input, _GLOBAL_SECTION_IDS)
            values: dict[str, Any] = {}

            for field, spec, err_key in (
                (CONF_CHUNK_SECONDS, SPEC_CHUNK_SECONDS, "invalid_chunk_seconds"),
                (CONF_RETENTION_SIZE_MB, SPEC_RETENTION_SIZE_MB, "retention_size_too_low"),
                (CONF_RETENTION_DAYS, SPEC_RETENTION_DAYS, "retention_days_too_low"),
                (CONF_MIN_DISK_FREE_MB, SPEC_MIN_DISK_FREE_MB, "min_disk_free_too_low"),
                (
                    CONF_MAX_PARALLEL_RESOURCES,
                    SPEC_MAX_PARALLEL_RESOURCES,
                    "invalid_max_parallel",
                ),
                (CONF_MAX_QUEUE_DEPTH, SPEC_MAX_QUEUE_DEPTH, "invalid_max_queue_depth"),
                (
                    CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
                    SPEC_QUEUE_WAIT_TIMEOUT_SECONDS,
                    "invalid_queue_timeout",
                ),
            ):
                try:
                    values[field] = int(spec.validate(submitted.get(field, current[field])))
                except (ValidationError, ValueError, TypeError):
                    errors[field] = err_key

            for field, spec, err_key in (
                (
                    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                    SPEC_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                    "invalid_tolerance",
                ),
                (
                    CONF_MAX_INCIDENT_SPAN_SECONDS,
                    SPEC_MAX_INCIDENT_SPAN_SECONDS,
                    "invalid_max_incident_span",
                ),
            ):
                try:
                    values[field] = float(spec.validate(submitted.get(field, current[field])))
                except (ValidationError, ValueError, TypeError):
                    errors[field] = err_key

            for flag in (CONF_ENABLE_AUTO_ASSOCIATION, CONF_ENABLE_DEBUG_LOGGING):
                values[flag] = bool(submitted.get(flag, current[flag]))

            raw_url = str(
                submitted.get(CONF_GO2RTC_BASE_URL, current[CONF_GO2RTC_BASE_URL]) or ""
            ).strip()
            username = str(
                submitted.get(CONF_GO2RTC_USERNAME, current[CONF_GO2RTC_USERNAME]) or ""
            ).strip()
            password_value = submitted.get(CONF_GO2RTC_PASSWORD, "")
            password = password_value if isinstance(password_value, str) else ""
            clear_password = bool(submitted.get("clear_go2rtc_password", False))
            saved_password = current[CONF_GO2RTC_PASSWORD]
            effective_password = None if clear_password else (password or saved_password)

            if raw_url:
                try:
                    values[CONF_GO2RTC_BASE_URL] = validate_go2rtc_base_url(raw_url)
                except ValueError:
                    errors[CONF_GO2RTC_BASE_URL] = "invalid_go2rtc_base_url"
            else:
                values[CONF_GO2RTC_BASE_URL] = None
            if bool(username) != bool(effective_password):
                errors[CONF_GO2RTC_USERNAME] = "go2rtc_credentials_incomplete"
            values[CONF_GO2RTC_USERNAME] = username or None
            if clear_password:
                values[CONF_GO2RTC_PASSWORD] = None
            elif password:
                values[CONF_GO2RTC_PASSWORD] = password
            else:
                values[CONF_GO2RTC_PASSWORD] = saved_password

            if not errors:
                options = self._current_options()
                options.update(values)
                return self.async_create_entry(title="", data=options)

        displayed = {**current, **submitted}
        # Never echo a stored or attempted secret back into the form.
        displayed[CONF_GO2RTC_PASSWORD] = ""
        schema = self._settings_schema(
            displayed, _sections_with_errors(errors, _GLOBAL_FIELD_SECTIONS)
        )

        return self.async_show_form(
            step_id="settings",
            data_schema=schema,
            errors=errors,
            description_placeholders=self._summary_placeholders(displayed),
        )

    @staticmethod
    def _settings_schema(displayed: dict[str, Any], expanded: set[str]) -> Any:
        """Build the single global settings form with collapsed advanced groups."""
        connection = vol.Schema(
            {
                vol.Optional(
                    CONF_GO2RTC_BASE_URL, default=str(displayed.get(CONF_GO2RTC_BASE_URL) or "")
                ): str,
                vol.Optional(
                    CONF_GO2RTC_USERNAME, default=str(displayed.get(CONF_GO2RTC_USERNAME) or "")
                ): str,
                vol.Optional(CONF_GO2RTC_PASSWORD, default=""): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional("clear_go2rtc_password", default=False): selector.BooleanSelector(),
            }
        )
        advanced = vol.Schema(
            {
                vol.Required(
                    CONF_MAX_PARALLEL_RESOURCES,
                    default=displayed[CONF_MAX_PARALLEL_RESOURCES],
                ): number_spec_selector(SPEC_MAX_PARALLEL_RESOURCES),
                vol.Required(
                    CONF_MAX_QUEUE_DEPTH, default=displayed[CONF_MAX_QUEUE_DEPTH]
                ): number_spec_selector(SPEC_MAX_QUEUE_DEPTH),
                vol.Required(
                    CONF_QUEUE_WAIT_TIMEOUT_SECONDS,
                    default=displayed[CONF_QUEUE_WAIT_TIMEOUT_SECONDS],
                ): number_spec_selector(SPEC_QUEUE_WAIT_TIMEOUT_SECONDS),
                vol.Required(
                    CONF_ENABLE_DEBUG_LOGGING,
                    default=bool(displayed[CONF_ENABLE_DEBUG_LOGGING]),
                ): selector.BooleanSelector(),
            }
        )
        correlation = vol.Schema(
            {
                vol.Required(
                    CONF_ENABLE_AUTO_ASSOCIATION,
                    default=bool(displayed[CONF_ENABLE_AUTO_ASSOCIATION]),
                ): selector.BooleanSelector(),
                vol.Required(
                    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                    default=float(displayed[CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS]),
                ): number_spec_selector(SPEC_AUTO_ASSOCIATION_TOLERANCE_SECONDS, step=1.0),
                vol.Required(
                    CONF_MAX_INCIDENT_SPAN_SECONDS,
                    default=float(displayed[CONF_MAX_INCIDENT_SPAN_SECONDS]),
                ): number_spec_selector(SPEC_MAX_INCIDENT_SPAN_SECONDS, step=10.0),
            }
        )
        return vol.Schema(
            {
                vol.Required(
                    CONF_CHUNK_SECONDS, default=displayed[CONF_CHUNK_SECONDS]
                ): number_spec_selector(SPEC_CHUNK_SECONDS),
                vol.Required(
                    CONF_RETENTION_SIZE_MB, default=displayed[CONF_RETENTION_SIZE_MB]
                ): number_spec_selector(SPEC_RETENTION_SIZE_MB),
                vol.Required(
                    CONF_RETENTION_DAYS, default=displayed[CONF_RETENTION_DAYS]
                ): number_spec_selector(SPEC_RETENTION_DAYS),
                vol.Required(
                    CONF_MIN_DISK_FREE_MB, default=displayed[CONF_MIN_DISK_FREE_MB]
                ): number_spec_selector(SPEC_MIN_DISK_FREE_MB),
                vol.Required(SECTION_CONNECTION): data_entry_flow.section(
                    connection, {"collapsed": SECTION_CONNECTION not in expanded}
                ),
                vol.Required(SECTION_ADVANCED): data_entry_flow.section(
                    advanced, {"collapsed": SECTION_ADVANCED not in expanded}
                ),
                vol.Required(SECTION_CORRELATION): data_entry_flow.section(
                    correlation, {"collapsed": SECTION_CORRELATION not in expanded}
                ),
            }
        )

    def _summary_placeholders(self, displayed: Mapping[str, Any]) -> dict[str, str]:
        """Return a concise, secret-free summary for the settings description."""
        return {
            "media_root": self._resolved_media_root(),
            "summary": (
                f"{displayed[CONF_CHUNK_SECONDS]}s chunks, "
                f"{displayed[CONF_RETENTION_SIZE_MB]} MB / "
                f"{displayed[CONF_RETENTION_DAYS]} days retention, "
                f"{len(self._profiles())} camera profile(s)"
            ),
        }

    def _global_current(self) -> dict[str, Any]:
        """Return current global settings, falling back to legacy entry data."""
        entry = self.config_entry

        def stored(key: str, default: Any) -> Any:
            return entry.options.get(key, entry.data.get(key, default))

        return {
            CONF_CHUNK_SECONDS: int(stored(CONF_CHUNK_SECONDS, DEFAULT_CHUNK_SECONDS)),
            CONF_RETENTION_SIZE_MB: int(
                stored(CONF_RETENTION_SIZE_MB, DEFAULT_RETENTION_SIZE_MB)
            ),
            CONF_RETENTION_DAYS: int(stored(CONF_RETENTION_DAYS, DEFAULT_RETENTION_DAYS)),
            CONF_MIN_DISK_FREE_MB: int(
                stored(CONF_MIN_DISK_FREE_MB, DEFAULT_MIN_DISK_FREE_MB)
            ),
            CONF_MAX_PARALLEL_RESOURCES: int(
                stored(CONF_MAX_PARALLEL_RESOURCES, DEFAULT_MAX_PARALLEL_RESOURCES)
            ),
            CONF_MAX_QUEUE_DEPTH: int(
                stored(CONF_MAX_QUEUE_DEPTH, DEFAULT_MAX_QUEUE_DEPTH)
            ),
            CONF_QUEUE_WAIT_TIMEOUT_SECONDS: int(
                stored(CONF_QUEUE_WAIT_TIMEOUT_SECONDS, DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS)
            ),
            CONF_ENABLE_AUTO_ASSOCIATION: bool(
                stored(CONF_ENABLE_AUTO_ASSOCIATION, DEFAULT_ENABLE_AUTO_ASSOCIATION)
            ),
            CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS: float(
                stored(
                    CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                    DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS,
                )
            ),
            CONF_MAX_INCIDENT_SPAN_SECONDS: float(
                stored(CONF_MAX_INCIDENT_SPAN_SECONDS, DEFAULT_MAX_INCIDENT_SPAN_SECONDS)
            ),
            CONF_ENABLE_DEBUG_LOGGING: bool(
                stored(CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING)
            ),
            CONF_GO2RTC_BASE_URL: stored(CONF_GO2RTC_BASE_URL, "") or "",
            CONF_GO2RTC_USERNAME: stored(CONF_GO2RTC_USERNAME, "") or "",
            CONF_GO2RTC_PASSWORD: stored(CONF_GO2RTC_PASSWORD, None),
        }

    def _resolved_media_root(self) -> str:
        """Return the read-only media root for the summary (never editable here)."""
        entry = self.config_entry
        raw_media = str(
            entry.options.get(
                CONF_MEDIA_ROOT,
                entry.data.get(CONF_MEDIA_ROOT, DEFAULT_MEDIA_DIR),
            )
        )
        config_root = Path(self.hass.config.path()).resolve()
        return str(
            (
                Path(raw_media) if Path(raw_media).is_absolute() else config_root / raw_media
            ).resolve()
        )

    async def async_step_effective_config(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Display a read-only configuration summary; saving keeps options as-is."""
        if user_input is not None:
            return self.async_create_entry(title="", data=self._current_options())

        current = self._global_current()
        profiles = self._profiles()
        auto_assoc_str = (
            f"enabled (tolerance: {current[CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS]:.1f}s, "
            f"max_span: {current[CONF_MAX_INCIDENT_SPAN_SECONDS]:.1f}s)"
            if current[CONF_ENABLE_AUTO_ASSOCIATION]
            else "disabled (default)"
        )

        description_placeholders = {
            "media_root": self._resolved_media_root(),
            "precedence": "action > camera_profile > entry_defaults",
            "rate_estimate": "1.0 MiB/s (8 Mbps nominal stream)",
            "watchdog_formula": f"chunk_seconds + lookback_seconds + {DEFAULT_WATCHDOG_PADDING_SECONDS:.1f}s",
            "hard_cap_behavior": "stop (fail-closed, writers quarantined)",
            "chunk_seconds": str(current[CONF_CHUNK_SECONDS]),
            "max_parallel": str(current[CONF_MAX_PARALLEL_RESOURCES]),
            "max_queue_depth": str(current[CONF_MAX_QUEUE_DEPTH]),
            "queue_timeout": str(current[CONF_QUEUE_WAIT_TIMEOUT_SECONDS]),
            "retention_size_mb": str(current[CONF_RETENTION_SIZE_MB]),
            "retention_days": str(current[CONF_RETENTION_DAYS]),
            "min_disk_free_mb": str(current[CONF_MIN_DISK_FREE_MB]),
            "profiles_count": str(len(profiles)),
            "auto_association": auto_assoc_str,
        }

        return self.async_show_form(
            step_id="effective_config",
            data_schema=vol.Schema({}),
            description_placeholders=description_placeholders,
        )

    async def async_step_camera_add(self, user_input: dict[str, Any] | None = None) -> FlowResult:
        """Add a camera profile from a single form with grouped optional controls."""
        errors: dict[str, str] = {}
        submitted: dict[str, Any] = {}
        entity_id = ""

        if user_input is not None:
            submitted = _flatten_sections(user_input, _CAMERA_SECTION_IDS)
            entity_id = str(submitted.get("camera_entity", "")).strip()
            defaults = self._camera_add_defaults(entity_id)
            attempt = dict(submitted)
            # Omitted identity fields are derived from the selected camera entity.
            if not str(attempt.get("source_id", "")).strip():
                attempt["source_id"] = defaults["source_id"]
            if not str(attempt.get("label", "")).strip():
                attempt["label"] = defaults["label"]
            profile = self._profile_from_input(attempt, defaults)
            errors.update(await self._validate_camera_entity(entity_id))
            if entity_id in self._profiles():
                errors["camera_entity"] = "camera_profile_exists"
            errors.update(validate_camera_profile(entity_id, profile))
            if not errors:
                profiles = self._profiles()
                if entity_id in profiles:
                    return self.async_abort(reason="camera_profile_missing")
                profiles[entity_id] = profile
                return self._save_profiles(profiles)

        defaults = self._camera_add_defaults(entity_id)
        displayed = {**defaults, **submitted}
        if user_input is None:
            displayed["source_id"] = ""
            displayed["label"] = ""
        else:
            displayed["source_id"] = str(submitted.get("source_id", "") or "")
            displayed["label"] = str(submitted.get("label", "") or "")
        errors = self._finalize_camera_errors(errors)

        return self.async_show_form(
            step_id="camera_add",
            data_schema=self._profile_schema(
                displayed,
                include_entity=True,
                entity_default=entity_id or None,
                expanded=_sections_with_errors(errors, _CAMERA_FIELD_SECTIONS),
            ),
            errors=errors,
            description_placeholders={
                "camera_name": self._camera_name(entity_id) if entity_id else ""
            },
        )

    def _camera_add_defaults(self, entity_id: str) -> dict[str, Any]:
        """Return profile defaults, using global timing settings for this camera."""
        return default_camera_profile(
            entity_id or "camera.",
            name=self._camera_name(entity_id) if entity_id else None,
            chunk_seconds=self._global_chunk_seconds(),
            max_window_seconds=self._global_max_window_seconds(),
        )

    @staticmethod
    def _finalize_camera_errors(errors: dict[str, str]) -> dict[str, str]:
        """Keep profile errors visible even when they target a hidden field."""
        if any(field not in _CAMERA_FORM_FIELDS for field in errors):
            errors.setdefault("base", "invalid_profile")
        return errors

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
        """Edit one stored camera profile in a single form."""
        profiles = self._profiles()
        original_entity = getattr(self, "_edit_camera_entity", None)
        if original_entity not in profiles:
            return self.async_abort(reason="camera_profile_missing")
        old_profile = profiles[original_entity]
        current = {**default_camera_profile(original_entity), **old_profile}
        errors: dict[str, str] = {}
        submitted: dict[str, Any] = {}
        entity_default = original_entity

        if user_input is not None:
            submitted = _flatten_sections(user_input, _CAMERA_SECTION_IDS)
            new_entity = str(submitted.get("camera_entity", original_entity)).strip()
            entity_default = new_entity or original_entity
            errors.update(await self._validate_camera_entity(entity_default))
            if new_entity != original_entity and new_entity in profiles:
                errors["camera_entity"] = "camera_profile_exists"
            # Preserve stored keys that are not part of the form (hold/tail/unknown).
            profile = self._profile_from_input(submitted, current, preserved=old_profile)
            errors.update(validate_camera_profile(entity_default, profile))
            if not errors:
                profiles.pop(original_entity)
                profiles[new_entity or original_entity] = profile
                return self._save_profiles(profiles)
            errors = self._finalize_camera_errors(errors)

        displayed = {**current, **submitted}
        # Never echo a stored or attempted credential back into the form.
        displayed[CONF_FRIGATE_AUTH_TOKEN] = ""
        return self.async_show_form(
            step_id="camera_edit_profile",
            data_schema=self._profile_schema(
                displayed,
                include_entity=True,
                entity_default=entity_default,
                expanded=_sections_with_errors(errors, _CAMERA_FIELD_SECTIONS),
            ),
            errors=errors,
            description_placeholders={
                "camera_name": self._camera_name(original_entity),
            },
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
    def _profile_schema(
        profile: dict[str, Any],
        *,
        include_entity: bool,
        entity_default: str | None = None,
        expanded: set[str] | None = None,
    ) -> Any:
        """Build the single camera form with grouped optional/advanced fields."""
        expanded = expanded or set()
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
        provider_choices = [
            {"value": PROVIDER_HOME_ASSISTANT, "label": "Home Assistant camera stream"},
            {"value": PROVIDER_GO2RTC, "label": "go2rtc MP4 stream"},
        ]
        schema.update(
            {
                vol.Optional("source_id", default=str(profile.get("source_id") or "")): str,
                vol.Optional("label", default=str(profile.get("label") or "")): str,
                vol.Required(
                    "recording_mode", default=profile.get("recording_mode", MODE_EXTEND)
                ): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=mode_choices, mode=selector.SelectSelectorMode.DROPDOWN
                    )
                ),
                vol.Required(
                    CONF_RECORDING_PROVIDER,
                    default=profile.get(CONF_RECORDING_PROVIDER, PROVIDER_HOME_ASSISTANT),
                ): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=provider_choices, mode=selector.SelectSelectorMode.DROPDOWN
                    )
                ),
                vol.Required(
                    "window_seconds",
                    default=_form_number(profile.get("window_seconds"), 15),
                ): number_spec_selector(SPEC_WINDOW_SECONDS),
                vol.Required(
                    "max_window_seconds",
                    default=_form_number(profile.get("max_window_seconds"), 300),
                ): number_spec_selector(SPEC_MAX_WINDOW_SECONDS),
            }
        )

        timing = vol.Schema(
            {
                vol.Optional(
                    "lookback_seconds",
                    default=_form_number(profile.get("lookback_seconds"), 0),
                ): number_spec_selector(SPEC_LOOKBACK_SECONDS),
                vol.Optional(
                    "chunk_seconds", default=_form_number(profile.get("chunk_seconds"), 10)
                ): number_spec_selector(SPEC_CHUNK_SECONDS),
            }
        )
        provider = vol.Schema(
            {
                vol.Optional(
                    CONF_GO2RTC_STREAM, default=str(profile.get(CONF_GO2RTC_STREAM) or "")
                ): str,
                vol.Optional(
                    CONF_FRIGATE_BASE_URL,
                    default=str(profile.get(CONF_FRIGATE_BASE_URL) or ""),
                ): str,
                vol.Optional(
                    CONF_FRIGATE_CAMERA_NAME,
                    default=str(profile.get(CONF_FRIGATE_CAMERA_NAME) or ""),
                ): str,
                # Credentials are never prefilled; blank keeps the stored token.
                vol.Optional(CONF_FRIGATE_AUTH_TOKEN, default=""): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional(
                    "clear_frigate_auth_token", default=False
                ): selector.BooleanSelector(),
                vol.Optional(
                    CONF_EXPORT_DELAY_SECONDS,
                    default=profile.get(
                        CONF_EXPORT_DELAY_SECONDS, DEFAULT_EXPORT_DELAY_SECONDS
                    ),
                ): number_spec_selector(SPEC_EXPORT_DELAY_SECONDS),
            }
        )
        correlation = vol.Schema(
            {
                vol.Optional(
                    "ownership_group", default=str(profile.get("ownership_group") or "")
                ): str,
                vol.Optional(
                    "correlation_group", default=str(profile.get("correlation_group") or "")
                ): str,
                vol.Optional("zone", default=str(profile.get("zone") or "")): str,
                vol.Optional(
                    "adjacent_zones",
                    default=", ".join(profile.get("adjacent_zones") or [])
                    if isinstance(profile.get("adjacent_zones"), (list, tuple))
                    else str(profile.get("adjacent_zones") or ""),
                ): str,
                vol.Optional(
                    "source_clock_offset_seconds",
                    default=_form_number(
                        profile.get("source_clock_offset_seconds"), 0.0
                    ),
                ): number_spec_selector(SPEC_SOURCE_CLOCK_OFFSET_SECONDS, step=0.1),
                vol.Optional(
                    "source_time_uncertainty_seconds",
                    default=_form_number(
                        profile.get("source_time_uncertainty_seconds"), 0.0
                    ),
                ): number_spec_selector(SPEC_SOURCE_TIME_UNCERTAINTY_SECONDS, step=0.1),
            }
        )

        schema[vol.Required(SECTION_TIMING)] = data_entry_flow.section(
            timing, {"collapsed": SECTION_TIMING not in expanded}
        )
        schema[vol.Required(SECTION_PROVIDER)] = data_entry_flow.section(
            provider, {"collapsed": SECTION_PROVIDER not in expanded}
        )
        schema[vol.Required(SECTION_CORRELATION)] = data_entry_flow.section(
            correlation, {"collapsed": SECTION_CORRELATION not in expanded}
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
        user_input: dict[str, Any],
        defaults: dict[str, Any],
        preserved: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        # Start from the stored profile so keys that are not on the form survive.
        profile: dict[str, Any] = dict(preserved) if preserved else {}
        profile.update(
            {
                key: user_input.get(key, defaults.get(key))
                for key in (
                    "source_id",
                    "label",
                    "recording_mode",
                    "window_seconds",
                    "lookback_seconds",
                    "chunk_seconds",
                    "max_window_seconds",
                    CONF_RECORDING_PROVIDER,
                    CONF_GO2RTC_STREAM,
                )
            }
        )
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

        # Credentials: blank keeps the stored token, the clear flag removes it.
        raw_token = user_input.get(CONF_FRIGATE_AUTH_TOKEN)
        if bool(user_input.get("clear_frigate_auth_token", False)):
            profile[CONF_FRIGATE_AUTH_TOKEN] = None
        elif isinstance(raw_token, str) and raw_token.strip():
            profile[CONF_FRIGATE_AUTH_TOKEN] = raw_token.strip()
        else:
            stored_token = defaults.get(CONF_FRIGATE_AUTH_TOKEN)
            profile[CONF_FRIGATE_AUTH_TOKEN] = stored_token if stored_token else None

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
            export_delay if export_delay is not None else DEFAULT_EXPORT_DELAY_SECONDS
        )

        raw_stream = user_input.get(CONF_GO2RTC_STREAM, defaults.get(CONF_GO2RTC_STREAM))
        profile[CONF_GO2RTC_STREAM] = raw_stream.strip() if isinstance(raw_stream, str) and raw_stream.strip() else None
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
        profile["source_clock_offset_seconds"] = (
            float(raw_offset)
            if isinstance(raw_offset, (int, float)) and not isinstance(raw_offset, bool)
            else raw_offset
        )

        raw_unc = user_input.get(
            "source_time_uncertainty_seconds", defaults.get("source_time_uncertainty_seconds", 0.0)
        )
        profile["source_time_uncertainty_seconds"] = (
            float(raw_unc)
            if isinstance(raw_unc, (int, float)) and not isinstance(raw_unc, bool)
            else raw_unc
        )

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
