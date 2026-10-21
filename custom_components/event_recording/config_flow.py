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

_GLOBAL_SECTION_IDS = frozenset({SECTION_CONNECTION, SECTION_ADVANCED, SECTION_CORRELATION})

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

# The camera list is one flat typed ObjectSelector row per camera. The camera
# entity itself is the row identity (it is also the runtime profile map key).
CONFIRM_CAMERA_REMOVALS = "confirm_camera_removals"

_CAMERA_ROW_FIELDS: tuple[str, ...] = (
    "camera_entity",
    "source_id",
    "label",
    "recording_mode",
    CONF_RECORDING_PROVIDER,
    "window_seconds",
    "max_window_seconds",
    "lookback_seconds",
    "chunk_seconds",
    CONF_GO2RTC_STREAM,
    CONF_FRIGATE_BASE_URL,
    CONF_FRIGATE_CAMERA_NAME,
    CONF_FRIGATE_AUTH_TOKEN,
    "clear_frigate_auth_token",
    CONF_EXPORT_DELAY_SECONDS,
    "ownership_group",
    "correlation_group",
    "zone",
    "adjacent_zones",
    "source_clock_offset_seconds",
    "source_time_uncertainty_seconds",
)
_CAMERA_ROW_FIELD_SET = frozenset(_CAMERA_ROW_FIELDS)

# Native list guard: reject absurd payloads instead of iterating unbounded input.
_MAX_CAMERA_PROFILE_ROWS = 100

# Concise, friendly labels used in row errors (never raw option keys or values).
_CAMERA_ROW_LABELS = {
    "camera_entity": "Camera",
    "source_id": "Source ID",
    "label": "Display name",
    "recording_mode": "Recording capture mode",
    CONF_RECORDING_PROVIDER: "Recording provider",
    "window_seconds": "Default recording window",
    "max_window_seconds": "Maximum recording window",
    "lookback_seconds": "Lookback",
    "chunk_seconds": "Chunk duration",
    CONF_GO2RTC_STREAM: "go2rtc stream alias",
    CONF_FRIGATE_BASE_URL: "Frigate base URL",
    CONF_FRIGATE_CAMERA_NAME: "Frigate camera name",
    CONF_FRIGATE_AUTH_TOKEN: "Frigate API token",
    "clear_frigate_auth_token": "Clear saved Frigate token",
    CONF_EXPORT_DELAY_SECONDS: "Export delay",
    "ownership_group": "Ownership group",
    "correlation_group": "Correlation group",
    "zone": "Zone",
    "adjacent_zones": "Adjacent zones",
    "source_clock_offset_seconds": "Source clock offset",
    "source_time_uncertainty_seconds": "Source time uncertainty",
    "hold": "Stored hold setting",
    "tail_window_seconds": "Stored tail window",
}

_CAMERA_ERROR_HINTS = {
    "invalid_camera_entity": "choose an existing Home Assistant camera entity",
    "camera_entity_not_found": "this camera entity does not exist in Home Assistant",
    "camera_profile_exists": "this camera is listed more than once; keep one row per camera",
    "invalid_source_id": "source ID must be 1 to 128 characters",
    "invalid_label": "display name must be 1 to 256 characters",
    "invalid_window_seconds": "recording window must be a whole number from 1 to 300 seconds",
    "invalid_lookback_seconds": "lookback must be a whole number from 0 to 60 seconds",
    "invalid_chunk_seconds": "chunk duration must be a whole number from 1 to 15 seconds",
    "invalid_max_window_seconds": "maximum window must be a whole number from 1 to 300 seconds",
    "max_window_below_window": "maximum window must be at least the default recording window",
    "invalid_recording_mode": "choose extend, native_fixed, native_snapshot, or buffered",
    "invalid_recording_provider": "choose Home Assistant or go2rtc as the recording provider",
    "go2rtc_unsupported_recording_mode": "go2rtc supports only extend and native_fixed modes",
    "invalid_go2rtc_stream": "enter a named go2rtc stream alias",
    "invalid_ownership_group": "ownership group must be at most 64 characters",
    "invalid_correlation_group": "correlation group must be at most 64 characters",
    "invalid_zone": "zone must be at most 64 characters",
    "invalid_adjacent_zones": "enter up to 16 comma-separated zones",
    "invalid_source_clock_offset_seconds": (
        "source clock offset must be between -3600 and 3600 seconds"
    ),
    "invalid_source_time_uncertainty_seconds": (
        "source time uncertainty must be between 0 and 300 seconds"
    ),
    "invalid_hold": "the stored hold setting is invalid",
    "invalid_frigate_base_url": "Frigate base URL must start with http:// or https://",
    "invalid_frigate_camera_name": (
        "Frigate camera name may only use letters, numbers, underscores, and hyphens"
    ),
    "invalid_export_delay_seconds": "export delay must be a whole number from 0 to 60 seconds",
}

_MISSING = object()


def _object_field(
    field_selector: Any,
    label: str,
    *,
    required: bool = False,
    default: Any = _MISSING,
) -> Any:
    """Build one typed ObjectSelector row field with a static friendly label."""
    field: dict[str, Any] = {
        "selector": field_selector,
        "label": label,
        "required": required,
    }
    if default is not _MISSING:
        field["default"] = default
    return selector.ObjectSelectorField(**field)


def _form_number(value: Any, default: Any) -> Any:
    """Return a form default without discarding an attempted invalid value."""
    if value is None or value == "" or isinstance(value, bool):
        return default
    if isinstance(value, float) and not math.isfinite(value):
        return default
    return value


class CameraProfileListSelector(selector.ObjectSelector):
    """Native typed object list that guards malformed rows before iteration.

    The native selector iterates row values directly, so a list containing a
    scalar (``None``, ``True``, an int) would raise ``TypeError`` before any
    handler runs. This subclass rejects such shapes with a normal schema error
    and then delegates to the native implementation, which still validates every
    declared field selector and rejects undeclared keys. Serialization stays the
    native typed ObjectSelector payload (multiple rows, typed fields).
    """

    def __call__(self, data: Any) -> Any:
        if isinstance(data, Mapping):
            candidates: list[Any] = [data]
        elif isinstance(data, (list, tuple)):
            candidates = list(data)
        else:
            raise vol.Invalid("camera profiles must be a list of camera rows")
        if len(candidates) > _MAX_CAMERA_PROFILE_ROWS:
            raise vol.Invalid("too many camera profile rows")
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                raise vol.Invalid("camera profile rows must be mappings")
        return super().__call__(data)


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
    """Handle options flow for Event Recording in a single settings page."""

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
        """Show and save every global setting and camera profile on one page."""
        current = self._global_current()
        stored = self._profiles()
        errors: dict[str, str] = {}
        submitted: dict[str, Any] = {}
        proposed_rows = self._profile_rows(stored)
        camera_detail = ""
        confirm_removals = False

        if user_input is not None:
            submitted = _flatten_sections(user_input, _GLOBAL_SECTION_IDS)
            values, global_errors = self._global_values(submitted, current)
            errors.update(global_errors)

            profiles: dict[str, dict[str, Any]] | None = None
            if CAMERA_PROFILES in submitted:
                profiles, row_error, proposed_rows, camera_detail = await self._build_profiles(
                    submitted[CAMERA_PROFILES], stored, values
                )
                if row_error:
                    errors[CAMERA_PROFILES] = row_error

            removed: list[str] = []
            if profiles is not None:
                removed = [entity_id for entity_id in stored if entity_id not in profiles]
            if removed and not bool(submitted.get(CONFIRM_CAMERA_REMOVALS, False)):
                confirm_removals = True
                errors.setdefault(CAMERA_PROFILES, "camera_removal_requires_confirmation")
                camera_detail = camera_detail or (
                    "Confirm removal of: "
                    + ", ".join(self._camera_name(entity_id) for entity_id in removed)
                )

            if not errors:
                options = self._current_options()
                options.update(values)
                if profiles is not None:
                    options[CAMERA_PROFILES] = profiles
                # The confirmation flag is a one-shot guard and is never persisted.
                options.pop(CONFIRM_CAMERA_REMOVALS, None)
                return self.async_create_entry(title="", data=options)

        displayed = {**current, **submitted}
        # Never echo a stored or attempted secret back into the form.
        displayed[CONF_GO2RTC_PASSWORD] = ""
        schema = self._init_schema(
            displayed,
            _sections_with_errors(errors, _GLOBAL_FIELD_SECTIONS),
            proposed_rows,
            confirm_removals,
        )
        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
            description_placeholders={
                **self._summary_placeholders(displayed),
                "camera_profile_error": camera_detail,
            },
        )

    def _global_values(
        self, submitted: Mapping[str, Any], current: Mapping[str, Any]
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Validate every global setting and return flat, typed option values."""
        values: dict[str, Any] = {}
        errors: dict[str, str] = {}

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

        return values, errors

    def _init_schema(
        self,
        displayed: dict[str, Any],
        expanded: set[str],
        rows: list[dict[str, Any]],
        show_confirm: bool,
    ) -> Any:
        """Build the single settings page: basics, camera list, then sections."""
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
        schema: dict[Any, Any] = {
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
            # A suggested value prefills the native list without injecting a
            # schema default, so an omitted field stays distinguishable from an
            # explicit (even empty) submission.
            vol.Optional(
                CAMERA_PROFILES, description={"suggested_value": rows}
            ): self._camera_profile_selector(),
        }
        if show_confirm:
            schema[
                vol.Required(
                    CONFIRM_CAMERA_REMOVALS,
                    default=bool(displayed.get(CONFIRM_CAMERA_REMOVALS, False)),
                )
            ] = selector.BooleanSelector()
        schema[vol.Required(SECTION_CONNECTION)] = data_entry_flow.section(
            connection, {"collapsed": SECTION_CONNECTION not in expanded}
        )
        schema[vol.Required(SECTION_ADVANCED)] = data_entry_flow.section(
            advanced, {"collapsed": SECTION_ADVANCED not in expanded}
        )
        schema[vol.Required(SECTION_CORRELATION)] = data_entry_flow.section(
            correlation, {"collapsed": SECTION_CORRELATION not in expanded}
        )
        return vol.Schema(schema)

    def _camera_profile_selector(self) -> Any:
        """Return the native typed camera-profile list selector (never YAML)."""
        return CameraProfileListSelector(
            selector.ObjectSelectorConfig(
                fields=self._camera_row_fields(),
                multiple=True,
                label_field="camera_entity",
                description_field="label",
            )
        )

    def _camera_row_fields(self) -> dict[str, Any]:
        """Describe the flat camera row editor; every field is a typed selector."""
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
        return {
            "camera_entity": _object_field(
                selector.EntitySelector(selector.EntitySelectorConfig(domain="camera")),
                "Camera",
                required=True,
            ),
            "source_id": _object_field(selector.TextSelector(), "Source ID"),
            "label": _object_field(selector.TextSelector(), "Display name"),
            "recording_mode": _object_field(
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=mode_choices, mode=selector.SelectSelectorMode.DROPDOWN
                    )
                ),
                "Recording capture mode",
                default=MODE_EXTEND,
            ),
            CONF_RECORDING_PROVIDER: _object_field(
                selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=provider_choices, mode=selector.SelectSelectorMode.DROPDOWN
                    )
                ),
                "Recording provider",
                default=PROVIDER_HOME_ASSISTANT,
            ),
            "window_seconds": _object_field(
                number_spec_selector(SPEC_WINDOW_SECONDS, unit_of_measurement="s"),
                "Default recording window",
                default=15,
            ),
            "max_window_seconds": _object_field(
                number_spec_selector(SPEC_MAX_WINDOW_SECONDS, unit_of_measurement="s"),
                "Maximum recording window",
                default=self._global_max_window_seconds(),
            ),
            "lookback_seconds": _object_field(
                number_spec_selector(SPEC_LOOKBACK_SECONDS, unit_of_measurement="s"),
                "Lookback",
                default=0,
            ),
            "chunk_seconds": _object_field(
                number_spec_selector(SPEC_CHUNK_SECONDS, unit_of_measurement="s"),
                "Chunk duration",
                default=self._global_chunk_seconds(),
            ),
            CONF_GO2RTC_STREAM: _object_field(selector.TextSelector(), "go2rtc stream alias"),
            CONF_FRIGATE_BASE_URL: _object_field(selector.TextSelector(), "Frigate base URL"),
            CONF_FRIGATE_CAMERA_NAME: _object_field(
                selector.TextSelector(), "Frigate camera name"
            ),
            CONF_FRIGATE_AUTH_TOKEN: _object_field(
                selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                "Frigate API token (blank keeps the saved token)",
                default="",
            ),
            "clear_frigate_auth_token": _object_field(
                selector.BooleanSelector(), "Clear saved Frigate token", default=False
            ),
            CONF_EXPORT_DELAY_SECONDS: _object_field(
                number_spec_selector(SPEC_EXPORT_DELAY_SECONDS, unit_of_measurement="s"),
                "Export delay",
                default=DEFAULT_EXPORT_DELAY_SECONDS,
            ),
            "ownership_group": _object_field(selector.TextSelector(), "Ownership group"),
            "correlation_group": _object_field(selector.TextSelector(), "Correlation group"),
            "zone": _object_field(selector.TextSelector(), "Zone"),
            "adjacent_zones": _object_field(selector.TextSelector(), "Adjacent zones"),
            "source_clock_offset_seconds": _object_field(
                number_spec_selector(
                    SPEC_SOURCE_CLOCK_OFFSET_SECONDS, step=0.1, unit_of_measurement="s"
                ),
                "Source clock offset",
                default=0.0,
            ),
            "source_time_uncertainty_seconds": _object_field(
                number_spec_selector(
                    SPEC_SOURCE_TIME_UNCERTAINTY_SECONDS, step=0.1, unit_of_measurement="s"
                ),
                "Source time uncertainty",
                default=0.0,
            ),
        }

    def _profile_rows(self, profiles: Mapping[str, Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Project stored profiles into typed rows without secrets or unknown keys."""
        return [self._profile_row(entity_id, profile) for entity_id, profile in profiles.items()]

    def _profile_row(self, entity_id: str, profile: Mapping[str, Any]) -> dict[str, Any]:
        """Return one display row containing only declared, non-secret fields."""
        base: dict[str, Any] = default_camera_profile(
            entity_id,
            chunk_seconds=self._global_chunk_seconds(),
            max_window_seconds=self._global_max_window_seconds(),
        )
        base.update(profile)
        adjacent = base.get("adjacent_zones")
        if isinstance(adjacent, (list, tuple)):
            adjacent_text = ", ".join(str(zone) for zone in adjacent)
        else:
            adjacent_text = str(adjacent or "")
        return {
            "camera_entity": entity_id,
            "source_id": str(base.get("source_id") or ""),
            "label": str(base.get("label") or ""),
            "recording_mode": str(base.get("recording_mode") or MODE_EXTEND),
            CONF_RECORDING_PROVIDER: str(
                base.get(CONF_RECORDING_PROVIDER) or PROVIDER_HOME_ASSISTANT
            ),
            "window_seconds": _form_number(base.get("window_seconds"), 15),
            "max_window_seconds": _form_number(
                base.get("max_window_seconds"), self._global_max_window_seconds()
            ),
            "lookback_seconds": _form_number(base.get("lookback_seconds"), 0),
            "chunk_seconds": _form_number(
                base.get("chunk_seconds"), self._global_chunk_seconds()
            ),
            CONF_GO2RTC_STREAM: str(base.get(CONF_GO2RTC_STREAM) or ""),
            CONF_FRIGATE_BASE_URL: str(base.get(CONF_FRIGATE_BASE_URL) or ""),
            CONF_FRIGATE_CAMERA_NAME: str(base.get(CONF_FRIGATE_CAMERA_NAME) or ""),
            # Saved and attempted secrets are never part of a form default.
            CONF_FRIGATE_AUTH_TOKEN: "",
            "clear_frigate_auth_token": False,
            CONF_EXPORT_DELAY_SECONDS: _form_number(
                base.get(CONF_EXPORT_DELAY_SECONDS), DEFAULT_EXPORT_DELAY_SECONDS
            ),
            "ownership_group": str(base.get("ownership_group") or ""),
            "correlation_group": str(base.get("correlation_group") or ""),
            "zone": str(base.get("zone") or ""),
            "adjacent_zones": adjacent_text,
            "source_clock_offset_seconds": _form_number(
                base.get("source_clock_offset_seconds"), 0.0
            ),
            "source_time_uncertainty_seconds": _form_number(
                base.get("source_time_uncertainty_seconds"), 0.0
            ),
        }

    def _normalize_rows(
        self, raw_rows: Any
    ) -> tuple[list[dict[str, Any]] | None, str | None]:
        """Guard the row list shape before touching stored profiles."""
        if isinstance(raw_rows, Mapping):
            candidates: list[Any] = [raw_rows]
        elif isinstance(raw_rows, (list, tuple)):
            candidates = list(raw_rows)
        else:
            return None, "invalid_camera_profiles"
        if len(candidates) > _MAX_CAMERA_PROFILE_ROWS:
            return None, "too_many_camera_profiles"
        rows: list[dict[str, Any]] = []
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                return None, "invalid_camera_profiles"
            if set(candidate) - _CAMERA_ROW_FIELD_SET:
                return None, "invalid_camera_profiles"
            rows.append(dict(candidate))
        return rows, None

    async def _build_profiles(
        self,
        raw_rows: Any,
        stored: Mapping[str, Mapping[str, Any]],
        global_values: Mapping[str, Any],
    ) -> tuple[dict[str, dict[str, Any]] | None, str | None, list[dict[str, Any]], str]:
        """Build and validate every submitted profile before any write happens."""
        rows, shape_error = self._normalize_rows(raw_rows)
        if shape_error is not None:
            return None, shape_error, self._profile_rows(stored), ""

        seed_rows = {row["camera_entity"]: row for row in self._profile_rows(stored)}
        display_rows: list[dict[str, Any]] = []
        problems: list[str] = []
        profiles: dict[str, dict[str, Any]] = {}
        seen: set[str] = set()

        for index, row in enumerate(rows):
            entity_id = str(row.get("camera_entity") or "").strip()
            display_rows.append(self._display_row(row))
            reference = self._row_reference(entity_id, index)

            if not entity_id.startswith("camera.") or not entity_id.partition(".")[2]:
                problems.append(
                    f"{reference}: {_CAMERA_ERROR_HINTS['invalid_camera_entity']}."
                )
                continue
            if entity_id in seen:
                problems.append(
                    f"{reference}: {_CAMERA_ERROR_HINTS['camera_profile_exists']}."
                )
                continue
            seen.add(entity_id)

            if await self._validate_camera_entity(entity_id):
                problems.append(
                    f"{reference}: {_CAMERA_ERROR_HINTS['camera_entity_not_found']}."
                )
                continue

            # Identity is the camera entity: an unchanged row keeps its stored
            # profile byte-for-byte, including unknown keys and saved secrets.
            # A token attempt or a clear request always takes the merge path.
            stored_profile = stored.get(entity_id)
            if (
                stored_profile is not None
                and not self._row_touches_token(row)
                and display_rows[-1] == seed_rows.get(entity_id)
            ):
                profiles[entity_id] = dict(stored_profile)
                continue

            profile, profile_errors = self._merge_profile_row(
                entity_id, row, stored_profile, global_values
            )
            profiles[entity_id] = profile
            for field, error_key in profile_errors.items():
                problems.append(self._profile_problem(reference, field, error_key))

        if problems:
            return None, "invalid_camera_profile", display_rows, self._problem_detail(problems)
        return profiles, None, display_rows, ""

    def _merge_profile_row(
        self,
        entity_id: str,
        row: Mapping[str, Any],
        stored_profile: Mapping[str, Any] | None,
        global_values: Mapping[str, Any],
    ) -> tuple[dict[str, Any], dict[str, str]]:
        """Merge one submitted row against the stored profile for the same entity."""
        errors: dict[str, str] = {}
        if stored_profile is None:
            chunk_seconds = int(
                global_values.get(CONF_CHUNK_SECONDS, self._global_chunk_seconds())
            )
            profile: dict[str, Any] = dict(
                default_camera_profile(
                    entity_id,
                    name=self._camera_name(entity_id),
                    chunk_seconds=chunk_seconds,
                    max_window_seconds=self._global_max_window_seconds(),
                )
            )
            is_new = True
        else:
            profile = dict(stored_profile)
            is_new = False

        for field in ("source_id", "label"):
            if field not in row:
                continue
            value = row[field]
            if isinstance(value, str) and value.strip():
                profile[field] = value.strip()
            elif not isinstance(value, str):
                errors[field] = f"invalid_{field}"
            # Blank on an existing row keeps the stored value; blank on a new
            # row keeps the value inferred from the selected camera entity.

        for field, spec in (
            ("window_seconds", SPEC_WINDOW_SECONDS),
            ("lookback_seconds", SPEC_LOOKBACK_SECONDS),
            ("chunk_seconds", SPEC_CHUNK_SECONDS),
            ("max_window_seconds", SPEC_MAX_WINDOW_SECONDS),
            (CONF_EXPORT_DELAY_SECONDS, SPEC_EXPORT_DELAY_SECONDS),
        ):
            if field not in row:
                continue
            try:
                profile[field] = int(spec.validate(row[field]))
            except (ValidationError, ValueError, TypeError):
                errors[field] = f"invalid_{field}"

        for field, spec in (
            ("source_clock_offset_seconds", SPEC_SOURCE_CLOCK_OFFSET_SECONDS),
            ("source_time_uncertainty_seconds", SPEC_SOURCE_TIME_UNCERTAINTY_SECONDS),
        ):
            if field not in row:
                continue
            try:
                profile[field] = float(spec.validate(row[field]))
            except (ValidationError, ValueError, TypeError):
                errors[field] = f"invalid_{field}"

        for field in ("recording_mode", CONF_RECORDING_PROVIDER):
            if field not in row or row[field] is None:
                continue
            value = row[field]
            if isinstance(value, str):
                profile[field] = value.strip()
            else:
                errors[field] = f"invalid_{field}"

        for field in (CONF_GO2RTC_STREAM, CONF_FRIGATE_BASE_URL, CONF_FRIGATE_CAMERA_NAME):
            if field not in row:
                continue
            value = row[field]
            if value is None:
                profile[field] = None
            elif isinstance(value, str):
                profile[field] = value.strip() or None
            else:
                errors[field] = f"invalid_{field}"

        for field in ("ownership_group", "correlation_group", "zone"):
            if field not in row:
                continue
            value = row[field]
            if value is None:
                profile[field] = None
            elif isinstance(value, str):
                profile[field] = value.strip() or None
            else:
                errors[field] = f"invalid_{field}"

        if "adjacent_zones" in row:
            raw_adjacent = row["adjacent_zones"]
            if raw_adjacent is None:
                profile["adjacent_zones"] = []
            elif isinstance(raw_adjacent, str):
                profile["adjacent_zones"] = [
                    zone.strip() for zone in raw_adjacent.split(",") if zone.strip()
                ]
            elif isinstance(raw_adjacent, (list, tuple)):
                profile["adjacent_zones"] = [
                    str(zone).strip() for zone in raw_adjacent if str(zone).strip()
                ]
            else:
                errors["adjacent_zones"] = "invalid_adjacent_zones"

        # Credentials only ever move within one camera entity: blank keeps the
        # stored token, the clear flag removes it, a typed token replaces it.
        clear_token = bool(row.get("clear_frigate_auth_token", False))
        typed_token = row.get(CONF_FRIGATE_AUTH_TOKEN)
        if clear_token:
            profile[CONF_FRIGATE_AUTH_TOKEN] = None
        elif isinstance(typed_token, str) and typed_token.strip():
            profile[CONF_FRIGATE_AUTH_TOKEN] = typed_token.strip()
        elif is_new:
            profile[CONF_FRIGATE_AUTH_TOKEN] = None

        profile.setdefault("hard_cap_behavior", "stop")

        for field, error_key in validate_camera_profile(entity_id, profile).items():
            errors.setdefault(field, error_key)
        return profile, errors

    def _display_row(self, row: Mapping[str, Any]) -> dict[str, Any]:
        """Return an attempted row for re-display with its secret scrubbed."""
        display = {
            key: value
            for key, value in row.items()
            if key in _CAMERA_ROW_FIELD_SET and key != CONF_FRIGATE_AUTH_TOKEN
        }
        display[CONF_FRIGATE_AUTH_TOKEN] = ""
        return display

    @staticmethod
    def _row_touches_token(row: Mapping[str, Any]) -> bool:
        """Return True when the raw row replaces or clears the Frigate token."""
        if bool(row.get("clear_frigate_auth_token", False)):
            return True
        typed_token = row.get(CONF_FRIGATE_AUTH_TOKEN)
        return isinstance(typed_token, str) and bool(typed_token.strip())

    def _row_reference(self, entity_id: str, index: int) -> str:
        """Return a concise, secret-free row reference for error details."""
        if entity_id.startswith("camera.") and entity_id.partition(".")[2]:
            return self._camera_name(entity_id)
        return f"Row {index + 1}"

    @staticmethod
    def _profile_problem(reference: str, field: str, error_key: str) -> str:
        """Format one row problem with a friendly field name and no values."""
        label = _CAMERA_ROW_LABELS.get(field, field)
        hint = _CAMERA_ERROR_HINTS.get(error_key, "this value is invalid")
        return f"{reference} -> {label}: {hint}."

    @staticmethod
    def _problem_detail(problems: list[str]) -> str:
        """Return a concise detail line for the camera list error."""
        shown = problems[:3]
        detail = " ".join(shown)
        if len(problems) > len(shown):
            detail = f"{detail} (+{len(problems) - len(shown)} more)"
        return detail

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

    def _current_options(self) -> dict[str, Any]:
        """Copy current options so saving preserves every unrelated setting."""
        return dict(self.config_entry.options)

    def _profiles(self) -> dict[str, dict[str, Any]]:
        """Return profiles with legacy entry data as fallback."""
        from .camera_profiles import get_camera_profiles

        return get_camera_profiles(self.config_entry)

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
