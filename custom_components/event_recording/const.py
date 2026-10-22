"""Constants for the Event Recording integration."""

from __future__ import annotations

DOMAIN = "event_recording"

# Configuration and options
CONF_MEDIA_ROOT = "media_root"
CONF_STORAGE_DIR = "storage_dir"
CONF_RETENTION_SIZE_MB = "retention_size_mb"
CONF_RETENTION_DAYS = "retention_days"
CONF_CHUNK_SECONDS = "chunk_seconds"
CONF_MAX_WINDOW_SECONDS = "max_window_seconds"
CONF_MAX_PARALLEL_RESOURCES = "max_parallel_resources"
CONF_MAX_QUEUE_DEPTH = "max_queue_depth"
CONF_QUEUE_WAIT_TIMEOUT_SECONDS = "queue_wait_timeout_seconds"
CONF_MIN_DISK_FREE_MB = "min_disk_free_mb"
CONF_RECORDING_MODE = "recording_mode"
CONF_OWNERSHIP_GROUP = "ownership_group"
CONF_ENABLE_DEBUG_LOGGING = "enable_debug_logging"

# Frigate buffered provider configuration keys
CONF_FRIGATE_BASE_URL = "frigate_base_url"
CONF_FRIGATE_CAMERA_NAME = "frigate_camera_name"
CONF_FRIGATE_AUTH_TOKEN = "frigate_auth_token"
CONF_EXPORT_DELAY_SECONDS = "export_delay_seconds"

# Shared go2rtc connection settings are stored only in config-entry options.
CONF_GO2RTC_BASE_URL = "go2rtc_base_url"
CONF_GO2RTC_USERNAME = "go2rtc_username"
CONF_GO2RTC_PASSWORD = "go2rtc_password"
CONF_RECORDING_PROVIDER = "recording_provider"
CONF_GO2RTC_STREAM = "go2rtc_stream"
CONF_GO2RTC_INCLUDE_AUDIO = "go2rtc_include_audio"
PROVIDER_HOME_ASSISTANT = "home_assistant"
PROVIDER_GO2RTC = "go2rtc"

# Auto-association configuration keys
CONF_ENABLE_AUTO_ASSOCIATION = "enable_auto_association"
CONF_AUTO_ASSOCIATION_TOLERANCE_SECONDS = "auto_association_tolerance_seconds"
CONF_MAX_INCIDENT_SPAN_SECONDS = "max_incident_span_seconds"
CONF_CORRELATION_GROUP = "correlation_group"
CONF_ZONE = "zone"
CONF_ADJACENT_ZONES = "adjacent_zones"
CONF_SOURCE_CLOCK_OFFSET_SECONDS = "source_clock_offset_seconds"
CONF_SOURCE_TIME_UNCERTAINTY_SECONDS = "source_time_uncertainty_seconds"

# Default configuration values
DEFAULT_MEDIA_DIR = "event_recording_media"
DEFAULT_RETENTION_SIZE_MB = 2048
DEFAULT_RETENTION_DAYS = 7
DEFAULT_CHUNK_SECONDS = 10
DEFAULT_MAX_WINDOW_SECONDS = 300
DEFAULT_MAX_HOLD_SECONDS = 300
DEFAULT_MAX_PARALLEL_RESOURCES = 2
DEFAULT_MAX_QUEUE_DEPTH = 32
DEFAULT_QUEUE_WAIT_TIMEOUT_SECONDS = 30
DEFAULT_MIN_DISK_FREE_MB = 50
DEFAULT_EXPORT_DELAY_SECONDS = 5
DEFAULT_ENABLE_AUTO_ASSOCIATION = False
DEFAULT_AUTO_ASSOCIATION_TOLERANCE_SECONDS = 10.0
DEFAULT_MAX_INCIDENT_SPAN_SECONDS = 300.0
DEFAULT_WATCHDOG_PADDING_SECONDS = 15.0
DEFAULT_ENABLE_DEBUG_LOGGING = False
MAX_PARALLEL_RESOURCES = 2

# Recording modes
MODE_EXTEND = "extend"
MODE_NATIVE_FIXED = "native_fixed"
MODE_NATIVE_SNAPSHOT = "native_snapshot"
MODE_BUFFERED = "buffered"
VALID_RECORDING_MODES = frozenset(
    [MODE_EXTEND, MODE_NATIVE_FIXED, MODE_NATIVE_SNAPSHOT, MODE_BUFFERED]
)

# Error codes
ERROR_QUEUE_TIMEOUT = "QUEUE_TIMEOUT"
ERROR_QUEUE_OVERFLOW = "QUEUE_OVERFLOW"
ERROR_EXPIRED_WINDOW = "EXPIRED_WINDOW"
ERROR_BUFFER_UNAVAILABLE = "BUFFER_UNAVAILABLE"
ERROR_DISK_FULL = "DISK_FULL"
ERROR_DISK_SPACE_EXHAUSTED = "DISK_SPACE_EXHAUSTED"
ERROR_STORAGE_CAPACITY_EXCEEDED = "STORAGE_CAPACITY_EXCEEDED"
ERROR_RESOURCE_QUARANTINED = "RESOURCE_QUARANTINED"
ERROR_ZERO_CLIPS_RECORDED = "ZERO_CLIPS_RECORDED"
ERROR_MEDIA_SETUP_FAILED = "MEDIA_SETUP_FAILED"
ERROR_CAPTURE_TASK_FAILED = "CAPTURE_TASK_FAILED"
ERROR_CAPTURE_TASK_CANCELLED = "CAPTURE_TASK_CANCELLED"
ERROR_RECORDING_INTERRUPTED = "RECORDING_INTERRUPTED"

# Timing provenance
TIME_QUALITY_DEVICE = "device_clock"
TIME_QUALITY_HOST = "host_recorded"
TIME_QUALITY_ESTIMATED = "estimated"
TIME_QUALITY_PROVIDER_REPORTED = "provider_reported"
TIME_QUALITY_UNKNOWN = "unknown"
VALID_TIME_QUALITIES = frozenset(
    [
        TIME_QUALITY_DEVICE,
        TIME_QUALITY_HOST,
        TIME_QUALITY_ESTIMATED,
        TIME_QUALITY_PROVIDER_REPORTED,
        TIME_QUALITY_UNKNOWN,
    ]
)

# Services
SERVICE_LOG_EVENT = "log_event"
SERVICE_RECORD = "record"
SERVICE_RECORD_EVENTS = "record_events"
SERVICE_ATTACH_MEDIA = "attach_media"
SERVICE_FINALIZE = "finalize"

# WebSocket commands
WS_TYPE_EVENTS = "event_recording/events"
WS_TYPE_EVENT = "event_recording/event"
WS_TYPE_SUBSCRIBE = "event_recording/subscribe"
WS_TYPE_MEDIA = "event_recording/media"
WS_TYPE_PROCESS = "event_recording/process"
WS_TYPE_LIST_ACTIVITY = "event_recording/list_activity"
WS_TYPE_INCIDENT = "event_recording/incident"
WS_TYPE_INCIDENTS = "event_recording/incidents"

# Frontend card path
CARD_URL_PATH = "/event_recording/event-card.js"

# Context and hass.data keys
DATA_COORDINATOR = "coordinator"
DATA_API = "api"
DATA_STORE = "store"
DATA_REGISTRY = "registry"
DATA_UNSUB = "unsub"
DATA_SETUP_DONE = "setup_done"
