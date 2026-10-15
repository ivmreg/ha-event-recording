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

# Default configuration values
DEFAULT_MEDIA_DIR = "event_recording_media"
DEFAULT_RETENTION_SIZE_MB = 2048
DEFAULT_RETENTION_DAYS = 7
DEFAULT_CHUNK_SECONDS = 10
DEFAULT_MAX_WINDOW_SECONDS = 300
DEFAULT_MAX_HOLD_SECONDS = 300
MAX_PARALLEL_RESOURCES = 2

# Services
SERVICE_LOG_EVENT = "log_event"
SERVICE_RECORD = "record"
SERVICE_ATTACH_MEDIA = "attach_media"
SERVICE_FINALIZE = "finalize"

# WebSocket commands
WS_TYPE_EVENTS = "event_recording/events"
WS_TYPE_EVENT = "event_recording/event"
WS_TYPE_SUBSCRIBE = "event_recording/subscribe"
WS_TYPE_MEDIA = "event_recording/media"
WS_TYPE_PROCESS = "event_recording/process"

# Frontend card path
CARD_URL_PATH = "/event_recording/event-card.js"

# Context and hass.data keys
DATA_COORDINATOR = "coordinator"
DATA_API = "api"
DATA_STORE = "store"
DATA_REGISTRY = "registry"
DATA_UNSUB = "unsub"
DATA_SETUP_DONE = "setup_done"
