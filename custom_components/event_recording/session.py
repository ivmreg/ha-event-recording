"""Recording session coordinator and intent admission for Event Recording.

Encapsulates profile defaults snapshotting, coalescing trigger defaults inheritance,
and session deadline evaluation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from .const import MODE_BUFFERED
from .core.models import ResourceSession, utc_now
from .core.store import JournalStore

_LOGGER = logging.getLogger(__name__)


def _safe_utc_now() -> Any:
    """Return UTC now, respecting dynamic module monkeypatching on coordinator."""
    import sys

    coord_mod = sys.modules.get("custom_components.event_recording.coordinator")
    if coord_mod is not None and hasattr(coord_mod, "utc_now"):
        return coord_mod.utc_now()
    return utc_now()


def prepare_profile_session_intent(
    session: ResourceSession | None,
    profile_defaults: dict[str, Any] | None,
    explicit_fields: set[str],
    explicit_recorder_config: dict[str, Any],
    resource_key: str | None,
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """Merge and pin camera-profile defaults to intent kwargs before durable store admission.

    Coalesced triggers inherit defaults from active session snapshot, while
    explicit fields on this trigger retain explicit values.
    """
    updated_kwargs = dict(kwargs)
    if isinstance(profile_defaults, dict) and resource_key:
        if session and session.active_event_id:
            session_config = dict(session.recorder_config or {})
            pinned_profile = dict(session_config.get("_event_recording_profile") or {})
            field_map = {
                "source_id": "source_id",
                "label": "label",
                "window_seconds": "window_seconds",
                "lookback_seconds": "lookback_seconds",
                "hold": "hold",
                "tail_window_seconds": "tail_window_seconds",
            }
            for arg_name, profile_name in field_map.items():
                if arg_name not in explicit_fields and profile_name in pinned_profile:
                    updated_kwargs[arg_name] = pinned_profile[profile_name]

            merged_config = dict(session_config)
            if explicit_recorder_config:
                merged_config.update(explicit_recorder_config)
            merged_config["_event_recording_profile"] = pinned_profile
            updated_kwargs["recorder_config"] = merged_config
        else:
            recorder_config = dict(updated_kwargs.get("recorder_config") or {})
            recorder_config["_event_recording_profile"] = dict(profile_defaults)
            updated_kwargs["recorder_config"] = recorder_config

    return updated_kwargs


class SessionCoordinator:
    """Coordinates recording intent admission, session profiles, and deadline checks."""

    def __init__(self, store: JournalStore) -> None:
        self.store = store

    async def admit_record_intent(self, *args: Any, **kwargs: Any) -> Any:
        """Pin camera-profile defaults to a new session before durable admission."""
        profile_defaults = kwargs.pop("_profile_defaults", None)
        explicit_fields = set(kwargs.pop("_explicit_fields", ()))
        explicit_recorder_config = dict(kwargs.pop("_explicit_recorder_config", {}) or {})
        resource_key = kwargs.get("resource_key")

        session: ResourceSession | None = None
        if isinstance(profile_defaults, dict) and resource_key:
            session = await self.store.get_resource_session(resource_key)

        effective_kwargs = prepare_profile_session_intent(
            session=session,
            profile_defaults=profile_defaults,
            explicit_fields=explicit_fields,
            explicit_recorder_config=explicit_recorder_config,
            resource_key=resource_key,
            kwargs=kwargs,
        )

        return await self.store.admit_record_intent(*args, **effective_kwargs)

    async def handle_stop_intent(self, resource_key: str) -> None:
        """Handle stop operation for a resource session."""
        session = await self.store.get_resource_session(resource_key)
        if session and session.active_event_id:
            await self.store.update_capture_state(session.active_event_id, "ready")
            await self.store.clear_resource_session(resource_key)

    def evaluate_queue_deadline(
        self,
        session: ResourceSession,
        recording_mode: str,
        queue_wait_timeout_seconds: float,
    ) -> tuple[bool, float, datetime]:
        """Evaluate session queue wait deadline and scene expiration.

        Returns (is_expired, deadline_seconds, scene_deadline).
        """
        now = _safe_utc_now()
        deadline_seconds = float(queue_wait_timeout_seconds)
        if recording_mode != MODE_BUFFERED and not session.hold:
            remaining_scene = (session.desired_capture_end - now).total_seconds()
            if remaining_scene <= 0:
                return True, 0.0, now
            deadline_seconds = min(deadline_seconds, remaining_scene)

        scene_deadline = now + timedelta(seconds=deadline_seconds)
        return False, deadline_seconds, scene_deadline
