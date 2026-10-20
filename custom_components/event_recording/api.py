"""Canonical public facade for the Event Recording integration.

Provides a neutral Python API for other integrations and consumers to interact with
durable event storage, resolve verified media paths, subscribe to revisions,
and register consumer capabilities without coupling to concrete AI analyzers.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from homeassistant.core import HomeAssistant

from .const import DATA_API, DOMAIN
from .core.journal.common import DEFAULT_PIN_LEASE_SECONDS
from .core.models import Incident, IncidentInputs, MediaPinLease, RecordedEvent
from .core.store import JournalStore


class ConsumerRegistration:
    """Represents a registered consumer capability."""

    def __init__(
        self,
        consumer_id: str,
        retry_callback: Callable[[str], Awaitable[bool]] | None = None,
        available: bool = True,
        is_busy: Callable[[], Awaitable[bool]] | None = None,
    ) -> None:
        self.id = consumer_id
        self.retry_callback = retry_callback
        self.available = available
        self.is_busy = is_busy


class RecordingAPI:
    """Canonical public facade for Event Recording."""

    def __init__(self, hass: HomeAssistant, store: JournalStore) -> None:
        self._hass = hass
        self._store = store
        self._consumers: dict[str, ConsumerRegistration] = {}

    @property
    def store(self) -> JournalStore:
        """Return the underlying JournalStore for pure core reads, writes, and CAS annotations."""
        return self._store

    @property
    def is_open(self) -> bool:
        return self._store.is_open

    async def list_finalized_media(
        self, cursor: str | None = None, limit: int = 50, namespace_lacking: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]:
        return await self._store.list_finalized_media(
            cursor=cursor, limit=limit, namespace_lacking=namespace_lacking
        )

    async def reserve_annotation(
        self,
        event_id: str,
        namespace: str,
        expected_media_revision: int,
        worker_id: str,
        ttl_seconds: float = 180.0,
    ) -> int | None:
        return await self._store.reserve_annotation(
            event_id=event_id,
            namespace=namespace,
            expected_media_revision=expected_media_revision,
            worker_id=worker_id,
            ttl_seconds=ttl_seconds,
        )

    async def renew_annotation_reservation(
        self,
        event_id: str,
        namespace: str,
        expected_media_revision: int,
        expected_generation: int,
        worker_id: str,
        ttl_seconds: float = 180.0,
    ) -> bool:
        return await self._store.renew_annotation_reservation(
            event_id=event_id,
            namespace=namespace,
            expected_media_revision=expected_media_revision,
            expected_generation=expected_generation,
            worker_id=worker_id,
            ttl_seconds=ttl_seconds,
        )

    async def set_annotation_cas(
        self,
        event_id: str,
        expected_media_revision: int,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int | None = None,
        status: str | None = None,
    ) -> bool:
        return await self._store.set_annotation_cas(
            event_id=event_id,
            expected_media_revision=expected_media_revision,
            namespace=namespace,
            payload=payload,
            expected_generation=expected_generation,
            status=status,
        )

    async def pin_media_lease(
        self,
        event_id: str,
        lease_seconds: float = DEFAULT_PIN_LEASE_SECONDS,
        reason: str | None = None,
    ) -> MediaPinLease | None:
        return await self._store.pin_media_lease(
            event_id=event_id, lease_seconds=lease_seconds, reason=reason
        )

    async def release_media_pin(self, lease_token: str) -> bool:
        return await self._store.release_media_pin(lease_token)

    async def list_incidents(
        self, limit: int = 50, before_cursor: str | None = None
    ) -> tuple[list[Incident], str | None]:
        return await self._store.list_incidents(limit=limit, before_cursor=before_cursor)

    async def get_event(self, event_id: str) -> RecordedEvent | None:
        """Async delegate to store.get_event."""
        return await self._store.get_event(event_id)

    async def resolve_media(self, event_id: str, media_id: str) -> Path:
        """Verify media association and availability, returning the absolute filesystem Path.

        Raises KeyError if event or media is missing, ValueError if unavailable,
        PermissionError if path traversal is detected, or FileNotFoundError if missing on disk.
        """
        matching = await self._store.get_media(media_id, event_id=event_id)

        if not matching:
            raise KeyError(f"Media '{media_id}' not associated with event '{event_id}'")

        if not matching.available:
            raise ValueError(f"Media '{media_id}' is marked unavailable")

        # Verify containment and existence off-loop to avoid blocking the event loop
        def _verify_path_containment() -> tuple[Path, bool, bool]:
            media_root = self._store.media_root.resolve()
            target_path = (media_root / matching.file_name).resolve()
            is_contained = target_path.is_relative_to(media_root)
            exists = target_path.is_file() if is_contained else False
            return target_path, is_contained, exists

        target_path, is_contained, exists = await self._hass.async_add_executor_job(
            _verify_path_containment
        )

        if not is_contained:
            raise PermissionError(
                f"Path traversal detected: {target_path} outside {self._store.media_root}"
            )

        if not exists:
            raise FileNotFoundError(f"Media file not found on disk: {target_path}")

        return target_path

    def subscribe(self, callback: Callable[[str, int], None]) -> Callable[[], None]:
        """Subscribe to post-commit event revision updates."""
        return self._store.subscribe(callback)

    def register_consumer(
        self,
        consumer_id: str,
        retry_callback: Callable[[str], Awaitable[bool]] | None = None,
        available: bool = True,
        *,
        is_busy: Callable[[], Awaitable[bool]] | None = None,
    ) -> Callable[[], None]:
        """Register an external consumer for capabilities, retry delegation, and busy reporting."""
        reg = ConsumerRegistration(consumer_id, retry_callback, available, is_busy=is_busy)
        self._consumers[consumer_id] = reg

        def _unsubscribe() -> None:
            # Only remove if the current registration is still this exact instance
            if self._consumers.get(consumer_id) is reg:
                self._consumers.pop(consumer_id, None)

        return _unsubscribe

    async def is_consumer_busy(self, consumer_id: str) -> bool:
        """Return whether the registered consumer is currently busy processing jobs.

        Fails closed (returns True) if the consumer is unknown, has no is_busy callback,
        or if the is_busy callback raises an exception.
        """
        consumer = self._consumers.get(consumer_id)
        if not consumer or not consumer.is_busy:
            return True
        try:
            return await consumer.is_busy()
        except Exception:
            return True

    async def are_any_consumers_busy(self) -> bool:
        """Check all registered consumers; return True if any consumer reports busy or fails closed.

        Fails closed (returns True) if any consumer lacks an is_busy callback or raises an error.
        """
        for consumer in self._consumers.values():
            if not consumer.is_busy:
                return True
            try:
                if await consumer.is_busy():
                    return True
            except Exception:
                return True
        return False

    def get_registered_consumers(self) -> list[dict[str, Any]]:
        """Return list of registered consumers for WebSocket capabilities reporting."""
        return [
            {
                "id": c.id,
                "available": c.available,
                "can_retry": c.retry_callback is not None,
            }
            for c in self._consumers.values()
        ]

    async def trigger_consumer_retry(self, consumer_id: str, event_id: str) -> bool:
        """Trigger registered consumer retry callback."""
        consumer = self._consumers.get(consumer_id)
        if not consumer or not consumer.available or not consumer.retry_callback:
            return False
        try:
            return await consumer.retry_callback(event_id)
        except Exception:
            return False

    # -------------------------------------------------------------------------
    # Incident Methods Delegation
    # -------------------------------------------------------------------------

    async def get_incident(self, incident_id: str) -> Incident | None:
        """Async delegate to store.get_incident."""
        return await self._store.get_incident(incident_id)

    async def get_incident_inputs(
        self, incident_id: str, input_namespace: str
    ) -> IncidentInputs | None:
        """Async delegate to store.get_incident_inputs with canonical input token generation."""
        return await self._store.get_incident_inputs(incident_id, input_namespace)

    async def list_event_incidents(self, event_id: str) -> list[Incident]:
        """Async delegate to store.list_event_incidents."""
        return await self._store.list_event_incidents(event_id)

    async def get_incident_events(self, incident_id: str) -> list[RecordedEvent]:
        """Trusted internal delegate to store.get_incident_events."""
        return await self._store.get_incident_events(incident_id)

    async def reserve_incident_annotation(
        self,
        incident_id: str,
        namespace: str,
        expected_input_token: str,
        input_namespace: str,
        profile_digest: str,
        worker_id: str | None = None,
        ttl_seconds: float = 60.0,
    ) -> int | None:
        """Reserve an incident annotation slot with transactional CAS comparing fresh input token."""
        return await self._store.reserve_incident_annotation(
            incident_id,
            namespace,
            expected_input_token,
            input_namespace,
            profile_digest=profile_digest,
            worker_id=worker_id,
            ttl_seconds=ttl_seconds,
        )

    async def renew_incident_annotation_reservation(
        self,
        incident_id: str,
        namespace: str,
        expected_generation: int,
        expected_input_token: str,
        profile_digest: str,
        worker_id: str | None = None,
        ttl_seconds: float = 60.0,
    ) -> bool:
        """Renew the same current incident reservation without changing its generation."""
        return await self._store.renew_incident_annotation_reservation(
            incident_id=incident_id,
            namespace=namespace,
            expected_generation=expected_generation,
            expected_input_token=expected_input_token,
            profile_digest=profile_digest,
            worker_id=worker_id,
            ttl_seconds=ttl_seconds,
        )

    async def set_incident_annotation_cas(
        self,
        incident_id: str,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int,
        expected_input_token: str,
        profile_digest: str,
    ) -> bool:
        """Commit incident annotation result with transactional CAS comparing fresh input token."""
        return await self._store.set_incident_annotation_cas(
            incident_id,
            namespace,
            payload,
            expected_generation,
            expected_input_token,
            profile_digest=profile_digest,
        )

    def subscribe_incidents(self, callback: Callable[[str, int], None]) -> Callable[[], None]:
        """Subscribe to post-commit incident revision updates."""
        return self._store.subscribe_incidents(callback)

    @property
    def ffmpeg_binary(self) -> str | None:
        """Return available ffmpeg binary path, if configured."""
        try:
            from homeassistant.components.ffmpeg import get_ffmpeg_manager

            mgr = get_ffmpeg_manager(self._hass)
            if mgr and getattr(mgr, "binary", None):
                return str(mgr.binary)
        except Exception:
            pass
        return None


def get_api(hass: HomeAssistant) -> RecordingAPI | None:
    """Retrieve canonical RecordingAPI instance from Home Assistant."""
    domain_data = hass.data.get(DOMAIN)
    if not domain_data:
        return None
    return domain_data.get(DATA_API)
