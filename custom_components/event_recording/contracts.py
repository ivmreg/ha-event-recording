"""Pure consumer contract for recording, analysis, and test adapters."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Protocol

from .core.journal.common import DEFAULT_PIN_LEASE_SECONDS
from .core.models import Incident, IncidentInputs, MediaPinLease, RecordedEvent


class RecordingConsumerAPI(Protocol):
    @property
    def is_open(self) -> bool: ...

    async def list_finalized_media(
        self, cursor: str | None = None, limit: int = 50, namespace_lacking: str | None = None
    ) -> tuple[list[dict[str, Any]], str | None]: ...

    async def reserve_annotation(
        self,
        event_id: str,
        namespace: str,
        expected_media_revision: int,
        worker_id: str,
        ttl_seconds: float = 180.0,
    ) -> int | None: ...

    async def renew_annotation_reservation(
        self,
        event_id: str,
        namespace: str,
        expected_media_revision: int,
        expected_generation: int,
        worker_id: str,
        ttl_seconds: float = 180.0,
    ) -> bool: ...

    async def set_annotation_cas(
        self,
        event_id: str,
        expected_media_revision: int,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int | None = None,
        status: str | None = None,
    ) -> bool: ...

    async def pin_media_lease(
        self,
        event_id: str,
        lease_seconds: float = DEFAULT_PIN_LEASE_SECONDS,
        reason: str | None = None,
    ) -> MediaPinLease | None: ...

    async def release_media_pin(self, lease_token: str) -> bool: ...

    async def list_incidents(
        self, limit: int = 50, before_cursor: str | None = None
    ) -> tuple[list[Incident], str | None]: ...

    async def get_event(self, event_id: str) -> RecordedEvent | None: ...

    async def resolve_media(self, event_id: str, media_id: str) -> Path: ...

    def subscribe(self, callback: Callable[[str, int], None]) -> Callable[[], None]: ...

    def register_consumer(
        self,
        consumer_id: str,
        retry_callback: Callable[[str], Awaitable[bool]] | None = None,
        available: bool = True,
        *,
        is_busy: Callable[[], Awaitable[bool]] | None = None,
    ) -> Callable[[], None]: ...

    async def is_consumer_busy(self, consumer_id: str) -> bool: ...

    async def are_any_consumers_busy(self) -> bool: ...

    def get_registered_consumers(self) -> list[dict[str, Any]]: ...

    async def trigger_consumer_retry(self, consumer_id: str, event_id: str) -> bool: ...

    async def get_incident(self, incident_id: str) -> Incident | None: ...

    async def get_incident_inputs(
        self, incident_id: str, input_namespace: str
    ) -> IncidentInputs | None: ...

    async def list_event_incidents(self, event_id: str) -> list[Incident]: ...

    async def get_incident_events(self, incident_id: str) -> list[RecordedEvent]: ...

    async def reserve_incident_annotation(
        self,
        incident_id: str,
        namespace: str,
        expected_input_token: str,
        input_namespace: str,
        profile_digest: str,
        worker_id: str | None = None,
        ttl_seconds: float = 60.0,
    ) -> int | None: ...

    async def renew_incident_annotation_reservation(
        self,
        incident_id: str,
        namespace: str,
        expected_generation: int,
        expected_input_token: str,
        profile_digest: str,
        worker_id: str | None = None,
        ttl_seconds: float = 60.0,
    ) -> bool: ...

    async def set_incident_annotation_cas(
        self,
        incident_id: str,
        namespace: str,
        payload: dict[str, Any],
        expected_generation: int,
        expected_input_token: str,
        profile_digest: str,
    ) -> bool: ...

    def subscribe_incidents(self, callback: Callable[[str, int], None]) -> Callable[[], None]: ...
