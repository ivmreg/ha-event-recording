"""Durable SQLite JournalStore for Event Recording.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
Thread-safe single-writer off-loop execution with WAL, synchronous=FULL,
atomic intent admission, media-set revisioning, and CAS annotations.

This module acts as the public facade, composing cohesive domain mixins
under `.journal`:
- `MigrationsMixin`: Database connection lifecycle, backups, SQLite schema & migrations.
- `EventsMixin`: Event hydration, query filtering, keyset pagination, and deletion.
- `MediaMixin`: Media lookup, storage capacity reservations, and atomic rename promotion.
- `SessionsMixin`: Recording intent admission, resource sessions, and capture states.
- `PinsRetentionMixin`: Media retention enforcement, pin lease protection, and crash reconciliation.
- `IncidentsMixin`: Incident tracking, CAS annotations, input token hashing, and auto-association.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any

from .journal import (
    DEFAULT_PIN_LEASE_SECONDS,
    MAX_PIN_LEASE_SECONDS,
    SCHEMA_VERSION,
    CommitEffects,
    EventsMixin,
    IncidentsMixin,
    JournalStoreBase,
    MediaMixin,
    MigrationsMixin,
    PinsRetentionMixin,
    SessionsMixin,
    _dict_factory,
)
from .media import (
    MediaProbe,
    MediaTypeMismatchError,
    ProbeUnavailableError,
    stage_and_promote_media,
    unlink_promoted_media,
    verify_media_path,
)
from .models import utc_now
from .storage_ledger import (
    InFlightReservation,
    StorageCapacityError,
    StorageLedger,
    StoragePolicy,
)

_LOGGER = logging.getLogger(__name__)


class JournalStore(
    MigrationsMixin,
    EventsMixin,
    MediaMixin,
    SessionsMixin,
    PinsRetentionMixin,
    IncidentsMixin,
    JournalStoreBase,
):
    """Neutral event journal and media store backed by SQLite."""

    def __init__(
        self,
        db_path: str | Path,
        media_root: str | Path,
        allowed_media_roots: Sequence[str | Path] | None = None,
        probe: MediaProbe | None = None,
    ) -> None:
        self._db_path_raw = Path(db_path)
        self._media_root_raw = Path(media_root)
        self._allowed_media_roots_raw = [
            Path(p) for p in (allowed_media_roots or [])
        ]
        self._db_path = self._db_path_raw
        self._media_root = self._media_root_raw
        self._allowed_media_roots = [self._media_root, *self._allowed_media_roots_raw]
        self._probe = probe
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._is_open = False
        self._subscribers: set[Callable[[str, int], None]] = set()
        self._incident_subscribers: set[Callable[[str, int], None]] = set()
        self._active_promotion_futures: set[asyncio.Future[Any]] = set()
        self.ledger = StorageLedger()
        self._storage_policy: StoragePolicy = StoragePolicy(min_disk_free_mb=50, retention_size_mb=None)

        # Backup metadata
        self._backup_in_progress = False
        self._last_backup_path = None
        self._last_backup_size_bytes = 0
        self._last_backup_created_at = None
        self._last_backup_from_version = None
        self._last_backup_to_version = None

    @property
    def storage_policy(self) -> StoragePolicy:
        """Return the current storage policy."""
        with self._lock:
            return self._storage_policy

    def set_storage_policy(self, policy: StoragePolicy) -> None:
        """Update the storage policy under store lock."""
        with self._lock:
            self._storage_policy = policy

    @property
    def storage_ledger(self) -> StorageLedger:
        """Return the maintained storage ledger."""
        return self.ledger

    @property
    def is_open(self) -> bool:
        """Return whether SQLite connection is active."""
        return self._is_open

    @property
    def media_root(self) -> Path:
        """Return resolved owned media root."""
        return self._media_root

    def subscribe(self, callback: Callable[[str, int], None]) -> Callable[[], None]:
        """Subscribe to post-commit event revision updates.

        Dispatched outside the SQLite lock on the event loop.
        """
        self._subscribers.add(callback)

        def _unsub() -> None:
            self._subscribers.discard(callback)

        return _unsub

    def subscribe_incidents(self, callback: Callable[[str, int], None]) -> Callable[[], None]:
        """Subscribe to post-commit incident revision updates.

        Dispatched outside the SQLite lock on the event loop.
        """
        self._incident_subscribers.add(callback)

        def _unsub() -> None:
            self._incident_subscribers.discard(callback)

        return _unsub


__all__ = [
    "DEFAULT_PIN_LEASE_SECONDS",
    "MAX_PIN_LEASE_SECONDS",
    "SCHEMA_VERSION",
    "CommitEffects",
    "InFlightReservation",
    "JournalStore",
    "MediaProbe",
    "MediaTypeMismatchError",
    "ProbeUnavailableError",
    "StorageCapacityError",
    "StorageLedger",
    "StoragePolicy",
    "_dict_factory",
    "stage_and_promote_media",
    "unlink_promoted_media",
    "utc_now",
    "verify_media_path",
]
