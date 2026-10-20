"""Common types, constants, and base store definition for JournalStore modules.

Strict boundary: Pure Python, zero Home Assistant imports, zero AI imports.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..media import MediaProbe
from ..storage_ledger import StorageLedger, StoragePolicy

_LOGGER = logging.getLogger(__name__)

SCHEMA_VERSION = 4
MAX_PIN_LEASE_SECONDS = 900.0
DEFAULT_PIN_LEASE_SECONDS = 300.0


@dataclass(slots=True)
class CommitEffects:
    """Encapsulates post-commit side effects executed safely on the event loop."""

    result: Any
    unlinked_bytes: int = 0
    unlinked_count: int = 0
    incident_notifications: list[tuple[str, int]] = field(default_factory=list)


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    """Convert SQLite row tuple to string-keyed dictionary."""
    fields = [col[0] for col in cursor.description]
    return dict(zip(fields, row, strict=False))


class JournalStoreBase:
    """Base class declaring internal storage state and subscriber dispatch."""

    _db_path: Path
    _media_root: Path
    _allowed_media_roots: list[Path]
    _probe: MediaProbe | None
    _lock: threading.RLock
    _conn: sqlite3.Connection | None
    _is_open: bool
    _subscribers: set[Callable[[str, int], None]]
    _incident_subscribers: set[Callable[[str, int], None]]
    _active_promotion_futures: set[asyncio.Future[Any]]
    ledger: StorageLedger
    _storage_policy: StoragePolicy

    # Backup metadata
    _backup_in_progress: bool
    _last_backup_path: Path | None
    _last_backup_size_bytes: int
    _last_backup_created_at: str | None
    _last_backup_from_version: int | None
    _last_backup_to_version: int | None

    def _notify_subscribers(self, event_id: str, revision: int) -> None:
        """Dispatch revision notification to all registered event subscribers."""
        for cb in list(self._subscribers):
            try:
                cb(event_id, revision)
            except Exception as err:
                _LOGGER.warning("Error notifying store subscriber for %s: %s", event_id, err)

    def _notify_incident_subscribers(self, incident_id: str, revision: int) -> None:
        """Dispatch revision notification to all registered incident subscribers."""
        for cb in list(self._incident_subscribers):
            try:
                cb(incident_id, revision)
            except Exception as err:
                _LOGGER.warning("Error notifying incident subscriber for %s: %s", incident_id, err)
