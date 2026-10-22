"""Bounded runtime repair, separate from cold-start filesystem reconciliation."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .const import (
    ERROR_CAPTURE_TASK_CANCELLED,
    ERROR_CAPTURE_TASK_FAILED,
    ERROR_RECORDING_INTERRUPTED,
)

_LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class CaptureOwner:
    """Exact event registered before a wrapper can start physical work."""

    resource_key: str
    event_id: str
    ownership_group: str | None = None


class CaptureTaskSupervisor:
    """Repair exited wrappers and abandoned durable ownership, eight at a time.

    Dispatch registration and repair share the coordinator admission lock.
    SQLite rechecks pending intents and terminal state before each mutation.
    Registry settlement remains authoritative for physical writer ownership.
    """

    PENDING_LIMIT = 64
    BATCH_SIZE = 8

    def __init__(self, coordinator: Any) -> None:
        self.coordinator = coordinator
        self.repair_grace_seconds = 5.0
        self.sweep_interval_seconds = 2.0
        self._pending: OrderedDict[CaptureOwner, str] = OrderedDict()
        self._missing_identity: OrderedDict[str, None] = OrderedDict()
        self._cursor: tuple[datetime, str] | None = None
        self._requested = True
        self._last_sweep = float("-inf")

    def request_repair(self) -> None:
        self._requested = True
        self.coordinator.notify_new_intent()

    def submit(self, owner: CaptureOwner, *, cancelled: bool, failed: bool) -> None:
        reason = (
            ERROR_CAPTURE_TASK_CANCELLED
            if cancelled
            else ERROR_CAPTURE_TASK_FAILED
            if failed
            else ERROR_RECORDING_INTERRUPTED
        )
        self._pending[owner] = reason
        self._pending.move_to_end(owner)
        while len(self._pending) > self.PENDING_LIMIT:
            self._pending.popitem(last=False)  # Durable sweep is the fallback.
        self.request_repair()

    def close(self) -> None:
        self._pending.clear()
        self._missing_identity.clear()
        self._cursor = None

    async def _settle(self, owner: CaptureOwner, reason: str) -> bool:
        coordinator = self.coordinator
        # Keep dispatch ownership registered through the off-loop transaction.
        async with coordinator.admission_lock:
            if owner.event_id in coordinator.active_capture_event_ids():
                return False
            registry = coordinator.registry
            if registry.is_quarantined(owner.resource_key, owner.ownership_group):
                return False
            if registry.is_busy(owner.resource_key, owner.ownership_group):
                return False
            result = await coordinator.store.settle_abandoned_capture(
                event_id=owner.event_id,
                resource_key=owner.resource_key,
                error_code=reason,
                now=coordinator.now(),
            )
        if result.status == "extended":
            return False
        if result.status == "finished":
            _LOGGER.warning(
                "Recovered abandoned capture as %s (%s)",
                result.event.capture_state,
                reason,
            )
        return True

    async def maintain(self) -> None:
        """Run only while loaded; DB errors defer work to a later bounded pass."""
        if not self.coordinator.running:
            return
        now = asyncio.get_running_loop().time()
        if not self._requested and now - self._last_sweep < self.sweep_interval_seconds:
            return
        self._last_sweep = now
        self._requested = False
        try:
            # Snapshot distinct entries: a deferred owner is tried once per pass.
            for owner, reason in list(self._pending.items())[: self.BATCH_SIZE]:
                if await self._try_settle(owner, reason):
                    self._pending.pop(owner, None)
                else:
                    self._pending.move_to_end(owner)
            await self.sweep_once()
        except asyncio.CancelledError:
            raise
        except Exception as err:
            # In particular, closed/unavailable SQLite is not evidence of safety.
            _LOGGER.debug("Capture repair deferred: %s", type(err).__name__)

    async def _try_settle(self, owner: CaptureOwner, reason: str) -> bool:
        try:
            return await self._settle(owner, reason)
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.debug("Capture owner repair deferred: %s", type(err).__name__)
            return False

    async def sweep_once(self, limit: int = BATCH_SIZE) -> int:
        """Rotate across inflight rows so live/unrepairable rows cannot starve peers."""
        if not self.coordinator.running:
            return 0
        after_time, after_id = self._cursor or (None, None)
        candidates = await self.coordinator.store.find_abandoned_capture_candidates(
            older_than=self.coordinator.now() - timedelta(seconds=self.repair_grace_seconds),
            limit=limit,
            after_updated_at=after_time,
            after_event_id=after_id,
        )
        self._cursor = (
            (candidates[-1].updated_at, candidates[-1].event_id)
            if len(candidates) == limit
            else None
        )
        settled = 0
        for candidate in candidates:
            if not candidate.resource_key:
                if candidate.event_id not in self._missing_identity:
                    _LOGGER.warning(
                        "Capture repair needs durable resource identity; "
                        "both intent and session evidence are missing"
                    )
                self._missing_identity[candidate.event_id] = None
                self._missing_identity.move_to_end(candidate.event_id)
                while len(self._missing_identity) > self.PENDING_LIMIT:
                    self._missing_identity.popitem(last=False)
                continue
            owner = CaptureOwner(
                candidate.resource_key, candidate.event_id, candidate.ownership_group
            )
            if await self._try_settle(owner, ERROR_RECORDING_INTERRUPTED):
                settled += 1
        return settled
