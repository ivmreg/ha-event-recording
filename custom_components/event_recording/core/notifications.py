"""Bounded, coalescing revision invalidations; source facts remain in the journal."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable

_LOGGER = logging.getLogger(__name__)


class RevisionDispatcher:
    """One task per subscriber, with latest-revision delivery and overflow catch-up."""

    def __init__(
        self,
        handler: Callable[[str, int], Awaitable[None]],
        catch_up: Callable[[], Awaitable[None]],
        max_pending: int = 1024,
    ) -> None:
        self._handler = handler
        self._catch_up = catch_up
        self._limit = max_pending
        self._pending: OrderedDict[str, int] = OrderedDict()
        self._overflow = False
        self._closed = False
        self._task: asyncio.Task[None] | None = None

    def notify(self, object_id: str, revision: int) -> None:
        if self._closed:
            return
        if object_id not in self._pending and len(self._pending) >= self._limit:
            self._overflow = True
        else:
            self._pending[object_id] = max(revision, self._pending.get(object_id, 0))
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._drain())

    async def _drain(self) -> None:
        while self._pending or self._overflow:
            try:
                if self._overflow:
                    self._overflow = False
                    await self._catch_up()
                else:
                    object_id, revision = self._pending.popitem(last=False)
                    await self._handler(object_id, revision)
            except Exception as err:
                _LOGGER.warning("Revision dispatch failed: %s", type(err).__name__)
            await asyncio.sleep(0)

    def close(self) -> None:
        self._closed = True
        self._pending.clear()
        self._overflow = False
        if self._task:
            self._task.cancel()

    async def wait(self) -> None:
        if self._task:
            await asyncio.gather(self._task, return_exceptions=True)
