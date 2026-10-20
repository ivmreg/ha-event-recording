"""Storage accounting ledger for Event Recording.

Maintains committed media statistics, per-asset in-flight reservations,
atomic charge transitions upon promotion, fail-closed physical disk headroom checks,
and periodic startup/maintenance reconciliation.
"""

from __future__ import annotations

import logging
import os
import shutil
import sqlite3
import stat
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger(__name__)


class StorageCapacityError(Exception):
    """Raised when storage admission fails due to physical disk or retention limits."""


@dataclass(frozen=True)
class StoragePolicy:
    """Configured storage admission and retention policy."""

    min_disk_free_mb: int = 50
    retention_size_mb: int | None = None  # None or <= 0 means disabled


@dataclass
class InFlightReservation:
    """Represents an active in-flight capture or import reservation."""

    reservation_id: str
    target_path: Path
    estimated_bytes: int
    sidecar_paths: tuple[Path, ...] = ()
    is_quarantined: bool = False
    created_at: float = 0.0


class StorageLedger:
    """Coherent maintained storage ledger.

    Prevents double-counting active assets and staging bytes by charging:
        charge = max(estimated_bytes, current_file_bytes)
    per in-flight reservation, atomically moving the charge to committed
    bytes upon promotion.
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._committed_bytes: int = 0
        self._committed_count: int = 0
        self._reservations: dict[str, InFlightReservation] = {}
        self._staging_unreserved_bytes: int = 0
        self._orphan_unreserved_bytes: int = 0

    @property
    def committed_bytes(self) -> int:
        with self._lock:
            return self._committed_bytes

    @property
    def committed_count(self) -> int:
        with self._lock:
            return self._committed_count

    def get_reservation(self, reservation_id: str) -> InFlightReservation | None:
        with self._lock:
            return self._reservations.get(reservation_id)

    def get_all_reservations(self) -> dict[str, InFlightReservation]:
        with self._lock:
            return dict(self._reservations)

    def transfer_reservations(self, reservations: dict[str, InFlightReservation]) -> None:
        """Adopt surviving in-flight reservations from prior registry / coordinator."""
        with self._lock:
            for r_id, res in reservations.items():
                if r_id not in self._reservations:
                    self._reservations[r_id] = res

    def _file_bytes_safe(self, path: Path) -> int:
        try:
            st = path.stat()
            return st.st_size if stat.S_ISREG(st.st_mode) else 0
        except FileNotFoundError:
            return 0
        except Exception as err:
            _LOGGER.warning("Stat failed on '%s' (failing closed): %s", path, err)
            raise StorageCapacityError(f"Filesystem error inspecting '{path}': {err}") from err

    def reservation_physical_bytes(self, res: InFlightReservation) -> int:
        """Sum physical bytes of primary target and all declared sidecars (.tmp, etc.)."""
        total = self._file_bytes_safe(res.target_path)
        for sidecar in res.sidecar_paths:
            total += self._file_bytes_safe(sidecar)
        return total

    def reservation_charge(self, res: InFlightReservation) -> int:
        """Calculate active charge: max of remaining estimated bytes or current disk bytes."""
        actual_bytes = self.reservation_physical_bytes(res)
        return max(res.estimated_bytes, actual_bytes)

    def total_in_flight_charge(self) -> int:
        """Sum charges across all active in-flight reservations."""
        with self._lock:
            return sum(self.reservation_charge(res) for res in self._reservations.values())

    def total_unwritten_reserved_bytes(self) -> int:
        """Sum unwritten expected bytes (estimated - actual) for physical disk headroom."""
        with self._lock:
            total = 0
            for res in self._reservations.values():
                actual = self.reservation_physical_bytes(res)
                total += max(0, res.estimated_bytes - actual)
            return total

    def reserve(
        self,
        reservation_id: str,
        target_path: Path,
        estimated_bytes: int,
        sidecar_paths: tuple[Path, ...] = (),
    ) -> InFlightReservation:
        """Register an in-flight reservation before capture starts."""
        with self._lock:
            res = InFlightReservation(
                reservation_id=reservation_id,
                target_path=target_path,
                estimated_bytes=max(0, int(estimated_bytes)),
                sidecar_paths=tuple(sidecar_paths),
                is_quarantined=False,
                created_at=time.time(),
            )
            self._reservations[reservation_id] = res
            return res

    def mark_quarantined(self, reservation_id: str) -> None:
        """Mark an in-flight reservation as quarantined awaiting task settlement."""
        with self._lock:
            res = self._reservations.get(reservation_id)
            if res:
                res.is_quarantined = True

    def release_reservation(self, reservation_id: str, force: bool = False) -> None:
        """Release reservation if task is completed or force=True (settled)."""
        with self._lock:
            res = self._reservations.get(reservation_id)
            if not res:
                return
            if res.is_quarantined and not force:
                _LOGGER.debug(
                    "Reservation '%s' is quarantined; retaining reservation charge until settled.",
                    reservation_id,
                )
                return
            self._reservations.pop(reservation_id, None)

    def commit_media(self, reservation_id: str | None, final_bytes: int) -> None:
        """Atomically transition in-flight reservation charge into committed bytes."""
        with self._lock:
            if reservation_id and reservation_id in self._reservations:
                del self._reservations[reservation_id]
            self._committed_bytes += max(0, int(final_bytes))
            self._committed_count += 1

    def record_unlinked(self, freed_bytes: int, count: int = 1) -> None:
        """Subtract unlinked media bytes and file count following retention eviction."""
        with self._lock:
            self._committed_bytes = max(0, self._committed_bytes - int(freed_bytes))
            self._committed_count = max(0, self._committed_count - int(count))

    def check_admission(
        self,
        required_bytes: int,
        media_root: Path,
        policy: StoragePolicy | None = None,
        min_disk_free_mb: int | None = None,
        retention_size_mb: int | None = None,
    ) -> None:
        """Check capacity and physical disk headroom before starting any capture or import.

        Fails closed on any stat, path, or disk check failure.
        """
        effective_floor_mb = 50
        if min_disk_free_mb is not None:
            effective_floor_mb = max(50, int(min_disk_free_mb))
        elif policy is not None:
            effective_floor_mb = max(50, int(policy.min_disk_free_mb))

        floor_bytes = effective_floor_mb * 1024 * 1024

        try:
            free_bytes = shutil.disk_usage(media_root).free
        except Exception as err:
            raise StorageCapacityError(
                f"Failed to check disk usage for '{media_root}': {err}"
            ) from err

        unwritten = self.total_unwritten_reserved_bytes()
        total_physical_needed = unwritten + required_bytes + floor_bytes
        if free_bytes < total_physical_needed:
            raise StorageCapacityError(
                f"Insufficient physical disk space on '{media_root}': "
                f"free={free_bytes}B, needed={total_physical_needed}B "
                f"(unwritten_reserved={unwritten}B, required={required_bytes}B, floor={floor_bytes}B)"
            )

        effective_budget_mb = (
            retention_size_mb
            if retention_size_mb is not None
            else (policy.retention_size_mb if policy else None)
        )
        if effective_budget_mb is not None and effective_budget_mb > 0:
            budget_bytes = int(effective_budget_mb) * 1024 * 1024
            projected = (
                self.committed_bytes
                + self.total_in_flight_charge()
                + self._staging_unreserved_bytes
                + self._orphan_unreserved_bytes
                + required_bytes
            )
            if projected > budget_bytes:
                raise StorageCapacityError(
                    f"Projected storage ({projected}B) exceeds configured retention budget ({budget_bytes}B)"
                )

    def check_and_reserve(
        self,
        reservation_id: str,
        target_path: Path,
        estimated_bytes: int,
        media_root: Path,
        policy: StoragePolicy | None = None,
        sidecar_paths: tuple[Path, ...] = (),
    ) -> InFlightReservation:
        """Atomically check admission and register in-flight reservation."""
        with self._lock:
            self.check_admission(
                required_bytes=estimated_bytes,
                media_root=media_root,
                policy=policy,
            )
            return self.reserve(
                reservation_id,
                target_path,
                estimated_bytes,
                sidecar_paths=sidecar_paths,
            )

    def check_and_admit_reservation(
        self,
        reservation_id: str,
        target_path: Path,
        actual_bytes: int,
        media_root: Path,
        policy: StoragePolicy | None = None,
    ) -> None:
        """Atomically check admission for reservation growth, excluding own reservation charge."""
        with self._lock:
            res = self._reservations.get(reservation_id)
            if res is None:
                self.check_admission(
                    required_bytes=actual_bytes,
                    media_root=media_root,
                    policy=policy,
                )
                return

            resulting_charge = max(res.estimated_bytes, actual_bytes)

            effective_floor_mb = 50
            if policy is not None and policy.min_disk_free_mb is not None:
                effective_floor_mb = max(50, int(policy.min_disk_free_mb))
            floor_bytes = effective_floor_mb * 1024 * 1024

            try:
                free_bytes = shutil.disk_usage(media_root).free
            except Exception as err:
                raise StorageCapacityError(
                    f"Failed to check disk usage for '{media_root}': {err}"
                ) from err

            other_unwritten = 0
            for r_id, r in self._reservations.items():
                if r_id != reservation_id:
                    other_actual = self.reservation_physical_bytes(r)
                    other_unwritten += max(0, r.estimated_bytes - other_actual)

            curr_disk_bytes = self.reservation_physical_bytes(res)
            additional_disk_needed = max(0, actual_bytes - curr_disk_bytes)
            peak_copy_bytes = actual_bytes
            total_physical_needed = (
                other_unwritten + additional_disk_needed + peak_copy_bytes + floor_bytes
            )

            if free_bytes < total_physical_needed:
                raise StorageCapacityError(
                    f"Insufficient physical disk space on '{media_root}': "
                    f"free={free_bytes}B, needed={total_physical_needed}B "
                    f"(other_unwritten={other_unwritten}B, additional_needed={additional_disk_needed}B, "
                    f"peak_copy={peak_copy_bytes}B, floor={floor_bytes}B)"
                )

            effective_budget_mb = policy.retention_size_mb if policy else None
            if effective_budget_mb is not None and effective_budget_mb > 0:
                budget_bytes = int(effective_budget_mb) * 1024 * 1024
                other_in_flight = sum(
                    self.reservation_charge(r)
                    for r_id, r in self._reservations.items()
                    if r_id != reservation_id
                )
                projected = (
                    self._committed_bytes
                    + other_in_flight
                    + resulting_charge
                    + self._staging_unreserved_bytes
                    + self._orphan_unreserved_bytes
                )
                if projected > budget_bytes:
                    raise StorageCapacityError(
                        f"Projected storage ({projected}B) exceeds configured retention budget ({budget_bytes}B)"
                    )

            res.estimated_bytes = max(res.estimated_bytes, actual_bytes)

    def reconcile(self, conn: sqlite3.Connection, media_root: Path) -> dict[str, int]:
        """Perform storage-only reconciliation against SQLite and owned media root files.

        Discovers committed available rows, unreferenced rows, active in-flight reservations,
        untracked staging files, and unreserved root orphans / leftovers.
        """
        with self._lock:
            cur = conn.cursor()
            cur.execute("PRAGMA table_info(media);")
            cols: set[str] = set()
            for r in cur.fetchall():
                if isinstance(r, dict):
                    cols.add(r.get("name", ""))
                elif isinstance(r, sqlite3.Row):
                    cols.add(r["name"])
                elif isinstance(r, (tuple, list)):
                    cols.add(r[1])
                else:
                    try:
                        cols.add(r["name"])
                    except Exception:
                        cols.add(r[1])
            has_file_name = "file_name" in cols
            if has_file_name:
                cur.execute(
                    "SELECT file_name, size_bytes, available FROM media WHERE available = 1;"
                )
                media_rows = cur.fetchall()
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name='media_deletions';"
                )
                if cur.fetchone():
                    cur.execute(
                        "SELECT file_name, size_bytes, 1 AS available FROM media_deletions;"
                    )
                    media_rows.extend(cur.fetchall())
            else:
                cur.execute("SELECT size_bytes, available FROM media;")
            rows = media_rows if has_file_name else cur.fetchall()
            committed_bytes = 0
            committed_count = 0
            available_files: set[str] = set()

            for r in rows:
                if isinstance(r, dict):
                    fn = r.get("file_name") if has_file_name else None
                    sz = int(r.get("size_bytes", 0) or 0)
                    avail = bool(r.get("available", 0))
                elif isinstance(r, sqlite3.Row):
                    fn = r["file_name"] if has_file_name else None
                    sz = int(r["size_bytes"]) if r["size_bytes"] is not None else 0
                    avail = bool(r["available"])
                else:
                    if has_file_name:
                        fn = r[0]
                        sz = int(r[1]) if r[1] is not None else 0
                        avail = bool(r[2])
                    else:
                        fn = None
                        sz = int(r[0]) if r[0] is not None else 0
                        avail = bool(r[1])
                if avail:
                    committed_bytes += sz
                    committed_count += 1
                    if fn:
                        available_files.add(fn.replace("\\", "/"))

            self._committed_bytes = committed_bytes
            self._committed_count = committed_count

            staging_dir = media_root / ".staging"
            staging_unreserved = 0
            tracked_paths: set[Path] = set()
            for res in self._reservations.values():
                try:
                    tracked_paths.add(res.target_path.resolve())
                except Exception:
                    pass
                for sc in res.sidecar_paths:
                    try:
                        tracked_paths.add(sc.resolve())
                    except Exception:
                        pass

            if staging_dir.is_dir():
                for p in staging_dir.iterdir():
                    try:
                        if p.is_file() and p.resolve() not in tracked_paths:
                            staging_unreserved += self._file_bytes_safe(p)
                    except FileNotFoundError:
                        pass

            self._staging_unreserved_bytes = staging_unreserved

            # Scan root directory and all nested paths for untracked orphans and unavailable leftovers
            orphan_unreserved = 0

            def _raise_walk_error(err: OSError) -> None:
                raise StorageCapacityError(
                    f"Filesystem error inspecting '{media_root}': {err}"
                ) from err

            try:
                if media_root.is_dir():
                    for root_str, dirs, files in os.walk(media_root, onerror=_raise_walk_error):
                        root_path = Path(root_str)
                        if ".staging" in dirs:
                            dirs.remove(".staging")
                        for file_name in files:
                            if file_name.startswith("event_recording.db"):
                                continue
                            p = root_path / file_name
                            try:
                                rel_posix = p.relative_to(media_root).as_posix()
                            except Exception:
                                rel_posix = p.name
                            try:
                                p_res = p.resolve()
                            except Exception:
                                p_res = p
                            if p_res in tracked_paths:
                                continue
                            if rel_posix in available_files:
                                continue
                            # Not an active reservation, not an available media row.
                            # Count physical bytes as unreserved usage (leftover or orphan).
                            orphan_unreserved += self._file_bytes_safe(p)
            except (StorageCapacityError, OSError) as walk_err:
                if isinstance(walk_err, StorageCapacityError):
                    raise
                if not isinstance(walk_err, FileNotFoundError):
                    raise StorageCapacityError(
                        f"Filesystem error inspecting '{media_root}': {walk_err}"
                    ) from walk_err

            self._orphan_unreserved_bytes = orphan_unreserved

            return {
                "committed_bytes": self._committed_bytes,
                "committed_count": self._committed_count,
                "in_flight_charge": sum(
                    self.reservation_charge(res) for res in self._reservations.values()
                ),
                "staging_unreserved_bytes": self._staging_unreserved_bytes,
                "orphan_unreserved_bytes": self._orphan_unreserved_bytes,
            }

    def stats(self) -> dict[str, Any]:
        """Return snapshot of ledger state."""
        with self._lock:
            return {
                "committed_bytes": self._committed_bytes,
                "committed_count": self._committed_count,
                "in_flight_charge": sum(
                    self.reservation_charge(res) for res in self._reservations.values()
                ),
                "active_reservations_count": len(self._reservations),
                "staging_unreserved_bytes": self._staging_unreserved_bytes,
                "orphan_unreserved_bytes": self._orphan_unreserved_bytes,
            }
