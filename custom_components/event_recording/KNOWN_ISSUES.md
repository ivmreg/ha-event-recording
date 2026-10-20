# Known Issues & Concrete Operational Limitations (Event Recording)

This document describes operational limitations and architectural boundaries of the `event_recording` integration (version 0.5.0).

---

## 1. What Shipped in Version 0.5.0
- **Modularized JournalStore:** Decomposed `JournalStore` into cohesive domain mixins under `core/journal/` (`events.py`, `media.py`, `incidents.py`, `retention.py`, `maintenance.py`, `queries.py`), leaving a clean facade.
- **Dedicated Capture & Session Modules:** Separated recording execution and active session coordination into `capture.py` and `session.py`.
- **Zero-Copy Staging Promotion:** Optimized internal settled capture promotion (`adopt_staging=True`) in `core/media.py` using direct SHA-256 streaming and atomic rename (`os.replace`), eliminating duplicate userspace file copies on SSD while preserving strict copy semantics for external imports (`adopt_staging=False`). Added containment, symlink rejection, and fail-closed cross-device rollback.
- **Shared Declarative Validation:** Introduced pure-Python validation foundation (`core/validation.py`) and Home Assistant/Voluptuous bridge (`validation.py`) with `NumberSpec`, `StringSpec`, `IdSpec`, and `SpecNumberSelector`, rejecting `bool`, non-finite values, fractions, huge integer overflows, and control characters while preserving UI selector metadata.

---

## 2. Pre-Trigger Video Capture
- **Best-Effort Lookback**: Native Home Assistant lookback may reuse an already active HLS buffer, but cannot guarantee pre-trigger footage; cold streams may yield none. go2rtc pre-roll comes from a bounded in-memory HLS fragment ring. It warms after startup or reconnect, can be short after outages, and repeated capture chunks may have gaps; absolute visual UTC remains unknown without a calibrated source clock.
- **go2rtc Coverage**: go2rtc captures begin after the consumer attaches and wait for a video keyframe. Bounded MP4 duration is a wall-clock timeout, not proof of media coverage. `extend` requests repeated bounded chunks, which may have gaps at startup or handoff. Probed duration and partial state are used to report material shortfalls; exact visual timestamps remain unknown.

## 3. Chunk Handoff Coverage & Timing Metadata
- **Best-Effort Segments & Source Media Bounds**: Native chunk handoff coverage depends on Home Assistant and stream keyframe timing. Coverage is best effort and has not been measured as gapless. Recording duration reflects actual received stream segment duration, with no wall-clock-duration promise beyond source media.
- **Monotonic DTS Normalization**: An integration-owned stream remuxer normalizes DTS per stream to strictly monotonically increasing order to prevent MP4 container aborts (`[mp4] Application provided invalid, non monotonically increasing dts to muxer`). While this remedies mux failures and preserves B-frame composition offsets and A/V alignment, it cannot promise damaged camera source timing is physically faithful if the encoder emitted duplicate timestamps. Packets with absent timestamps fail explicitly rather than inventing artificial clocks.
- **Native Lookback/Chunk Boundary Timing**: Boundary timing metadata remains `"unknown"` or `"estimated"` without an NVR provider; native camera streams provide no verified PTS timestamps.

## 4. Snapshot Recording Mode
- **Still Images Only**: Native snapshot mode captures single static images (`image/jpeg` or `image/png`) upon trigger activation. It does not record video clips or animated sequences.

## 5. Overlapping Requests & Concurrency Bounds
- **Bounded Capacity**: The system enforces hard concurrency limits (`max_parallel_resources`, 1-8) and queue depth bounds (`max_queue_depth`, 1-256) rather than unbounded queuing.
- **Admission & Quarantine**: Storage capacity admission failure is reported as `STORAGE_CAPACITY_EXCEEDED`. A writer is quarantined only when it remains unsettled after timeout or cancellation.
- **Writer Ownership**: Stream coordination covers this integration's writers. Other integrations or automations recording the same camera can still compete with it.

## 6. Visual UTC Timestamp Provenance
- **Unverified Camera Clocks**: Frame timestamps and media capture times report `time_quality = "unknown"` or `"estimated"` unless calibrated against an authoritative NTP or NVR clock provider.
- **Requested Window vs. Visual Time**: Event `start_at` is the logical requested-window start (`occurred_at` minus requested lookback), not intake time or a verified image timestamp.

## 7. Storage Characteristics & SSD Notes
- **Durable Disk/SSD Archive**: Media recordings are stored durably on disk/SSD by default.
- **Logical Writes vs. Flash Wear**: Zero-copy staging adoption eliminates redundant application-level file copies, but physical SSD write amplification depends on controller wear leveling, erase block boundaries, and filesystem caching.
- **Process Restart vs. Host Reboot**: Data on an OS `tmpfs` mount survives Home Assistant process-only restarts where the mount namespace remains intact, but is lost upon host reboot or container recreation.

---

## 8. Explicitly Deferred Features (Not Bugs)
- **Single Combined Queue DB**: Kept separate SQLite database files across recording, vision analysis, and fusion to avoid high-risk schema migrations.
- **Grouped Multi-Camera Timeline Presentation**: Grouped multi-camera aggregation in the Lovelace card remains deferred; events are presented per-event with filtering and keyset pagination.
- **Storage Root Migration Wizard**: Dedicated in-flight storage-root migration wizards are deferred. Storage directories are configured at setup time.
- **Dedicated RAM-Only (`tmpfs`) Recording Archive**: Media assets remain durably written to disk/SSD storage roots by default.
- **Minimum Dependency Requirement**: `event_vision 0.4.0` requires `event_recording >= 0.5.0` for shared validation modules. Operators must update both integrations in HACS before restarting Home Assistant.
