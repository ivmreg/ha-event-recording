# Known Issues & Concrete Operational Limitations (Event Recording)

This document describes operational limitations and architectural boundaries of the `event_recording` integration (version 0.4.0).

---

## 1. Pre-Trigger Video Capture
- **Best-Effort Lookback**: Native Home Assistant lookback may reuse an already active HLS buffer, but cannot guarantee pre-trigger footage; cold streams may yield none. An external retained-video provider such as Frigate is optional, and no NVR is required.

## 2. Chunk Handoff Coverage
- **Best-Effort Segments**: Native chunk handoff coverage depends on Home Assistant and stream keyframe timing. Coverage is best effort and has not been measured as gapless.

## 3. Snapshot Recording Mode
- **Still Images Only**: Native snapshot mode captures single static images (`image/jpeg` or `image/png`) upon trigger activation. It does not record video clips or animated sequences.

## 4. Overlapping Requests & Concurrency Bounds
- **Bounded Capacity**: The system enforces hard concurrency limits (`max_parallel_resources`, 1-8) and queue depth bounds (`max_queue_depth`, 1-256) rather than unbounded queuing.
- **Admission & Quarantine**: Storage capacity admission failure is reported as `STORAGE_CAPACITY_EXCEEDED`. A writer is quarantined only when it remains unsettled after timeout or cancellation.
- **Writer Ownership**: Stream coordination covers this integration's writers. Other integrations or automations recording the same camera can still compete with it.

## 5. Visual UTC Timestamp Provenance
- **Unverified Camera Clocks**: Frame timestamps and media capture times report `time_quality = "unknown"` or `"estimated"` unless calibrated against an authoritative NTP or NVR clock provider.
- **Requested Window vs. Visual Time**: Event `start_at` is the logical requested-window start (`occurred_at` minus requested lookback), not intake time or a verified image timestamp.

## 6. Storage Migration & Timeline Presentation
- **Storage Root Migration UI**: Dedicated in-flight storage-root migration wizards are deferred. Storage directories are selected at setup time.
- **Grouped Card Presentation**: Multi-camera grouped card aggregation remains deferred in frontend Lovelace cards; events are listed per-event with filtering and keyset pagination.

## 7. Deferred Structural Evolutions & Scope Boundaries
- **JournalStore Decomposition & Capture Strategy Extraction**: Broad modular decomposition of `JournalStore` and extraction of capture strategy pipelines are deferred structural follow-ups. Correctness, lifecycle safety helpers (`CommitEffects`), active media pin enforcement, honest clock handling, and staging cleanup are included in this release.
- **Shared Coordination Scope**: Stream and resource session coordination is enforced only within this integration; external integrations recording the same camera streams are not centrally coordinated.
- **History Migration**: Additive schema upgrades retain a pre-upgrade SQLite backup. A history relocation wizard is not included.
- **Vision Queue Architecture**: The unified generic job queue rewrite across single-event and incident jobs is deferred under disjoint `event_vision` component ownership.
