# Event Recording

Durable event journaling, optional camera capture, private media, and a timeline card. Recording runs independently of analysis. Native Home Assistant automations supply arbitrary triggers; no camera or cloud key is required for event-only use.

Requires Home Assistant **2026.10.1** or later. Version **0.7.18**.

## HACS custom repository

In HACS > Custom repositories, add the following repository with type **Integration**:

`ivmreg/ha-event-recording`

HACS requires public GitHub repositories; a private staging mirror is not installable.

## Setup

1. Download **Event Recording** from HACS, then restart Home Assistant.
2. Add **Event Recording** under Settings > Devices & services.
3. Keep the default private media directory, `event_recording_media`, or choose another private directory.
4. For native camera recording, merge `/config/event_recording_media` into `homeassistant.allowlist_external_dirs`, validate configuration, and restart.
5. Register `/event_recording/event-card.js?v=0.7.18` as a JavaScript module dashboard resource.
6. Add a card with `type: custom:event-recording-card` and `page_size: 20`.

The card is bundled with this integration and updates with it. No separate card repository is required.

The optional analyzer is distributed separately: [Event Vision](https://github.com/ivmreg/ha-event-vision-hacs).

See [the installation guide](docs/install.md) for configuration and automation examples.

## Known Limitations

Concrete operational limitations and architectural boundaries are documented in `custom_components/event_recording/KNOWN_ISSUES.md` and visible in the installed integration Options screen.

This distribution is generated from the source identified in `SOURCE.json`. It contains one integration only; local recordings, API keys, and Home Assistant configuration are not part of the package.
