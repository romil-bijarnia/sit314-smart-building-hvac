# Domain contract

The dependency-free CommonJS domain module exports `validateRaw`, `deriveEvent`, `ZoneAggregator`, `decideCommand`, `ownsZone`, and `CepEngine`. Domain tests run with `node --test tests/domain.test.cjs`. Time-dependent tests use explicit source and receipt timestamps; they do not sleep or infer measured system performance.

## Events and MQTT topics

Every envelope has `schemaVersion: 1`, `id`, `type`, an ISO timestamp with an explicit timezone, `runId`, `correlationId`, and an object `payload`. Derived envelopes also have `sourceEventId`. The seven types are:

| Stage | Event type | Topic segment |
| --- | --- | --- |
| Device telemetry | `device.telemetry.raw` | `raw` |
| Normalized telemetry | `sensor.reading` | `reading` |
| Zone window | `zone.metrics.updated` | `metrics` |
| HVAC decision | `actuator.command.created` | `command` |
| Actuator acknowledgement | `actuator.command.applied` | `applied` |
| Gateway rejection | `gateway.telemetry.rejected` | `rejected` |
| Complex-event alert | `cep.alert.created` | `alert` |

Topic shape is `hvac/<main|flow>/<runId>/<stage>/<deviceId|zoneId>`. Device-level telemetry/readings/rejections/alerts use a device ID; zone metrics/commands/acknowledgements use a zone ID. `main` and `flow` are isolated execution namespaces. The broker and service layer own routing, subscription validation, QoS, authentication, retries, and persistence.

`contracts/event.schema.json` describes the envelope and stage payloads. JSON Schema validators must enable `date-time` format checking. Runtime validation additionally rejects non-finite JavaScript numbers, impossible calendar dates, and raw events whose `correlationId` is not equal to `id`. Identifiers start with an ASCII letter or digit and contain only ASCII letters, digits, dot, underscore, colon or hyphen, excluding MQTT wildcards and separators. Raw, run, correlation, device, zone and instance IDs are at most 128 characters. Derived IDs allow 160 characters so a maximum-length root ID can retain its suffix.

## Raw validation

`validateRaw(event)` returns `null` for valid raw telemetry, otherwise a diagnostic reason string. It handles null, primitives, arrays, missing fields, malformed timestamps, invalid identities, missing numeric fields, `NaN`, and infinities without throwing.

| Payload field | Constraint |
| --- | --- |
| `deviceId`, `zoneId` | Required safe identifiers |
| `temperatureC` | Finite number, -20 through 80 inclusive |
| `humidityPercent` | Finite number, 0 through 100 inclusive |
| `occupancyCount` | Non-negative JavaScript safe integer |
| `co2Ppm` | Finite number, 400 through 5000 inclusive |
| `batteryPercent` | Finite number, 0 through 100 inclusive |
| `sequence` | Optional non-negative JavaScript safe integer |

One reading per device per second is the intended telemetry rate, not a rate asserted or enforced by this pure module. Extra metadata is permitted. Input timestamps are not rejected merely for being old or ahead of the service clock; ordering and liveness are separate concerns.

## Deterministic derivation

`deriveEvent(parent, type, suffix, payload, nowMs = Date.now())` creates an envelope with an explicit ISO emission timestamp. `suffix` is a simple identity of 1–31 characters and may have one leading colon. The root ID is `parent.correlationId`, or `parent.id` when correlation is absent. The result has `id = rootId + ':' + suffix`, `correlationId = rootId`, and `sourceEventId = parent.id`.

For one raw event `raw-1`, the usual IDs are `raw-1:reading`, `raw-1:metrics`, `raw-1:command`, and `raw-1:rejected`; IDs never accumulate intermediate suffixes. Immediate-parent lineage and end-to-end correlation therefore remain separate. The caller owns the payload and chooses the type and suffix. Invalid derivation arguments throw `TypeError`.

Emission timestamps measure stage emission, while a reading payload's `sourceTimestamp` preserves the device's original timestamp. Downstream end-to-end latency calculations should use that source time, not the most recent envelope emission time. Wall-clock source timestamps require synchronized clocks for cross-host latency interpretation.

## Zone aggregation and checkpoints

`new ZoneAggregator(windowSize = 10)` creates one count-based ring window per `(runId, zoneId)`. `update(readingEvent, instanceId = 'aggregator')` returns:

```json
{
  "zoneId": "zone-1",
  "sampleCount": 10,
  "averageTemperatureC": 24.25,
  "averageHumidityPercent": 48.5,
  "averageOccupancyCount": 2.4,
  "averageCo2Ppm": 1002,
  "aggregatorId": "aggregator",
  "sourceTimestamp": "2026-09-06T00:00:10.000Z"
}
```

These are illustrative values, not an experiment result. `sourceTimestamp` comes from the newest accepted reading payload, falling back to that reading's envelope timestamp. A duplicate `(runId, event.id)` returns `null`, including ID collisions that try to move the same event into another zone. Invalid input throws `TypeError` before state changes. The service layer dispatches only normalized reading events to this method.

An accepted update uses constant-size arithmetic and an O(1) ring replacement rather than rescanning the window. Temperature, humidity and occupancy averages use two decimal places; CO2 uses an integer. `sampleCount` is the number of retained readings, not the number of devices, seconds, or readings since startup. A ten-reading multi-device zone window is not inherently a ten-second window. Readings are aggregated in arrival order; CEP has its own stricter source-time ordering.

`serialize()` returns a detached, JSON-serializable version-1 checkpoint containing window size, ring contents/cursors and deduplication IDs. `restore(snapshot)` atomically validates and restores it, replacing the instance's configured window size and returning the instance. Running sums are recomputed from restored samples instead of trusting persisted sums. Mutating a checkpoint does not mutate the aggregator, and a failed restore leaves the previous state intact.

Sample storage is bounded by `windowSize` per run and zone. Exact run-lifetime duplicate suppression retains seen IDs separately, so deduplication memory grows with accepted events. Retire run-scoped instances/checkpoints when a run is finished. Checkpoint storage and transaction coupling to MQTT acknowledgement remain service-layer responsibilities; the domain module alone does not establish exactly-once delivery.

## Control and partition ownership

`decideCommand(metrics, targetTemperature = 22)` always returns `{zoneId, mode, fanSpeed, reason, targetTemperatureC}`. Occupied means average occupancy at least 2. The cooling threshold is target temperature plus 1.5 degrees, or 23.5 degrees with the default target.

| Condition, in priority order | Mode | Fan speed |
| --- | --- | --- |
| Occupied and temperature strictly above cooling threshold | `cool` | `high` if CO2 is strictly above 900; otherwise `medium` |
| Occupied and CO2 strictly above 900 | `ventilate` | `high` |
| Otherwise | `idle` | `low` |

Equality at 23.5 degrees does not cool; equality at 900 ppm does not trigger high-CO2 ventilation. Occupancy 2 is occupied. Invalid required metrics or target values throw `TypeError`.

`ownsZone(zoneId, index, count)` assigns `zone-N` to zero-based shard `(N - 1) % count`. N must be a positive safe integer without leading zeros. Invalid zone IDs, shard counts, and shard indices return `false`. The function does not move checkpoints or coordinate changes in shard count.

## Complex-event processing

`new CepEngine({ sustainedMs = 10000, silentMs = 5000 } = {})` tracks each `(runId, deviceId)` separately. `reading(event, nowMs = Date.now())` returns zero or one sustained alert payload. `sweep(nowMs = Date.now())` returns any newly silent-device alert payloads.

Source time is `Date.parse(event.payload.sourceTimestamp || event.timestamp)`. Receipt time is the explicit `nowMs`. Duplicate run/event IDs, equal or older per-device source timestamps, backwards per-device receipt times, and malformed readings are ignored without refreshing liveness. State is created only for observed, accepted devices.

Sustained CO2 requires every accepted reading in an episode to have CO2 strictly above 900 ppm and occupancy at least 2. The source-time duration since the first qualifying reading must reach `sustainedMs`. Three rapid updates are not a ten-second sustained event. A low-CO2 or low-occupancy reading, zone change, source-time gap of at least `silentMs`, receipt-time gap of at least `silentMs`, or silence detection breaks continuity. This rule does not interpolate healthy data through missing telemetry. One `sustained_co2` alert is emitted per qualifying episode; another can fire only after the episode resets.

Silence starts when receipt time since the last accepted fresh reading reaches `silentMs`. `sweep` emits one `silent_device` alert until a fresh reading is accepted. Duplicates and late traffic cannot mask silence. No separate recovery alert is emitted; accepting a fresh reading rearms silence detection. A device never seen by the engine cannot be declared silent without an external device registry.

All alert payloads contain `kind`, `runId`, `deviceId`, `zoneId`, `sourceEventId`, `correlationId`, `sourceTimestamp`, and `detectedAt`. Sustained alerts additionally contain `co2Ppm`, `occupancyCount`, `durationMs`, and `thresholdCo2Ppm: 900`. Silent alerts additionally contain `silentForMs` and `lastSeenAt`. The service layer wraps payloads as `cep.alert.created` events and chooses distinct alert IDs. CEP state is in memory; its API does not include checkpoint restoration.
