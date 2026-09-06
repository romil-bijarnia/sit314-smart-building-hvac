# Independent Node-RED HVAC pipeline

This is a deployed alternative to the JavaScript microservices, not a mock flow. It subscribes to real raw telemetry over mutually authenticated MQTT/TLS, calls the same pure domain module, and publishes normalized readings, rolling zone metrics, actuator commands and validation rejections. Only the core Node-RED palette is required.

## Runtime

- Official image: `nodered/node-red:4.1.14-22` (Node-RED 4.1.14 / Node.js 22), verified against [the publisher's Docker Hub tags](https://hub.docker.com/r/nodered/node-red/tags) on 6 September 2026.
- Build from the repository root: `docker build -f node-red/Dockerfile -t sit314-hvac-node-red .`
- Container command: `["--settings", "/data/settings.cjs", "/data/flows.json"]`. The official image entrypoint already starts Node-RED; these are its arguments, not a new `npm start` command. Verify the runtime log names `/data/settings.cjs`, not the default `/data/settings.js`.
- Set `MQTT_HOST=mosquitto` (default). Join the broker's Docker network so this hostname resolves.
- Mount the generated certificate directory read-only at `/certs`. The runtime requires readable `node-red.crt`, `node-red.key` and `ca.crt`. The broker must use the client certificate CN as the MQTT username and authorize `node-red` only for its flow namespace.
- Publish the UI only on loopback: `127.0.0.1:1880:1880`. The local dashboard is [http://127.0.0.1:1880/dashboard](http://127.0.0.1:1880/dashboard), the editor is [http://127.0.0.1:1880/admin/](http://127.0.0.1:1880/admin/) and live state is [http://127.0.0.1:1880/api/status](http://127.0.0.1:1880/api/status).

MQTT is always TLS with server verification and client certificates. The loopback-only UI uses HTTP by default; this is an explicit local UI exception, not a claim of universal transport encryption. Optional `NODE_RED_HTTPS=true` loads `/certs/dashboard.crt` and `/certs/dashboard.key` and changes the UI to HTTPS. Never expose the unauthenticated editor to a non-loopback host interface.

Settings and flows are baked into `/data`. A fresh Docker named volume may copy these initial files; an existing volume must be updated explicitly after rebuilding. Do not obscure `/data` with an empty bind mount. Neither secrets nor certificate contents are included in the image.

## Real event path

```text
MQTT in: hvac/flow/+/raw/+ (QoS 1, mTLS)
  -> gateway: JSON/schema/range/topic validation + raw ID deduplication
  -> sensor.reading -> MQTT out hvac/flow/<runId>/reading/<zoneId>
  -> 10-reading rolling window -> zone.metrics.updated
       -> MQTT out hvac/flow/<runId>/metrics/<zoneId>
  -> shared 22 C control rules -> actuator.command.created
       -> MQTT out hvac/flow/<runId>/command/<zoneId>

Invalid JSON, invalid envelopes/readings or mismatched source topics
  -> gateway.telemetry.rejected
  -> MQTT out hvac/flow/<runId>/rejected/<deviceId>
```

Raw topics end with the payload `deviceId`, and their run segment must match `runId`. No subscription touches the main microservice namespace. Each accepted raw event creates root-derived `:reading`, `:metrics` and `:command` IDs, preserving the original correlation ID and direct-parent `sourceEventId`. Normalized payloads record `receivedAt`, `normalizedBy: "node-red"` and the original `sourceTimestamp`.

The shared `ZoneAggregator(10)` is held in node context. Raw deduplication suppresses repeat QoS 1 deliveries before downstream work, retaining the 20,000 most recent unique run/event IDs. State is in memory: restart clears windows, counters and deduplication; replay after restart or after eviction is not durable exactly-once processing. Windows and dashboard records are isolated by run and zone.

The dashboard uses core HTTP/template/response nodes, native browser code and same-origin polling every two seconds. It displays actual MQTT connection state, actual counters and the latest event values; there are no synthetic chart points, injected telemetry or external assets. Runtime faults are recorded separately rather than silently generating a command.

## Regeneration and checks

`build-flows.cjs` is the editable source for the function nodes and layout; `dashboard.html` is the editable UI source. Run `node node-red/build-flows.cjs` after either changes and commit the regenerated `flows.json`.

Run `node --test tests/nodered-flow.test.cjs` from the repository root. These checks compile all function nodes, verify wiring and TLS/QoS/topic settings, execute the real flow functions with the shared domain, check the rolling window and duplicate/error paths, and check live dashboard/API structure. Live MQTT delivery, container health and browser rendering are checked separately by the repository's integration experiment.
