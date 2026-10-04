# Smart Building HVAC — SIT314

An executable local progression from an in-process IoT prototype to MQTT-connected services, a full Node-RED pipeline, and measured zone-partitioned processing.

## Final evaluation — 22 September 2026

Tag [`v6.3d-final`](https://github.com/romil-bijarnia/sit314-smart-building-hvac/tree/v6.3d-final) is the revision the SIT314 6.3D project report refers to. It contains the service source measured on 22 September, the deployment configuration, every raw trial, and the scripts that re-check the reported numbers.

| Where | What it holds |
|---|---|
| [`src/`](src) | Gateway, aggregation owner, controller, actuator, CEP and storage services; MQTT transport; domain rules; durable inbox/outbox and document store |
| [`src/routing.cjs`](src/routing.cjs) | Zone-selective subscription routing, the change evaluated in the report |
| [`tests/`](tests) | 63 unit and regression tests |
| [`compose.yaml`](compose.yaml), [`Dockerfile`](Dockerfile), [`infra/`](infra), [`node-red/`](node-red) | Container deployment, broker configuration and the independent Node-RED flow |
| [`scripts/`](scripts) | Certificate generation, stack control, functional tests and experiment campaigns |
| [`research/`](research) | Twelve routing trials and four owner-count trials with raw inputs, actuator acknowledgements, resource samples, protocols and independent verifiers |
| [`verification/`](verification) | Unit and functional test records from 22 September, and a [clean-clone check](verification/clean-clone-20261004) on 4 October |
| [`evidence/`](evidence) | The 6 September broker-based baseline campaign |
| [`baseline/`](baseline) | The July in-process prototype kept for comparison |

### Result summary

Zone-selective routing gives each aggregation owner only the readings for the zones it owns. Against the earlier broadcast design, with two owners in both cases, it halved aggregation PUBLISH deliveries and payload bytes in every pair, cut aggregator journal growth by about 40.6% and aggregator CPU time by 25.6–41.8%. At 180 devices and 1 Hz, p95 latency improved from 646 / 549 ms to 460 / 472 ms. At 12 devices it was no faster, and at 180 devices and 10 Hz the system still missed the two-second target in both modes.

A separate comparison of one owner against two selective owners at 180 devices and 1 Hz did not show a consistent latency benefit from the second owner. All 58,080 offered readings across the sixteen trials have a matched simulated-actuator acknowledgement.

### Re-check the reported numbers

```sh
python3 research/verify_results.py     # twelve routing trials
python3 research/verify_scaleout.py    # four owner-count trials
```

Both scripts recompute latency percentiles, deadline counts, hashes and stage balances from the raw files under `research/`, and need only Python 3. The GitHub Actions workflow in `.github/workflows/verify.yml` runs them on every push, together with the unit tests and the ten functional scenarios on Docker.

### Re-run the experiments

```sh
podman machine start
npm ci --ignore-scripts
npm test
node scripts/generate-certs.cjs 180
node scripts/research-stack.cjs build
node scripts/research-campaign.cjs
node scripts/research-scaleout.cjs
```

The research stack uses the container prefix `sit314-eval-` and loopback ports 18883, 13140 and 13180, so it does not collide with the default deployment below. The campaigns rewrite the files under `research/`; use a separate clone to keep the recorded results. Broadcast remains the default routing mode. Set `AGGREGATION_ROUTING=selective` with the full `ZONE_COUNT` to start the selective stack by hand. Owner and zone changes require a drained transition, not live rebalancing.

The service source at this tag is byte-identical to the measured build; `research/environment/source-sha256.txt` lists the hashes. `research/archive-manifest-20260922.json` lists every file of the 22 September evidence set, with paths under `smart-building-hvac/` corresponding to the root of this repository.

## What runs

Nine local containers: Mosquitto, gateway, two aggregation owners, HVAC controller, simulated actuators, complex-event processing, document storage/dashboard, and Node-RED. Each simulated device connects with its own client certificate. MQTT transport uses QoS 1 and verified mutual TLS; topic ACLs prevent a device publishing as another device.

The main path is `raw telemetry → validated reading → zone metrics → command → applied acknowledgement`. Node-RED independently implements validation, aggregation and control in a separate topic namespace. The two paths share tested control rules, not an injected display feed.

## Quick start with Docker

Requirements: Node.js 22 or newer, npm, OpenSSL, Docker Engine with Compose. No AWS account or paid cloud resource is used.

```sh
npm ci --ignore-scripts
node scripts/generate-certs.cjs 180
docker compose up --build -d
node scripts/experiment.cjs --devices 24 --zones 4 --ticks 20 --interval 1000
```

Open the evidence dashboard at **http://localhost:3140** and the independent Node-RED dashboard at **http://localhost:3180/dashboard**. The Node-RED editor is **http://localhost:3180/admin/**. These ports are bound to loopback; do not expose the development HTTP interfaces remotely.

Run only one fleet experiment/campaign at a time because per-device MQTT client identities are stable. A normal experiment ends after its configured ticks and preserves source events plus its result summary under `evidence/runs/`.

## Podman option used for the recorded Mac experiments

The Dockerfiles also build Docker-format images with Podman. The recorded local host is an Apple M3 Max; final container experiments use a rootless Linux VM with 7 vCPUs and 4 GiB configured RAM. The GitHub workflow independently exercises Docker.

```sh
podman machine start
npm ci --ignore-scripts
node scripts/generate-certs.cjs 180
node scripts/stack.cjs build
node scripts/stack.cjs up 2
npm run test:integration
node scripts/campaign.cjs
```

If the stopped Podman VM has less memory, configure it before starting: `podman machine set --memory 4096`. This project does not install a system-wide trusted CA. Generated certificate keys stay under ignored `.private/` and are never required from this repository.

## Verification

```sh
npm test
npm run test:integration
node scripts/verify-experiments.cjs
```

Unit tests cover validation, count-based windows, controller boundaries, elapsed-time CEP, ownership, durable inbox/outbox recovery, transactional document storage and TTL/rollup recovery. Integration tests require the running stack and exercise real MQTT traffic, all modes, Node-RED equivalence, duplicates, malformed measurements, TLS and ACL rejection, controller/storage restarts and blocked WAN egress. Set `CONTAINER_ENGINE=docker` when using `scripts/stack.cjs` with Docker rather than Podman.

`campaign.cjs` runs two 20-second repetitions for each of 12/60/180 devices at 1 Hz with one and two aggregation owners, then the original plan's accelerated 10 Hz load shape and a 24-device soak. It preserves all final results; the earlier 2 GiB development campaign and its observed storage OOM remain separately labelled in `evidence/development-2gib/`.

Static owner changes are only allowed between fully drained experiments with publishers stopped and a fresh run ID. They are not automatic scaling or live state migration. Normal process restarts retain persistent sessions.

## Data and evidence

- `contracts/event.schema.json`: versioned envelopes and telemetry constraints.
- `src/`: broker adapters, domain logic, services and durable storage.
- `node-red/`: importable full flow, dashboard, settings and container image.
- `compose.yaml`, `Dockerfile`, `infra/`: repeatable local deployment.
- `scripts/`: certificate setup, deployment, experiments and verification.
- `evidence/`: actual test logs, raw synthetic inputs and machine-readable measurements.
- `baseline/`: the preserved July EventEmitter prototype and historical measurements.
- `docs/architecture.md`: layers, ownership, recovery semantics and deployment boundaries.
- `hardware/`: corrected proposed pin/sample mapping; not completed circuit evidence.

The store keeps an fsynced JSON document journal, atomically checkpointed state, logical collection exports, 30-day raw retention and one-minute rollups. Its in-memory indexes and local synchronous I/O remain capacity considerations; it is not represented as a production DynamoDB deployment.

## Recorded Week 8 scope

This is the local Week 8 progress milestone. Sensor readings and actuator actions are simulated. A real Tinkercad/physical circuit, AWS IoT Core/Lambda/ECS/DynamoDB/CloudWatch, cloud auto-scaling, hosted authenticated HTTPS and measured building energy savings are not claimed as complete. See `hardware/README.md` for the corrected DHT22, PIR and CO2 assumptions before physical integration.

Results are retained from executed experiments. Personal assessment PDFs and credentials are excluded from the repository.

## Recorded status evidence — 6 September 2026

- **59 unit/regression tests and 10 real functional scenarios passed.** A hosted CI run also built and tested the stack on Docker Engine in Ubuntu; its record is `evidence/github-ci.json`.
- **12 normal trials:** 12, 60 and 180 devices at 1 Hz, one/two aggregation owners, two 20-second repetitions. All 20,160 readings reached simulated actuator application within the offering window. Worst latency was 505 ms.
- **60-second soak:** 24 devices, 1,440 readings, maximum 146 ms.
- **Accelerated 10 Hz follow-up:** the 180-device case exposed a limit, with 14.344 s maximum latency. It is not presented as meeting the 2 s target.
- Backlog in these results means missing control acknowledgements. Broker queue depth and pending durable writes were not directly instrumented; post-offer journal observations can lag application.

See [verified measurements](evidence/campaign-summary.json), [CSV](evidence/campaign-summary.csv), [functional proof](evidence/functional-tests.json), [unit log](evidence/unit-tests.log), [storage arithmetic](evidence/storage-growth.json), and [verified Docker CI](https://github.com/romil-bijarnia/sit314-smart-building-hvac/actions/runs/34005255874). Earlier OOM and latency failures are separately retained as development evidence, not mixed into the final statistics.
