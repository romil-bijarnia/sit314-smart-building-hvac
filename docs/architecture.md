# Architecture and operating boundaries

## Main pipeline

```
Synthetic room device (own certificate,1Hz)
  -> Mosquitto / mutual TLS / QoS1
  -> Gateway validation
  -> Zone aggregator A or B (static zone ownership,10-reading window)
  -> HVAC controller
  -> Simulated actuator acknowledgement
```

The storage subscriber journals all event types. A separate CEP service reads normalized readings and detects sustained high CO2 and device silence. Node-RED implements an independent full validation/aggregation/control path in the `flow` namespace; main Node.js microservices use `main`. Both share the same pure-domain rules and have been compared end-to-end, not only by inspecting exported flow JSON.

## Layer mapping

- Things: manifest-driven synthetic device telemetry and software actuator acknowledgement.
- Communications: the real Mosquitto broker, MQTT QoS1 and mutually verified TLS certificates.
- Applications/analytics: validation, aggregation, control, time-based CEP, document storage and dashboards.

## Event contracts and identities

Every accepted envelope has schemaVersion,id,type,timestamp,runId,correlationId andpayload. Derived stages also carrysourceEventId; IDs are deterministic from the original reading, so retransmissions can be deduplicated. Schema:`contracts/event.schema.json`.

Topic:`hvac/<main|flow>/<runId>/<stage>/<deviceId|zoneId>`. The broker takes identity from the client-certificate CN. Each simulated device can publish only to its own raw topic; the gateway verifies payload identity against that topic. Each service mounts only its own certificate/private key plus the public CA certificate. The generated CA key is never mounted into a container or committed.

## Recovery and storage

Main services use stable persistent MQTT sessions and append an inbox record before acknowledging receipt. A prepared outbox records derived outputs before publishing; downstream logical IDs handle replay. Aggregator state is reconstructed from committed transformations. These are measured local process-restart guarantees, not a claim of fault-tolerant cloud consensus or full host-power-failure certification.

Storage uses an fsynced append journal plus an atomic state snapshot. Snapshot-before-journal-reset ordering avoids losing rollups during compaction. Raw events expire after30days with minute/sweep granularity; one-minute rollups remain. TTL tests advance a controlled clock rather than claiming a30day observation. Normalized data are validated before persistence; invalid wire envelopes are quarantined for inspection.

The local store holds indexes and retained documents in process memory and uses synchronous writes. It is deliberately a measured local document-store implementation, not a production substitute for DynamoDB. Cloud storage and longer capacity/soak tests remain later milestones.

## Scaling scope

Two aggregation containers own alternate zone IDs: `(zoneNumber-1) mod shardCount`. Each zone has one writer. The broker fans each normalized reading to the configured subscribers, and each discards unowned zones. This makes ownership explicit but adds fan-out traffic. Shard-count changes are controlled between-trial operations only: stop publishers, verify the previous run has fully drained through actuator acknowledgements and all active stage ledgers have no pending work, then stop both aggregators. Reset only the exact `sit314-aggregator-a` and `sit314-aggregator-b` MQTT sessions using their own client identities with `clean: true` and a zero session-expiry interval; disconnect the reset clients before recreating the desired owners. This intentionally discards obsolete broker queues from completed trials, including messages buffered for an inactive second shard. Start the next trial with a fresh `runId`, rather than continuing a window across ownership changes. Normal service restarts, including the controller recovery test, retain their persistent sessions. This is not transparent online rebalancing or AWS auto-scaling.

The window is the last10readings, not10seconds. As device density grows, those10readings cover less wall-clock time. CEP therefore uses elapsed source time for sustained thresholds and receipt time for silence.

## Security and deployment scope

All MQTT device/service traffic uses TLS with client certificates and topic ACLs. Services use unprivileged image users, dropped capabilities, read-only certificate mounts and an internal-only control network. Broker, storage dashboard and Node-RED additionally join a dedicated ingress bridge for host access; only loopback ports are published. Gateway, aggregation, controller, actuator and CEP remain on the internal control network only.

The dashboards and Node-RED editor intentionally use loopback HTTP during local development. They must not be exposed remotely without authenticated HTTPS. AWS identity, IAM, private subnets, SSM secrets, hosted HTTPS and billing guardrails remain Week9–11 work. No cloud deployment, automatic scaling, measured energy saving or physical comfort validation is claimed here.
