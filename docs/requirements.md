# Plan-to-evidence requirements

The authority is the submitted 1.2D Smart Building Air-Conditioning Scalability Project Plan (25 July 2026). 4.2D asks for a 4–5-page project-status document and a tutor discussion; it does not require pretending the final cloud deployment is complete.

| Plan commitment through Week 8 | Build/evidence target |
|---|---|
| Week 3: real Mosquitto MQTT QoS1 | Certificate-authenticated local broker, real per-device TCP/TLS sessions, original fleet load scenarios rerun |
| Week 4: full Node-RED pipeline | Independent raw→validation→10-reading window→controller flow, real MQTT inputs/outputs, live dashboard |
| Week 5: document data, retention, rollups | Durable JSON document journal, logical collections, 30-day raw retention, one-minute rollups, restart/TTL tests |
| Week 6: layers/contracts/hardware | Versioned event schema and layer mapping; accurate hardware mapping; circuit evidence requires a real simulator build |
| Week 7: separate services and CEP | Gateway, aggregation, controller and storage processes plus time-based high-CO2 and silent-device detection |
| Week 8: containers and replicated aggregation | Dockerfiles/Compose, container runtime, two static zone owners, measured delivery/backlog/latency |
| NFR1 | At least98% nominal input load with zero backlog for local12–180-device1Hz runs; also report the accelerated legacy scenario separately |
| NFR2 | Source telemetry to actual simulated-actuator acknowledgement at most2s during normal tested fleet runs |
| NFR3 | Local control continues with container WAN egress blocked; controlled restart recovery tested separately |
| NFR4 | Retention/rollup code verified using controlled time; no claim of waiting30days |
| NFR5 | MQTT mutualTLS/client identity/topicACLs; loopback HTTP dashboards are an explicit remaining security exception |
| NFR6 | Raw inputs, machine-readable summaries, scripts and source revision identify each experiment |

Weeks9–11 remain AWS IoT Core/Lambda/ECS/DynamoDB/CloudWatch, automatic scaling, security hardening and final reporting. Static local partitioning is not AWS automatic scaling. Synthetic environmental signals do not prove measured comfort or energy savings in a real building.
