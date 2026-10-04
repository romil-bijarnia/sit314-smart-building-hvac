# Zone-selective routing experiment

Recorded before the experimental comparison on 22 September 2026.

## Question and criteria

Does moving zone ownership into MQTT subscription selection reduce aggregation work and improve source-to-simulated-actuator acknowledgement latency compared with the current broadcast-to-both-aggregators architecture?

The baseline and candidate both use two aggregation processes, the same ten-reading window, durable inbox/outbox, controller, actuator, store, TLS and QoS 1. The sole treatment is subscription routing. Broadcast subscribes both owners to all normalized readings; selective subscribes each owner only to its fixed assigned zone topics. The candidate is a local implementation of topic-to-owner selective routing, not a distributed broker overlay or automatic scaling system.

The primary performance measures are p95 and maximum end-to-end latency, completion fraction, and achieved offered rate. Resource measures are received MQTT PUBLISH counts, payload bytes at the two aggregators, durable aggregator ledger bytes, and process CPU/memory observations. Count/byte reductions must not be relabelled as latency improvement. No results will be discarded because they oppose the hypothesis.

## Workloads and order

Three workloads: 12 devices / 3 zones / 1 Hz / 10 seconds; 180 devices / 12 zones / 1 Hz / 10 seconds; 180 devices / 12 zones / 10 Hz / 6 seconds. Two repetitions of each treatment/workload produce twelve trials. Each trial starts with a clean broker and clean service state in its own local deployment. Repetition 1 uses broadcast then selective; repetition 2 reverses treatment and workload order. Identical deterministic sensor values are used for each matched workload; absolute timestamps and run IDs differ.

The MQTT broker is Mosquitto 2.0.22. All services run in an isolated rootless Podman Linux VM deployment with seven virtual CPUs and 4 GiB configured RAM. Host test scripts use Node.js. Starting/stopping and TLS client setup are outside the offering window. Each final result retains raw sent JSONL, per-event actuator acknowledgements, service-health samples, ledger sizes, resource observations and source hashes.

## Analysis

Compare each pair within the same repetition/workload; show both repetitions rather than pooling all events into a false large sample. The short normal workload target remains at least 98% nominal offering, complete acknowledgement delivery and maximum latency at most 2,000 ms. The accelerated case diagnoses capacity and is not assumed to pass. Completion observed after offering is not evidence of zero backlog at the exact offer deadline: timestamped acknowledgements determine in-window delivery.

Two repetitions on one local host establish a bounded engineering comparison, not production capacity, geographical network effects, or a statistical confidence interval. The broker, controller and storage remain shared bottlenecks. No live reassignment or crash during owner reconfiguration is tested by this comparison. The independent Node-RED path remains unchanged.

## Research basis

Bonomi et al. (2012) motivate placing latency-sensitive IoT control near devices. Redondi, Arcia-Moret and Manzoni (2019), section V-B, describe selective event routing in which subsets of topics map to responsible nodes, contrasting it with flooding. This experiment applies that routing principle to aggregation consumers behind one broker; it does not claim reproduction of the paper's multi-broker, 5G or MQTT+ system.

Sources:
- https://conferences.sigcomm.org/sigcomm/2012/paper/mcc/p13.pdf
- https://arxiv.org/html/1902.07022
- https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html
