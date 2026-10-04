# One-owner versus two-owner scale-out experiment

Recorded before running this additional experiment on 22 September 2026. This is separate from the twelve-trial routing comparison; none of those earlier observations is removed or pooled into it.

## Purpose

The routing experiment changes only subscription selectivity while retaining two owners. It therefore cannot on its own establish the effect of increasing the owner count. This follow-up compares a single aggregation owner receiving all readings with two zone-selective aggregation owners at the project's largest normal local workload: 180 devices across 12 zones at 1 Hz, ten seconds per trial. The source, control logic, QoS 1, mutual TLS, durable stores and shared VM are otherwise the same.

Two paired repetitions give four fresh-deployment trials: single owner then two owners in repetition one, and two owners then single owner in repetition two. Raw sensor payloads are identical between paired treatments. Record p95 and maximum source-to-applied-acknowledgement latency, exact acknowledgement count by the offer deadline, measured offered rate, aggregation PUBLISH/payload/ledger volume, process CPU delta and final RSS. Report both pairs and any regressions. The normal acceptance criteria remain at least 98% nominal offered rate, zero deadline acknowledgement backlog, and maximum latency at most 2,000 ms.

The larger treatment changes both owner count and subscription selection: it evaluates the completed scale-out design as a package. The earlier two-owner controlled experiment isolates the routing mechanism separately. A single 7-vCPU, 4-GiB local VM is not two physical edge sites or cloud automatic scaling. Start/stop, certificate connection setup and warm-up remain outside the offering interval. Trial state is fresh; the same teardown, raw-input and independent actuator-capture procedures are reused.
