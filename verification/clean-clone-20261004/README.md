# Clean-clone check — 4 October 2026

A fresh clone of commit `21f4345` was installed, built and exercised on the same local VM class used for the recorded experiments (rootless Podman 6.1.0, Linux ARM64, 7 vCPUs, 4 GiB). This is a check that the published repository builds and passes from scratch. It is not a new performance campaign, and the 22 September measurements under `research/` are unchanged.

| Step | Result | Record |
|---|---|---|
| `npm ci --ignore-scripts && npm test` | 63 of 63 tests pass | `unit-tests.log` |
| `python3 research/verify_results.py` | Twelve routing trials re-verified from raw files | `verify-routing.log` |
| `python3 research/verify_scaleout.py` | Four owner-count trials re-verified from raw files | `verify-scaleout.log` |
| `node scripts/research-stack.cjs build` | Service and Node-RED images build | `build.log` |
| Functional suite, `AGGREGATION_ROUTING=selective` | 10 of 10 scenarios pass | `functional-selective.log`, `functional-tests-selective.json` |
| Functional suite, `AGGREGATION_ROUTING=broadcast` | 10 of 10 scenarios pass | `functional-broadcast.log`, `functional-tests-broadcast.json` |

After the same functional suite, the two aggregation owners had received 52 and 40 PUBLISH packets in selective mode and 92 each in broadcast mode, which is the halving of aggregation deliveries the routing change is meant to produce.

`main-dashboard-selective.png` and `node-red-dashboard-selective.png` are browser captures of the two dashboards taken while the selective stack was running, immediately after the functional suite.

The functional suite was run against the research stack with:

```sh
export CONTAINER_ENGINE=podman MQTT_URL=mqtts://127.0.0.1:18883 MQTT_PORT=18883 \
  STATUS_BASE_URL=http://127.0.0.1:13140 NODE_RED_BASE_URL=http://127.0.0.1:13180 \
  STACK_PREFIX=sit314-eval-
AGGREGATION_ROUTING=selective ZONE_COUNT=12 node scripts/research-stack.cjs up 2
npm run test:integration
node scripts/research-stack.cjs clean
```
