# Historical in-process baseline

Preserved from the local workspace accompanying the July project plan. These files use Node.js EventEmitter, not a network MQTT broker. The three root JSON summaries are historical records dated 7 July 2026; they are not the new container results.

To rerun the exact legacy 12/60/180-device, 3/6/12-zone, 100 ms, six-second scenarios in this directory:

```
node src/load-test.js
```

New summaries are written under `baseline/data/`, leaving the historical root summaries unchanged. The original hardware manifest contains conceptual pin assignments and simulated values, not proof of a built physical or Tinkercad circuit.
