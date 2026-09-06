import { defaultConfig } from './config.js';
import { runSimulation } from './simulation.js';

const scenarios = [
  { label: 'small', deviceCount: 12, zoneCount: 3 },
  { label: 'medium', deviceCount: 60, zoneCount: 6 },
  { label: 'large', deviceCount: 180, zoneCount: 12 }
];

const results = [];

for (const scenario of scenarios) {
  const summary = await runSimulation({
    ...defaultConfig,
    ...scenario,
    durationSeconds: 6,
    intervalMs: 100,
    storageEnabled: false,
    summaryPath: `data/load-test-${scenario.label}.json`
  });

  results.push({
    label: scenario.label,
    devices: scenario.deviceCount,
    zones: scenario.zoneCount,
    acceptedReadings: summary.gateway.accepted,
    rejectedReadings: summary.gateway.rejected,
    totalCommands: summary.controller.totalCommands,
    throughputEventsPerSecond: summary.throughputEventsPerSecond
  });
}

console.table(results);
