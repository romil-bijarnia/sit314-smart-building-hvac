import { mkdir, writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { DeviceFleet } from './deviceSimulator.js';
import { EventBus } from './eventBus.js';
import { Gateway } from './gateway.js';
import { AggregationService } from './services/aggregationService.js';
import { HvacController } from './services/hvacController.js';
import { StorageService } from './services/storageService.js';

export async function runSimulation(config) {
  const bus = new EventBus();
  const counters = {};
  const counterUnsubscribe = bus.subscribe('*', (event) => {
    counters[event.type] = (counters[event.type] ?? 0) + 1;
  });

  const storage = new StorageService({
    bus,
    outputPath: config.outputPath,
    enabled: config.storageEnabled
  });
  await storage.ready;

  const gateway = new Gateway({ bus });
  const aggregation = new AggregationService({ bus, windowSize: 10 });
  const controller = new HvacController({
    bus,
    targetTemperature: config.targetTemperature
  });
  const fleet = new DeviceFleet({
    bus,
    buildingId: config.buildingId,
    deviceCount: config.deviceCount,
    zoneCount: config.zoneCount,
    intervalMs: config.intervalMs
  });

  const startedAt = Date.now();
  fleet.start();
  await sleep(config.durationSeconds * 1000);
  fleet.stop();
  await sleep(Math.min(config.intervalMs, 250));

  const finishedAt = Date.now();
  counterUnsubscribe();
  gateway.close();
  aggregation.close();
  controller.close();
  await storage.close();

  const durationMs = finishedAt - startedAt;
  const summary = {
    ranAt: new Date().toISOString(),
    config,
    durationMs,
    throughputEventsPerSecond: round((Object.values(counters).reduce((a, b) => a + b, 0) / durationMs) * 1000),
    counters,
    hardware: fleet.snapshot(),
    gateway: gateway.snapshot(),
    zones: aggregation.snapshot(),
    controller: controller.snapshot(),
    storage: storage.snapshot()
  };

  if (config.summaryPath) {
    await mkdir(dirname(config.summaryPath), { recursive: true });
    await writeFile(config.summaryPath, `${JSON.stringify(summary, null, 2)}\n`);
  }

  return summary;
}

function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function round(value) {
  return Math.round(value * 100) / 100;
}
